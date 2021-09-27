//
// Created by Xinjing on 9/12/21.
//

#pragma once
#include <vector>

#include "core/Executor.h"
#include "protocol/H-Store/HStore.h"

namespace star {
template <class Workload>
class HStoreExecutor
    : public Executor<Workload, HStore<typename Workload::DatabaseType>>

{
private:
  LockfreeQueue<Message *> hstore_master_in_queue, hstore_master_in_queue2, hstore_master_out_queue;
  std::vector<std::unique_ptr<Message>> hstore_master_cluster_worker_messages;
  // Partition-level Locking table on the master node.
  // master_partition_owned_by[i] indicates the owner of the partition i, -1 if not owned by anyone.
  std::vector<int32_t> master_partition_owned_by;

  std::vector<std::unique_ptr<Message>> cluster_worker_messages;
  std::vector<bool> parts_touched;
  std::vector<int> parts_touched_tables;
  int cluster_worker_num;
  Percentile<uint64_t> txn_try_times;
  Percentile<uint64_t> hstore_master_queuing_time;
  Percentile<uint64_t> avg_num_concurrent_mp;
  Percentile<uint64_t> avg_mp_queue_depth;

  uint64_t worker_commit = 0;
public:
  using base_type = Executor<Workload, HStore<typename Workload::DatabaseType>>;

  using WorkloadType = Workload;
  using ProtocolType = HStore<typename Workload::DatabaseType>;
  using DatabaseType = typename WorkloadType::DatabaseType;
  using TransactionType = typename WorkloadType::TransactionType;
  using ContextType = typename DatabaseType::ContextType;
  using RandomType = typename DatabaseType::RandomType;
  using MessageType = typename ProtocolType::MessageType;
  using MessageFactoryType = typename ProtocolType::MessageFactoryType;
  using MessageHandlerType = typename ProtocolType::MessageHandlerType;

  using StorageType = typename WorkloadType::StorageType;

  int this_cluster_worker_id;
  std::vector<int> owned_partition_locked_by;
  std::vector<int> managed_partitions;
  //int ;
  bool hstore_master = false;

  HStoreExecutor(std::size_t coordinator_id, std::size_t worker_id, DatabaseType &db,
                const ContextType &context,
                std::atomic<uint32_t> &worker_status,
                std::atomic<uint32_t> &n_complete_workers,
                std::atomic<uint32_t> &n_started_workers)
      : base_type(coordinator_id, worker_id, db, context, worker_status,
                  n_complete_workers, n_started_workers) {
      cluster_worker_num = this->context.worker_num * this->context.coordinator_num;
      DCHECK(this->context.partition_num % this->context.coordinator_num == 0);
      DCHECK(this->context.partition_num % this->context.worker_num == 0);
      if (worker_id > context.worker_num) {
        //LOG(INFO) << "HStore Master Executor " << worker_id;
        this_cluster_worker_id = 0;
        hstore_master = true;
      } else {
        this_cluster_worker_id = worker_id + coordinator_id * context.worker_num;
        DCHECK(this_cluster_worker_id < (int)this->context.partition_num);
        hstore_master = false;
        std::string managed_partitions_str;
        for (int p = 0; p < (int)this->context.partition_num; ++p) {
          if (this_cluster_worker_id == partition_owner_cluster_worker(p)) {
            managed_partitions_str += std::to_string(p) + ",";
            managed_partitions.push_back(p);
          }
        }
        DCHECK(managed_partitions.empty() == false);
        managed_partitions_str.pop_back(); // Remove last ,
        //LOG(INFO) << "Cluster worker id " << this_cluster_worker_id << " node worker id "<< worker_id
                  //<< " partitions managed [" << managed_partitions_str << "]";
      }

      owned_partition_locked_by.resize(this->context.partition_num, -1);
      cluster_worker_messages.resize(cluster_worker_num);
      hstore_master_cluster_worker_messages.resize(cluster_worker_num);
      parts_touched.resize(this->context.partition_num, false);
      parts_touched_tables.resize(this->context.partition_num, -1);
      for (int i = 0; i < (int)cluster_worker_num; ++i) {
        cluster_worker_messages[i] = std::make_unique<Message>();
        init_message(cluster_worker_messages[i].get(), i);
        hstore_master_cluster_worker_messages[i] = std::make_unique<Message>();
        init_message(hstore_master_cluster_worker_messages[i].get(), i);
      }
      master_partition_owned_by.resize(this->context.partition_num, -1);
      this->message_stats.resize((size_t)HStoreMessage::NFIELDS, 0);
      this->message_sizes.resize((size_t)HStoreMessage::NFIELDS, 0);
  }

  ~HStoreExecutor() = default;


  void search(std::size_t table_id, std::size_t partition_id,
                  const void *key, void *value) const {
    DCHECK((int)partition_owner_cluster_worker(partition_id) == this_cluster_worker_id);
    ITable *table = this->db.find_table(table_id, partition_id);
    auto value_bytes = table->value_size();
    auto row = table->search(key);
    HStoreHelper::read(row, value, value_bytes);
  }

  uint64_t generate_tid(TransactionType &txn) {
    static std::atomic<uint64_t> tid_counter{1};
    return tid_counter.fetch_add(1);
  }

  void abort(TransactionType &txn,
             std::vector<std::unique_ptr<Message>> &messages) {

    // assume all writes are updates
    if (!txn.is_single_partition()) {
      for (int i = 0; i < (int)this->context.partition_num; ++i) {
        if (parts_touched[i] == false)
          continue;
        int partitionId = i;
        auto owner_cluster_worker = partition_owner_cluster_worker(partitionId);
        if (owner_cluster_worker == this_cluster_worker_id) {
          if (owned_partition_locked_by[partitionId] == this_cluster_worker_id) {
            //LOG(INFO) << "Abort release lock partition " << partitionId << " by cluster worker" << this_cluster_worker_id;
            owned_partition_locked_by[partitionId] = -1; // unlock partitions
          }
        } else {
          // send messages to other partitions to abort and unlock partitions
          // No need to wait for the response.
          txn.pendingResponses++;
          auto tableId = parts_touched_tables[i];
          auto table = this->db.find_table(tableId, partitionId);
          txn.network_size += MessageFactoryType::new_release_partition_lock_message(
              *messages[owner_cluster_worker], *table, this_cluster_worker_id, true);
        }
      }
      sync_messages(txn, true);
    } else {
      DCHECK(txn.pendingResponses == 0);
      DCHECK(txn.get_partitions().size() == 1);
      auto partitionId = txn.get_partitions()[0];
      DCHECK(owned_partition_locked_by[partitionId] == this_cluster_worker_id);
      //LOG(INFO) << "Abort release lock partition " << partitionId << " by cluster worker" << this_cluster_worker_id;
      owned_partition_locked_by[partitionId] = -1;
    }
  }

  bool commit(TransactionType &txn,
              std::vector<std::unique_ptr<Message>> &messages) {

    if (txn.abort_lock) {
      abort(txn, messages);
      return false;
    }

    // all locks are acquired

    // generate tid
    uint64_t commit_tid = generate_tid(txn);

    // write and replicate
    // Release partition locks

    write_and_replicate(txn, commit_tid, messages);

    // release locks
    //release_lock(txn, commit_tid, messages);

    return true;
  }

  void write_and_replicate(TransactionType &txn, uint64_t commit_tid,
                           std::vector<std::unique_ptr<Message>> &messages) {
    //auto &readSet = txn.readSet;
    auto &writeSet = txn.writeSet;
    for (auto i = 0u; i < writeSet.size(); i++) {
      auto &writeKey = writeSet[i];
      auto tableId = writeKey.get_table_id();
      auto partitionId = writeKey.get_partition_id();
      auto owner_cluster_worker = partition_owner_cluster_worker(partitionId);
      auto table = this->db.find_table(tableId, partitionId);

      // write
      if ((int)owner_cluster_worker == this_cluster_worker_id) {
        auto key = writeKey.get_key();
        auto value = writeKey.get_value();
        table->update(key, value);
      } else {
        txn.pendingResponses++;
        txn.network_size += MessageFactoryType::new_write_back_message(
            *messages[owner_cluster_worker], *table, writeKey.get_key(),
            writeKey.get_value(), this_cluster_worker_id);
        //LOG(INFO) << "Partition worker " << this_cluster_worker_id << " issueed write request on partition " << partitionId;
      }
      // value replicate

      // std::size_t replicate_count = 0;

      // for (auto k = 0u; k < partitioner.total_coordinators(); k++) {

      //   // k does not have this partition
      //   if (!partitioner.is_partition_replicated_on(partitionId, k)) {
      //     continue;
      //   }

      //   // already write
      //   if (k == partitioner.master_coordinator(partitionId)) {
      //     continue;
      //   }

      //   replicate_count++;

      //   // local replicate
      //   if (k == txn.coordinator_id) {
      //     auto key = writeKey.get_key();
      //     auto value = writeKey.get_value();
      //     table->update(key, value);
      //   } else {

      //     txn.pendingResponses++;
      //     auto coordinatorID = k;
      //     txn.network_size += MessageFactoryType::new_replication_message(
      //         *messages[coordinatorID], *table, writeKey.get_key(),
      //         writeKey.get_value(), commit_tid);
      //   }
      // }

      //DCHECK(replicate_count == partitioner.replica_num() - 1);
    }
    if (txn.is_single_partition() == false) {
      for (int i = 0; i < (int)this->context.partition_num; ++i) {
        if (parts_touched[i] == false)
          continue;
        int partitionId = i;
        auto owner_cluster_worker = partition_owner_cluster_worker(partitionId);
        if (owner_cluster_worker == this_cluster_worker_id) {
          //LOG(INFO) << "Commit release lock partition " << partitionId << " by cluster worker" << this_cluster_worker_id;
          owned_partition_locked_by[partitionId] = -1; // unlock partitions
        } else {
            txn.pendingResponses++;
            auto tableId = parts_touched_tables[i];
            auto table = this->db.find_table(tableId, partitionId);
            // send messages to other partitions to unlock partitions;
            txn.network_size += MessageFactoryType::new_release_partition_lock_message(
                *messages[owner_cluster_worker], *table, this_cluster_worker_id, true);
            //LOG(INFO) << "Partition worker " << this_cluster_worker_id << " issueed lock release request on partition " << partitionId;
        }
      }
      sync_messages(txn);
    } else {
      DCHECK(txn.pendingResponses == 0);
      DCHECK(txn.get_partitions().size() == 1);
      auto partitionId = txn.get_partitions()[0];
      DCHECK(owned_partition_locked_by[partitionId] == this_cluster_worker_id);
      //LOG(INFO) << "Commit release lock partition " << partitionId << " by cluster worker" << this_cluster_worker_id;
      owned_partition_locked_by[partitionId] = -1;
    }
  }

  void release_lock(TransactionType &txn, uint64_t commit_tid,
                    std::vector<std::unique_ptr<Message>> &messages) {
    if (txn.is_single_partition()) {
      // For single-partition transactions, do nothing.
      // release single partition lock

    } else {

      sync_messages(txn);
    }

  }

  void sync_messages(TransactionType &txn, bool wait_response = true) {              
    txn.message_flusher();
    if (wait_response) {
      //LOG(INFO) << "Waiting for " << txn.pendingResponses << " responses";
      while (txn.pendingResponses > 0) {
        if (txn.remote_request_handler() ){
          txn.message_flusher();
        }
      }
    }
  }

  void setupHandlers(TransactionType &txn)

      override {
    txn.lock_request_handler =
        [this, &txn](std::size_t table_id, std::size_t partition_id,
                     uint32_t key_offset, const void *key, void *value,
                     bool local_index_read, bool write_lock, bool &success,
                     bool &remote) {
      this->parts_touched[partition_id] = true;
      this->parts_touched_tables[partition_id] = table_id;
      int owner_cluster_worker = partition_owner_cluster_worker(partition_id);
      if ((int)owner_cluster_worker == this_cluster_worker_id) {
        remote = false;
        if (owned_partition_locked_by[partition_id] != -1 && owned_partition_locked_by[partition_id] != this_cluster_worker_id) {
          success = false;
          return;
        }
        //if (owned_partition_locked_by[partition_id] == -1)
          //LOG(INFO) << "Tranasction from worker " << this_cluster_worker_id << " locked partition " << partition_id;
        owned_partition_locked_by[partition_id] = this_cluster_worker_id;

        success = true;

        this->search(table_id, partition_id, key, value);

      } else {
        bool in_parts = false;
        for (auto i = 0u; i < txn.get_partitions().size(); ++i) {
          if ((int)partition_id == txn.get_partitions()[i]) {
            in_parts = true;
            break;
          }
        }
        DCHECK(in_parts);
        ITable *table = this->db.find_table(table_id, partition_id);

        remote = true;

        // LOG(INFO) << "Requesting locking partition " << partition_id << " by cluster worker" << this_cluster_worker_id << " on owner_cluster_worker " << owner_cluster_worker; 
        txn.network_size += MessageFactoryType::new_acquire_partition_lock_message(
              *(cluster_worker_messages[owner_cluster_worker]), *table, key, key_offset, this_cluster_worker_id);
        txn.distributed_transaction = true;
        txn.pendingResponses++;
      }
    };

    txn.remote_request_handler = [this]() { return this->process_request(); };
    txn.message_flusher = [this]() { this->flush_messages(); };
  };

  using Transaction = TransactionType;
  void acquire_partition_lock_request_handler(MessagePiece inputPiece,
                                          Message &responseMessage,
                                          ITable &table, Transaction *txn) {
    DCHECK(inputPiece.get_message_type() ==
           static_cast<uint32_t>(HStoreMessage::ACQUIRE_PARTITION_LOCK_REQUEST));
    auto table_id = inputPiece.get_table_id();
    auto partition_id = inputPiece.get_partition_id();
    DCHECK(table_id == table.tableID());
    DCHECK(partition_id == table.partitionID());
    DCHECK((int)partition_owner_cluster_worker(partition_id) == this_cluster_worker_id);
    auto key_size = table.key_size();
    auto value_size = table.value_size();

    
    /*
     * The structure of a write lock request: (primary key, key offset, request_remote_worker_id)
     * The structure of a write lock response: (success?, key offset, value?)
     */
    uint32_t request_remote_worker_id;
    auto stringPiece = inputPiece.toStringPiece();
    uint32_t key_offset;

    DCHECK(inputPiece.get_message_length() ==
           MessagePiece::get_header_size() + key_size + sizeof(key_offset) + sizeof(uint32_t));

    const void *key = stringPiece.data();
    auto row = table.search(key);

    stringPiece.remove_prefix(key_size);
    star::Decoder dec(stringPiece);
    dec >> key_offset >> request_remote_worker_id;

    DCHECK(dec.size() == 0);
    bool success = false;
    if (owned_partition_locked_by[partition_id] == -1 || owned_partition_locked_by[partition_id] == (int)request_remote_worker_id) {
      //lock it;
      // if (owned_partition_locked_by[partition_id] == -1)
      //   LOG(INFO) << "Partition " << partition_id << " locked by remote cluster worker " << request_remote_worker_id << " by this_cluster_worker_id " << this_cluster_worker_id;
      owned_partition_locked_by[partition_id] = request_remote_worker_id;
      success = true;
    } else {
      //  LOG(INFO) << "Partition " << partition_id << " was failed to be locked by cluster worker " << request_remote_worker_id 
      //            << " already locked by " << owned_partition_locked_by[partition_id];
    }
    // if (this->context.enable_hstore_master) {
    //   DCHECK(success);
    // }
    // prepare response message header
    auto message_size =
        MessagePiece::get_header_size() + sizeof(bool) + sizeof(key_offset);

    if (success) {
      message_size += value_size;
    } else{
      //LOG(INFO) << "acquire_partition_lock_request from cluster worker " << request_remote_worker_id
      //            << " on partition " << partition_id
      //            << " partition locked acquired faliled, lock owned by " << owned_partition_locked_by[partition_id];
    }
    
    auto message_piece_header = MessagePiece::construct_message_piece_header(
        static_cast<uint32_t>(HStoreMessage::ACQUIRE_PARTITION_LOCK_RESPONSE), message_size,
        table_id, partition_id);

    star::Encoder encoder(responseMessage.data);
    encoder << message_piece_header;
    encoder << success << key_offset;

    if (success) {
      // reserve size for read
      responseMessage.data.append(value_size, 0);
      void *dest =
          &responseMessage.data[0] + responseMessage.data.size() - value_size;
      // read to message buffer
      HStoreHelper::read(row, dest, value_size);
    }

    responseMessage.flush();
  }


  void acquire_partition_lock_response_handler(MessagePiece inputPiece,
                                          Message &responseMessage,
                                          ITable &table, Transaction *txn) {
    DCHECK(inputPiece.get_message_type() ==
           static_cast<uint32_t>(HStoreMessage::ACQUIRE_PARTITION_LOCK_RESPONSE));
    auto table_id = inputPiece.get_table_id();
    auto partition_id = inputPiece.get_partition_id();
    DCHECK(table_id == table.tableID());
    DCHECK(partition_id == table.partitionID());
    auto key_size = table.key_size();
    auto value_size = table.value_size();

    /*
     * The structure of a read lock response: (success?, key offset, value?)
     */

    bool success;
    uint32_t key_offset;

    StringPiece stringPiece = inputPiece.toStringPiece();
    Decoder dec(stringPiece);
    dec >> success >> key_offset;

    if (success) {
      uint32_t msg_length = inputPiece.get_message_length();
      auto header_size = MessagePiece::get_header_size() ;
      uint32_t exp_length = header_size + sizeof(success) +
                 sizeof(key_offset) + value_size;
      DCHECK(msg_length == exp_length);

      TwoPLRWKey &readKey = txn->readSet[key_offset];
      dec.read_n_bytes(readKey.get_value(), value_size);
    } else {
      DCHECK(inputPiece.get_message_length() ==
             MessagePiece::get_header_size() + sizeof(success) +
                 sizeof(key_offset));

      txn->abort_lock = true;
    }
    txn->pendingResponses--;
    txn->network_size += inputPiece.get_message_length();
    // LOG(INFO) << "acquire_partition_lock_response for worker " << this_cluster_worker_id
    //           << " on partition " << partition_id
    //           << " partition locked acquired " << success 
    //           << " pending responses " << txn->pendingResponses;
  }

  void write_back_request_handler(MessagePiece inputPiece,
                                  Message &responseMessage,
                                  ITable &table, Transaction *txn) {
    DCHECK(inputPiece.get_message_type() ==
           static_cast<uint32_t>(HStoreMessage::WRITE_BACK_REQUEST));
    auto table_id = inputPiece.get_table_id();
    auto partition_id = inputPiece.get_partition_id();
    DCHECK(table_id == table.tableID());
    DCHECK(partition_id == table.partitionID());
    DCHECK(this_cluster_worker_id == (int)partition_owner_cluster_worker(partition_id));
    auto key_size = table.key_size();
    auto field_size = table.field_size();

    /*
     * The structure of a write request: (request_remote_worker, primary key, field value)
     * The structure of a write response: (success?)
     */
    uint32_t request_remote_worker;

    auto stringPiece = inputPiece.toStringPiece();

    Decoder dec(stringPiece);
    dec >> request_remote_worker;
    bool success = false;
    // Make sure the partition is currently owned by request_remote_worker
    if (owned_partition_locked_by[partition_id] == (int)request_remote_worker) {
      success = true;
    }
    // LOG(INFO) << "write_back_request_handler for worker " << request_remote_worker
    //   << " on partition " << partition_id
    //   << " partition locked acquired " << success
    //   << " current partition owner " << owned_partition_locked_by[partition_id];

    DCHECK(owned_partition_locked_by[partition_id] == (int)request_remote_worker);

    if (success) {
      stringPiece = dec.bytes;
      DCHECK(inputPiece.get_message_length() ==
      MessagePiece::get_header_size() + key_size + field_size + sizeof(uint32_t));
      const void *key = stringPiece.data();
      stringPiece.remove_prefix(key_size);
      table.deserialize_value(key, stringPiece);
    }

    // prepare response message header
    auto message_size = MessagePiece::get_header_size() + sizeof(bool);
    auto message_piece_header = MessagePiece::construct_message_piece_header(
        static_cast<uint32_t>(HStoreMessage::WRITE_BACK_RESPONSE), message_size,
        table_id, partition_id);

    star::Encoder encoder(responseMessage.data);
    encoder << message_piece_header << success;
    responseMessage.flush();
  }


  void write_back_response_handler(MessagePiece inputPiece,
                                    Message &responseMessage,
                                    ITable &table, Transaction *txn) {
    DCHECK(inputPiece.get_message_type() ==
           static_cast<uint32_t>(HStoreMessage::WRITE_BACK_RESPONSE));
    auto table_id = inputPiece.get_table_id();
    auto partition_id = inputPiece.get_partition_id();
    DCHECK(table_id == table.tableID());
    DCHECK(partition_id == table.partitionID());
    auto key_size = table.key_size();
    auto value_size = table.value_size();

    /*
     * The structure of a partition write and release response: (success?)
     */

    bool success;

    StringPiece stringPiece = inputPiece.toStringPiece();
    Decoder dec(stringPiece);
    dec >> success;
    
    DCHECK(success);
    if (success) {
      DCHECK(inputPiece.get_message_length() ==
             MessagePiece::get_header_size() + sizeof(success));
    } else {
      DCHECK(inputPiece.get_message_length() ==
             MessagePiece::get_header_size() + sizeof(success));
      txn->abort_lock = true;
    }

    txn->pendingResponses--;
    txn->network_size += inputPiece.get_message_length();
    // LOG(INFO) << "write_back_response_handler for worker " << this_cluster_worker_id
    //   << " on partition " << partition_id
    //   << " remote partition locked released " << success 
    //   << " pending responses " << txn->pendingResponses;
  }

  void release_partition_lock_request_handler(MessagePiece inputPiece,
                                          Message &responseMessage,
                                          ITable &table, Transaction *txn) {
    DCHECK(inputPiece.get_message_type() ==
           static_cast<uint32_t>(HStoreMessage::RELEASE_PARTITION_LOCK_REQUEST));
    auto table_id = inputPiece.get_table_id();
    auto partition_id = inputPiece.get_partition_id();
    DCHECK(table_id == table.tableID());
    DCHECK(partition_id == table.partitionID());
    DCHECK(this_cluster_worker_id == (int)partition_owner_cluster_worker(partition_id));
    auto key_size = table.key_size();
    auto field_size = table.field_size();

    /*
     * The structure of a release partition lock request: (request_remote_worker, sync)
     * No response.
     */
    uint32_t request_remote_worker;
    bool sync;

    DCHECK(inputPiece.get_message_length() ==
        MessagePiece::get_header_size() + sizeof(uint32_t) + sizeof(bool));
    auto stringPiece = inputPiece.toStringPiece();

    Decoder dec(stringPiece);
    dec >> request_remote_worker >> sync;
    bool success;
    if (owned_partition_locked_by[partition_id] != (int)request_remote_worker) {
      success = false;
    } else {
      // if (owned_partition_locked_by[partition_id] != -1)
      //   LOG(INFO) << "Partition " << partition_id << " unlocked by cluster worker" << request_remote_worker << " by this_cluster_worker_id " << this_cluster_worker_id;
      owned_partition_locked_by[partition_id] = -1;
      success = true;
    }
    // LOG(INFO) << "release_partition_lock_request_handler from worker " << this_cluster_worker_id
    //   << " on partition " << partition_id
    //   << " partition lock released " << success;
    if (!sync)
      return;
    // prepare response message header
    auto message_size = MessagePiece::get_header_size() + sizeof(bool);
    auto message_piece_header = MessagePiece::construct_message_piece_header(
        static_cast<uint32_t>(HStoreMessage::RELEASE_PARTITION_LOCK_RESPONSE), message_size,
        table_id, partition_id);

    star::Encoder encoder(responseMessage.data);
    encoder << message_piece_header << success;
    responseMessage.flush();
  }

  void release_partition_lock_response_handler(MessagePiece inputPiece,
                                          Message &responseMessage,
                                          ITable &table, Transaction *txn) {
    DCHECK(inputPiece.get_message_type() ==
           static_cast<uint32_t>(HStoreMessage::RELEASE_PARTITION_LOCK_RESPONSE));
    auto table_id = inputPiece.get_table_id();
    auto partition_id = inputPiece.get_partition_id();
    DCHECK(table_id == table.tableID());
    DCHECK(partition_id == table.partitionID());
    auto key_size = table.key_size();
    auto value_size = table.value_size();

    /*
     * The structure of a partition write and release response: (success?)
     */

    bool success;

    StringPiece stringPiece = inputPiece.toStringPiece();
    Decoder dec(stringPiece);
    dec >> success;
    
    if (success) {
      DCHECK(inputPiece.get_message_length() ==
             MessagePiece::get_header_size() + sizeof(success));
    } else {
      DCHECK(inputPiece.get_message_length() ==
             MessagePiece::get_header_size() + sizeof(success));
      txn->abort_lock = true;
    }

    txn->pendingResponses--;
    txn->network_size += inputPiece.get_message_length();
    // LOG(INFO) << "release_partition_lock_response_handler for worker " << this_cluster_worker_id
    //   << " on partition " << partition_id
    //   << " remote partition locked released " << success 
    //   << " pending responses " << txn->pendingResponses;
  }


  std::size_t process_request() {

    std::size_t size = 0;
    int times = 0;
    while (!this->in_queue.empty()) {
      ++size;
      std::unique_ptr<Message> message(this->in_queue.front());
      bool ok = this->in_queue.pop();
      CHECK(ok);
      DCHECK(message->get_worker_id() == this->id);
      for (auto it = message->begin(); it != message->end(); it++) {

        MessagePiece messagePiece = *it;
        auto type = messagePiece.get_message_type();
        auto message_partition_id = messagePiece.get_partition_id();
        auto message_partition_owner_cluster_worker_id = partition_owner_cluster_worker(message_partition_id);
        
        if (type != (int)HStoreMessage::MASTER_UNLOCK_PARTITION_RESPONSE && type != (int)HStoreMessage::MASTER_LOCK_PARTITION_RESPONSE
            && type != (int)HStoreMessage::ACQUIRE_PARTITION_LOCK_RESPONSE && type != (int) HStoreMessage::WRITE_BACK_RESPONSE && type != (int)HStoreMessage::RELEASE_READ_LOCK_RESPONSE
            && type != (int)HStoreMessage::RELEASE_PARTITION_LOCK_RESPONSE) {
          CHECK(message_partition_owner_cluster_worker_id == this_cluster_worker_id);
        }
        ITable *table = this->db.find_table(messagePiece.get_table_id(),
                                      messagePiece.get_partition_id());

//        DCHECK(message->get_source_cluster_worker_id() != this_cluster_worker_id);
        DCHECK(message->get_source_cluster_worker_id() < (int32_t)this->context.partition_num);
        if (type == (int)HStoreMessage::ACQUIRE_PARTITION_LOCK_REQUEST) {
          acquire_partition_lock_request_handler(messagePiece,
                                                 *cluster_worker_messages[message->get_source_cluster_worker_id()], *table,
                                                 this->transaction.get());
        } else if (type == (int)HStoreMessage::ACQUIRE_PARTITION_LOCK_RESPONSE) {
          acquire_partition_lock_response_handler(messagePiece,
                                                 *cluster_worker_messages[message->get_source_cluster_worker_id()], *table,
                                                 this->transaction.get());
        } else if (type == (int)HStoreMessage::WRITE_BACK_REQUEST) {
          write_back_request_handler(messagePiece,
                                    *cluster_worker_messages[message->get_source_cluster_worker_id()], *table,
                                    this->transaction.get());
        } else if (type == (int)HStoreMessage::WRITE_BACK_RESPONSE) {
          write_back_response_handler(messagePiece,
                                      *cluster_worker_messages[message->get_source_cluster_worker_id()], *table,
                                      this->transaction.get());
        } else if (type == (int)HStoreMessage::RELEASE_PARTITION_LOCK_REQUEST) {
          release_partition_lock_request_handler(messagePiece,
                                                 *cluster_worker_messages[message->get_source_cluster_worker_id()], *table,
                                                 this->transaction.get());
        } else if (type == (int)HStoreMessage::RELEASE_PARTITION_LOCK_RESPONSE) {
          release_partition_lock_response_handler(messagePiece,
                                                 *cluster_worker_messages[message->get_source_cluster_worker_id()], *table,
                                                 this->transaction.get());
        }  else if (type == (int)HStoreMessage::MASTER_LOCK_PARTITION_RESPONSE) {
          master_lock_partitions_response_handler(messagePiece,
                                                *table,
                                                this->transaction.get());
        } else if (type == (int)HStoreMessage::MASTER_UNLOCK_PARTITION_RESPONSE) {
          master_unlock_partitions_response_handler(messagePiece,
                                                *table,
                                                this->transaction.get());
        } else {
          CHECK(false);
        }

        this->message_stats[type]++;
        this->message_sizes[type] += messagePiece.get_message_length();
      }

      size += message->get_message_count();
      flush_messages();
    }
    return size;
  }
  
  virtual void push_master_special_message(Message *message) override { 
    //LOG(INFO) << "special message for hstore master";
    message->set_put_to_in_queue_time(Time::now());
    this->hstore_master_in_queue2.push(message);
  }

  virtual void push_master_message(Message *message) override { 
    //LOG(INFO) << "message for hstore master";
    message->set_put_to_in_queue_time(Time::now());
    this->hstore_master_in_queue.push(message); 
  }

  Message* pop_message_internal(LockfreeQueue<Message *> & queue) {
    if (queue.empty())
      return nullptr;

    Message *message = queue.front();

    if (this->delay->delay_enabled()) {
      auto now = std::chrono::steady_clock::now();
      if (std::chrono::duration_cast<std::chrono::microseconds>(now -
                                                                message->time)
              .count() < this->delay->message_delay()) {
        return nullptr;
      }
    }

    bool ok = queue.pop();
    CHECK(ok);

    return message;
  }

  Message *pop_message() override {
    if (hstore_master) {
      return pop_message_internal(this->hstore_master_out_queue);
    } else {
      return pop_message_internal(this->out_queue);
    }
  }

  bool master_unlock_partitions_request_handler(MessagePiece inputPiece,
                                          Message &responseMessage) {
    DCHECK(inputPiece.get_message_type() ==
           static_cast<uint32_t>(HStoreMessage::MASTER_UNLOCK_PARTITION_REQUEST));
    auto table_id = inputPiece.get_table_id();
    auto partition_id = inputPiece.get_partition_id();

    /*
     * The structure of a release partition lock request: (remote_worker_id)
     * No response.
     */
    uint32_t request_remote_worker;

    auto stringPiece = inputPiece.toStringPiece();

    Decoder dec(stringPiece);
    dec >> request_remote_worker;

    DCHECK(inputPiece.get_message_length() ==
        MessagePiece::get_header_size() + sizeof(uint32_t));
    for (size_t i = 0; i < this->context.partition_num; ++i) {
      uint32_t p = i;
      if (master_partition_owned_by[p] == (int32_t)request_remote_worker) {
        master_partition_owned_by[p] = -1;
        ///LOG(INFO) << "master_unlock_partitions_request_handler for remote worker " << request_remote_worker
       //<< " remote partitions locked partition " << p;
      }
        
    }

    // prepare response message header
    auto message_size = MessagePiece::get_header_size() + sizeof(bool);
    auto message_piece_header = MessagePiece::construct_message_piece_header(
        static_cast<uint32_t>(HStoreMessage::MASTER_UNLOCK_PARTITION_RESPONSE), message_size,
        table_id, partition_id);

    star::Encoder encoder(responseMessage.data);
    encoder << message_piece_header << true;
    responseMessage.flush();
    //LOG(INFO) << "master_unlock_partitions_request_handler for remote worker " << request_remote_worker;
    return true;
  }

  bool master_lock_partitions_request_handler(MessagePiece inputPiece,
                                          Message &responseMessage) {
    DCHECK(inputPiece.get_message_type() ==
           static_cast<uint32_t>(HStoreMessage::MASTER_LOCK_PARTITION_REQUEST));
    auto table_id = inputPiece.get_table_id();
    auto partition_id = inputPiece.get_partition_id();

    /*
     * The structure of a release partition lock request: (remote_worker_id, # parts, part1, part2...)
     * No response.
     */
    uint32_t request_remote_worker;

    uint32_t num_parts;
    auto stringPiece = inputPiece.toStringPiece();

    Decoder dec(stringPiece);
    dec >> request_remote_worker >> num_parts;

    std::string dbg_str;

    DCHECK(inputPiece.get_message_length() ==
        MessagePiece::get_header_size() + sizeof(uint32_t) + sizeof(uint32_t) + sizeof(int32_t) * num_parts);
    for (size_t i = 0; i < num_parts; ++i) {
      int32_t p;
      dec >> p;
      if (master_partition_owned_by[p] != -1 && master_partition_owned_by[p] != (int32_t)request_remote_worker) {
        //LOG(INFO) << "master_lock_partitions_request_handler for remote worker " << request_remote_worker
       //<< " remote partitions locked on partitions failed on partition " << p << " locked by " << master_partition_owned_by[p];
        return false;
      }
    }

    dec = Decoder(stringPiece);
    dec >> request_remote_worker >> num_parts;
    for (size_t i = 0; i < num_parts; ++i) {
      int32_t p;
      dec >> p;
      DCHECK(master_partition_owned_by[p] == -1 || master_partition_owned_by[p] == (int32_t)request_remote_worker);
      master_partition_owned_by[p] = request_remote_worker;
      dbg_str += std::to_string(p) + ",";
    }

    // prepare response message header
    auto message_size = MessagePiece::get_header_size() + sizeof(bool);
    auto message_piece_header = MessagePiece::construct_message_piece_header(
        static_cast<uint32_t>(HStoreMessage::MASTER_LOCK_PARTITION_RESPONSE), message_size,
        table_id, partition_id);

    star::Encoder encoder(responseMessage.data);
    encoder << message_piece_header << true;
    responseMessage.flush();
    //LOG(INFO) << "master_lock_partitions_request_handler for remote worker " << request_remote_worker
    //   << " remote partitions locked on partitions " << dbg_str;
    return true;
  }

  void master_lock_partitions_response_handler(MessagePiece inputPiece,
                                          ITable &table, Transaction *txn) {
    DCHECK(inputPiece.get_message_type() ==
           static_cast<uint32_t>(HStoreMessage::MASTER_LOCK_PARTITION_RESPONSE));
    auto table_id = inputPiece.get_table_id();
    auto partition_id = inputPiece.get_partition_id();
    DCHECK(table_id == table.tableID());
    DCHECK(partition_id == table.partitionID());
    auto key_size = table.key_size();
    auto value_size = table.value_size();

    /*
     * The structure of a partition write and release response: (success?)
     */

    bool success;

    StringPiece stringPiece = inputPiece.toStringPiece();
    Decoder dec(stringPiece);
    dec >> success;
    
    DCHECK(success);

    txn->pendingResponses--;
    txn->network_size += inputPiece.get_message_length();
    //LOG(INFO) << "master_lock_partitions_response_handler for worker " << this_cluster_worker_id
    //  << " remote partitions locked " << success 
    //  << " pending responses " << txn->pendingResponses;
  }

  void master_unlock_partitions_response_handler(MessagePiece inputPiece,
                                          ITable &table, Transaction *txn) {
    DCHECK(inputPiece.get_message_type() ==
           static_cast<uint32_t>(HStoreMessage::MASTER_UNLOCK_PARTITION_RESPONSE));
    auto table_id = inputPiece.get_table_id();
    auto partition_id = inputPiece.get_partition_id();
    DCHECK(table_id == table.tableID());
    DCHECK(partition_id == table.partitionID());
    auto key_size = table.key_size();
    auto value_size = table.value_size();

    /*
     * The structure of a partition write and release response: (success?)
     */

    bool success;

    StringPiece stringPiece = inputPiece.toStringPiece();
    Decoder dec(stringPiece);
    dec >> success;
    
    DCHECK(success);

    //txn->pendingResponses--;
    //txn->network_size += inputPiece.get_message_length();
    //LOG(INFO) << "master_unlock_partitions_response_handler for worker " << this_cluster_worker_id
    //  << " remote partitions locked " << success 
    //  << " pending responses " << txn->pendingResponses;
  }

  int get_concurrent_mp_num() {
    std::vector<bool> parts(this->context.partition_num, false);
    for (size_t i = 0; i < this->context.partition_num; ++i) {
      if (master_partition_owned_by[i] != -1) {
        parts[master_partition_owned_by[i]] = 1;
      }
    }
    int cnt = 0;
    for (size_t i = 0; i < this->context.partition_num; ++i) {
      if (parts[i]) {
        cnt++;
      }
    }
    return cnt;
  }

  std::vector<Message*> master_lock_request_messages;

  std::size_t process_hstore_master_requests() {
    std::size_t size = 0;
    int times = 0;

    while (!this->hstore_master_in_queue2.empty()) {
      ++size;
      std::unique_ptr<Message> message(this->hstore_master_in_queue2.front());
      bool ok = hstore_master_in_queue2.pop();
      //LOG(INFO) << "message from hstore_master_in_queue2";
       
      CHECK(ok);
      DCHECK(message->get_message_count() > 0);
      for (auto it = message->begin(); it != message->end(); it++) {

        MessagePiece messagePiece = *it;
        auto type = messagePiece.get_message_type();
        DCHECK(message->get_source_cluster_worker_id() < (int32_t)this->context.partition_num);
        if (type == (int)HStoreMessage::MASTER_UNLOCK_PARTITION_REQUEST) {
          bool success = master_unlock_partitions_request_handler(messagePiece,
                                                *hstore_master_cluster_worker_messages[message->get_source_cluster_worker_id()]);
          DCHECK(success);
        } else {
          CHECK(false);
        }

      }
      
      size += message->get_message_count();
      flush_hstore_master_messages();
    }

    ExecutorStatus status;
    // while ((!this->hstore_master_in_queue.empty() || !master_lock_request_messages.empty() ) 
    // && (status = static_cast<ExecutorStatus>(this->worker_status.load())) != ExecutorStatus::STOP && status != ExecutorStatus::CLEANUP) {
    //   ++size;
      while (!this->hstore_master_in_queue.empty()) {
        Message * message = this->hstore_master_in_queue.front();
        bool ok = hstore_master_in_queue.pop();
        CHECK(ok);
        master_lock_request_messages.push_back(message);
      }

      std::vector<Message*> tmp_master_lock_request_messages;

      for (size_t i = 0; i < master_lock_request_messages.size(); ++i) {
        Message * message = master_lock_request_messages[i];
        bool success = false;
        int msg_cnt = message->get_message_count();
        CHECK(msg_cnt == 1);
        for (auto it = message->begin(); it != message->end(); it++) {

          MessagePiece messagePiece = *it;
          auto type = messagePiece.get_message_type();

          DCHECK(message->get_source_cluster_worker_id() < (int32_t)this->context.partition_num);
          if (type == (int)HStoreMessage::MASTER_LOCK_PARTITION_REQUEST) {
            success = master_lock_partitions_request_handler(messagePiece,
                                                  *hstore_master_cluster_worker_messages[message->get_source_cluster_worker_id()]);
            if (success == false) {
              // Wait until the next round.
              tmp_master_lock_request_messages.push_back(message);
            } else {
              hstore_master_queuing_time.add((Time::now() - message->get_put_to_in_queue_time()) / 1000);
              size += message->get_message_count();
            }
          } else {
            CHECK(false);
          }
        }
        if (success) {
          flush_hstore_master_messages();
          std::unique_ptr<Message> rel(message);
        }
      }
      master_lock_request_messages = tmp_master_lock_request_messages;
    //}
    return size;
  }

  void start_hstore_master() {
    LOG(INFO) << "HStore master " << this->id << " starts.";
    uint64_t last_seed = 0;

    ExecutorStatus status;

    while ((status = static_cast<ExecutorStatus>(this->worker_status.load())) !=
           ExecutorStatus::START) {
      std::this_thread::yield();
    }
    this->n_started_workers.fetch_add(1);

    int cnt = 0;
    uint64_t last_time_check = Time::now();
    do {
      if (++cnt == 100) {
        if (Time::now() - last_time_check >= 1000000) { // sample at every 10ms
          avg_num_concurrent_mp.add(get_concurrent_mp_num());
          last_time_check = Time::now();
          avg_mp_queue_depth.add(hstore_master_in_queue.read_available() + master_lock_request_messages.size());
        }
        cnt = 0;
      }
      process_hstore_master_requests();
      status = static_cast<ExecutorStatus>(this->worker_status.load());
    } while (status != ExecutorStatus::STOP);

    this->n_complete_workers.fetch_add(1);

    LOG(INFO) << "HStore master message_queuing time: "
              << hstore_master_queuing_time.nth(50) << " us(50th) "
              << hstore_master_queuing_time.nth(75) << " us(75th) "
              << hstore_master_queuing_time.nth(95) << " us(95th) "
              << hstore_master_queuing_time.nth(99) << " us(99th) "
              << " MP concurrency: "
              << avg_num_concurrent_mp.nth(50) << " (50th) "
              << avg_num_concurrent_mp.nth(75) << " (75th) "
              << avg_num_concurrent_mp.nth(95) << " (95th) "
              << avg_num_concurrent_mp.nth(99) << " (99th) "
              << " MP queue depth: "
              << avg_mp_queue_depth.nth(50) << " (50th) "
              << avg_mp_queue_depth.nth(75) << " (75th) "
              << avg_mp_queue_depth.nth(95) << " (95th) "
              << avg_mp_queue_depth.nth(99) << " (99th) "
              ;

    // once all workers are stop, we need to process the replication
    // requests

    while (static_cast<ExecutorStatus>(this->worker_status.load()) !=
           ExecutorStatus::CLEANUP) {
      process_hstore_master_requests();
    }

    this->n_complete_workers.fetch_add(1);

    LOG(INFO) << "HStore master " << this->id << " exits.";
  }

  void obtain_master_partitions_lock(Transaction & txn) {
    const auto & parts = txn.get_partitions();
    std::string dbg_str;
    for (size_t i = 0; i < parts.size(); ++i) {
      dbg_str += std::to_string(parts[i]) + ",";
    }
    auto table = this->db.find_table(0, 0);
    DCHECK(cluster_worker_messages[0]->get_message_count() == 0);
    cluster_worker_messages[0] = std::make_unique<Message>();
    init_message(cluster_worker_messages[0].get(), 0);
    cluster_worker_messages[0]->set_worker_id(this->context.worker_num + 1);
    txn.network_size += MessageFactoryType::new_master_lock_partition_message(
        *cluster_worker_messages[0], *table, this_cluster_worker_id, parts);
    txn.pendingResponses++;
    //LOG(INFO) << "obtain_master_partitions_lock from cluster worker " << this_cluster_worker_id << " wait for " << dbg_str;
    sync_messages(txn, true);
    //LOG(INFO) << "obtain_master_partitions_lock from cluster worker " << this_cluster_worker_id << " wait for " << dbg_str << " done";
  }

  void release_master_partitions_lock(Transaction & txn) {
    const auto & parts = txn.get_partitions();
    auto table = this->db.find_table(0, 0);
    cluster_worker_messages[0] = std::make_unique<Message>();
    init_message(cluster_worker_messages[0].get(), 0);
    cluster_worker_messages[0]->set_worker_id(this->context.worker_num + 2);
    txn.network_size += MessageFactoryType::new_master_unlock_partition_message(
        *cluster_worker_messages[0], *table, this_cluster_worker_id);
    //txn.pendingResponses++;
    //LOG(INFO) << "release_master_partitions_lock wait, cluster worker " << this_cluster_worker_id ;
    sync_messages(txn, false);
    //LOG(INFO) << "release_master_partitions_lock done, cluster worker"  << this_cluster_worker_id ;
  }

  void start() override {
    LOG(INFO) << "Executor " << this->id << " starts.";

    StorageType storage;
    uint64_t last_seed = 0;

    ExecutorStatus status;

    while ((status = static_cast<ExecutorStatus>(this->worker_status.load())) !=
           ExecutorStatus::START) {
      std::this_thread::yield();
    }

    this->n_started_workers.fetch_add(1);
    
    int cnt = 0;
    
    worker_commit = 0;
    int try_times = 0;
    auto startTime = std::chrono::steady_clock::now();
    bool retry_transaction = false;
    int partition_id = managed_partitions[this->random.next() % managed_partitions.size()];
    do {
      process_request();
      partition_id = retry_transaction ? partition_id: managed_partitions[this->random.next() % managed_partitions.size()];
      if (!this->partitioner->is_backup() && owned_partition_locked_by[partition_id] == -1) {
        // backup node stands by for replication
        last_seed = this->random.get_seed();
        ++try_times;
        if (retry_transaction) {
          this->transaction->reset();
        } else {
          DCHECK((int)partition_owner_cluster_worker(partition_id) == this_cluster_worker_id);
          DCHECK(owned_partition_locked_by[partition_id] == -1);
          this->transaction =
              this->workload.next_transaction(this->context, partition_id, storage, this->id);
          setupHandlers(*this->transaction);
          if (this->transaction->is_single_partition()) {
            //LOG(INFO) << "Local txn";
            // This executor owns this partition for now.
            ////LOG(INFO) << "Local tranasction from worker " << this_cluster_worker_id << " locked partition " << partition_id;
            owned_partition_locked_by[partition_id] = this_cluster_worker_id;
          } else {
            ////LOG(INFO) << "Dist txn";
            std::fill(parts_touched.begin(), parts_touched.end(), false);
            if (this->context.enable_hstore_master) {
              obtain_master_partitions_lock(*this->transaction);
            }
          }
        }

        auto result = this->transaction->execute(this->id);

        if (result == TransactionResult::READY_TO_COMMIT) {
          bool commit = this->commit(*this->transaction, cluster_worker_messages);
          ////LOG(INFO) << "Txn Execution result " << (int)result << " commit " << commit;
          this->n_network_size.fetch_add(this->transaction->network_size);
          if (commit) {
            if (!this->transaction->is_single_partition()) {
              if (this->context.enable_hstore_master) {
                release_master_partitions_lock(*this->transaction);
              }
            }
            ++worker_commit;
            this->txn_try_times.add(try_times);
            try_times = 0;
            this->n_commit.fetch_add(1);
            if (this->transaction->si_in_serializable) {
              this->n_si_in_serializable.fetch_add(1);
            }
            retry_transaction = false;
            auto latency =
                std::chrono::duration_cast<std::chrono::microseconds>(
                    std::chrono::steady_clock::now() - startTime)
                    .count();
            this->percentile.add(latency);
            if (this->transaction->distributed_transaction) {
              this->dist_latency.add(latency);
            } else {
              this->local_latency.add(latency);
            }
            startTime = std::chrono::steady_clock::now();
          } else {
            if (this->transaction->abort_lock) {
              this->n_abort_lock.fetch_add(1);
            } else {
              DCHECK(this->transaction->abort_read_validation);
              this->n_abort_read_validation.fetch_add(1);
            }
            if (this->context.sleep_on_retry) {
              //std::this_thread::sleep_for(std::chrono::microseconds(
              //    this->random.uniform_dist(0, this->context.sleep_time)));
            }
            this->random.set_seed(last_seed);
            retry_transaction = true;
          }
        } else {
          ////LOG(INFO) << "Txn Execution result " << (int)result << " abort ";
          this->abort(*this->transaction, cluster_worker_messages);
          this->n_abort_no_retry.fetch_add(1);
          if (!this->transaction->is_single_partition()) {
            if (this->context.enable_hstore_master) {
              release_master_partitions_lock(*this->transaction);
            }
          }
          retry_transaction = false;
        }
        if (this->transaction->is_single_partition()) {
          // Make sure it is unlocked.
          DCHECK(owned_partition_locked_by[partition_id] == -1);
        }
      }
      
      status = static_cast<ExecutorStatus>(this->worker_status.load());
    } while (status != ExecutorStatus::STOP);

    this->n_complete_workers.fetch_add(1);

    // once all workers are stop, we need to process the replication
    // requests

    while (static_cast<ExecutorStatus>(this->worker_status.load()) !=
           ExecutorStatus::CLEANUP) {
      process_request();
    }

    process_request();
    this->n_complete_workers.fetch_add(1);

    //LOG(INFO) << "Executor " << this->id << " exits.";
  }

  void onExit() override {

    LOG(INFO) << "Worker " << this->id << " commit: "<< this->worker_commit<< " latency: " << this->percentile.nth(50)
              << " us (50%) " << this->percentile.nth(75) << " us (75%) "
              << this->percentile.nth(95) << " us (95%) " << this->percentile.nth(99)
              << " us (99%). dist txn latency: " << this->dist_latency.nth(50)
              << " us (50%) " << this->dist_latency.nth(75) << " us (75%) "
              << this->dist_latency.nth(95) << " us (95%) " << this->dist_latency.nth(99)
              << " us (99%). local txn latency: " << this->local_latency.nth(50)
              << " us (50%) " << this->local_latency.nth(75) << " us (75%) "
              << this->local_latency.nth(95) << " us (95%) " << this->local_latency.nth(99)
              << " us (99%). txn try times : " << this->txn_try_times.nth(50)
              << " (50%) " << this->txn_try_times.nth(75) << " (75%) "
              << this->txn_try_times.nth(95) << " (95%) " << this->txn_try_times.nth(99)
              << " (99%). ";

    if (this->id == 0) {
      for (auto i = 0u; i < this->message_stats.size(); i++) {
        LOG(INFO) << "message stats, type: " << i
                  << " count: " << this->message_stats[i]
                  << " total size: " << this->message_sizes[i];
      }
      this->percentile.save_cdf(this->context.cdf_path);
    }
  }

protected:
  virtual void flush_messages() override {

    DCHECK(cluster_worker_messages.size() == (size_t)cluster_worker_num);
    for (int i = 0; i < (int)cluster_worker_messages.size(); i++) {
      if (cluster_worker_messages[i]->get_message_count() == 0) {
        continue;
      }

      auto message = cluster_worker_messages[i].release();
      this->out_queue.push(message);
      message->set_put_to_out_queue_time(Time::now());

      cluster_worker_messages[i] = std::make_unique<Message>();
      init_message(cluster_worker_messages[i].get(), i);
    }
  }

  void flush_hstore_master_messages() {

    DCHECK(hstore_master_cluster_worker_messages.size() == (size_t)cluster_worker_num);
    for (int i = 0; i < (int)hstore_master_cluster_worker_messages.size(); i++) {
      if (hstore_master_cluster_worker_messages[i]->get_message_count() == 0) {
        continue;
      }

      auto message = hstore_master_cluster_worker_messages[i].release();
      this->hstore_master_out_queue.push(message);
      message->set_put_to_out_queue_time(Time::now());

      hstore_master_cluster_worker_messages[i] = std::make_unique<Message>();
      init_message(hstore_master_cluster_worker_messages[i].get(), i);
    }
  }

  int partition_owner_worker_id_on_a_node(int partition_id) const {
    auto nth_partition_on_master_coord = partition_id / this->context.coordinator_num;
    auto node_worker_id_this_partition_belongs_to = nth_partition_on_master_coord % this->context.worker_num; // A worker could handle more than 1 partition
    return node_worker_id_this_partition_belongs_to;
  }

  int partition_owner_cluster_worker(int partition_id) const {
    auto master_coord_id = this->partitioner->master_coordinator(partition_id);
    auto cluster_worker_id_starts_at_this_node = master_coord_id * this->context.worker_num;

    return cluster_worker_id_starts_at_this_node + partition_owner_worker_id_on_a_node(partition_id);
  }

  int cluster_worker_id_to_coordinator_id(int dest_cluster_worker_id) {
    return dest_cluster_worker_id / this->context.worker_num;
  }

  int cluster_worker_id_to_worker_id_on_a_node(int dest_cluster_worker_id) {
    return dest_cluster_worker_id % this->context.worker_num;
  }


  void init_message(Message *message, int dest_cluster_worker_id) {
    DCHECK(dest_cluster_worker_id >= 0 && dest_cluster_worker_id < (int)this->context.partition_num);
    message->set_source_node_id(this->coordinator_id);
    int dest_coord_id = cluster_worker_id_to_coordinator_id(dest_cluster_worker_id);
    message->set_dest_node_id(dest_coord_id);
    int dest_worker_id = cluster_worker_id_to_worker_id_on_a_node(dest_cluster_worker_id);
    message->set_worker_id(dest_worker_id);
    message->set_source_cluster_worker_id(this_cluster_worker_id);
  }
};
} // namespace star
