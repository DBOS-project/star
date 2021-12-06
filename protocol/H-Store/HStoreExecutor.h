//
// Created by Xinjing on 9/12/21.
//

#pragma once
#include <vector>

#include "core/Executor.h"
#include "protocol/H-Store/HStore.h"

namespace star {

struct TxnCommand {
  int64_t tid; 
  int coordinator_worker;
  std::string command_data;
};

template <class Workload>
class HStoreExecutor
    : public Executor<Workload, HStore<typename Workload::DatabaseType>>

{
private:
  std::vector<std::unique_ptr<Message>> cluster_worker_messages;
  std::vector<bool> parts_touched;
  std::vector<int> parts_touched_tables;
  int cluster_worker_num;
  Percentile<uint64_t> txn_try_times;
  Percentile<int64_t> mp_round_concurrency;
  Percentile<int64_t> sp_round_concurrency;
  std::size_t batch_per_worker;
  std::atomic<bool> ended{false};
  uint64_t worker_commit = 0;
  uint64_t sent_sp_replication_requests = 0;
  uint64_t received_sp_replication_responses = 0;
  uint64_t sent_persist_cmd_buffer_requests = 0;
  uint64_t received_persist_cmd_buffer_response = 0;
public:
  using base_type = Executor<Workload, HStore<typename Workload::DatabaseType>>;

  const std::string tid_to_string(uint64_t tid) {
    return "[coordinator=" + std::to_string(tid >> 56) + ", tid=" + std::to_string(tid & ~(1ULL << 56)) + "]";
  }

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
  std::vector<int64_t> owned_partition_locked_by;
  std::vector<int> managed_partitions;
  std::deque<std::unique_ptr<TransactionType>> queuedTxns;
  bool is_replica_worker = false;
  LockfreeQueue<Message *> replica_in_queue, replica_out_queue;
  HStoreExecutor* replica_worker = nullptr;
  std::unordered_map<long long, TransactionType*> active_txns;
  std::deque<TransactionType*> pending_txns;

  std::deque<TxnCommand> command_buffer;
  
  HStoreExecutor(std::size_t coordinator_id, std::size_t worker_id, DatabaseType &db,
                const ContextType &context,
                std::atomic<uint32_t> &worker_status,
                std::atomic<uint32_t> &n_complete_workers,
                std::atomic<uint32_t> &n_started_workers,
                bool is_replica_worker = false)
      : base_type(coordinator_id, worker_id, db, context, worker_status,
                  n_complete_workers, n_started_workers), is_replica_worker(is_replica_worker) {
      cluster_worker_num = this->context.worker_num * this->context.coordinator_num;
      DCHECK(this->context.partition_num % this->context.coordinator_num == 0);
      DCHECK(this->context.partition_num % this->context.worker_num == 0);
      if (worker_id > context.worker_num) {
        //LOG(INFO) << "HStore Master Executor " << worker_id;
        this_cluster_worker_id = 0;
      } else {
        this_cluster_worker_id = worker_id + coordinator_id * context.worker_num;
        DCHECK(this_cluster_worker_id < (int)this->context.partition_num);
        std::string managed_partitions_str;
        if (is_replica_worker == false) {
          for (int p = 0; p < (int)this->context.partition_num; ++p) {
            if (this_cluster_worker_id == partition_owner_cluster_worker(p, 0)) {
              managed_partitions_str += std::to_string(p) + ",";
              managed_partitions.push_back(p);
            }
          }
          DCHECK(managed_partitions.empty() == false);
          managed_partitions_str.pop_back(); // Remove last ,
        }
        
        std::string managed_replica_partitions_str;
        if (is_replica_worker) {
          for (int p = 0; p < (int)this->context.partition_num; ++p) {
            for (size_t i = 1; i < this->partitioner->replica_num(); ++i) {
              if (this_cluster_worker_id == partition_owner_cluster_worker(p, i)) {
                managed_replica_partitions_str += std::to_string(p) + ",";
              }
            }
          }
          managed_replica_partitions_str.pop_back(); // Remove last ,
        }
        LOG(INFO) << "Cluster worker id " << this_cluster_worker_id << " node worker id "<< worker_id
                  << " partitions managed [" << managed_partitions_str 
                  << "], replica partitions maanged [" << managed_replica_partitions_str << "]" 
                  << " is_replica_worker " << is_replica_worker;
        batch_per_worker = std::max(this->context.batch_size / this->context.worker_num, (std::size_t)1);
      }

      owned_partition_locked_by.resize(this->context.partition_num, -1);
      cluster_worker_messages.resize(cluster_worker_num);
      parts_touched.resize(this->context.partition_num, false);
      parts_touched_tables.resize(this->context.partition_num, -1);
      for (int i = 0; i < (int)cluster_worker_num; ++i) {
        cluster_worker_messages[i] = std::make_unique<Message>();
        init_message(cluster_worker_messages[i].get(), i);
      }
      this->message_stats.resize((size_t)HStoreMessage::NFIELDS, 0);
      this->message_sizes.resize((size_t)HStoreMessage::NFIELDS, 0);

      if (this->partitioner->replica_num() > 1) {
        if (is_replica_worker == false) {
          replica_worker = new HStoreExecutor(coordinator_id, worker_id, 
                                              db, context, worker_status, 
                                              n_complete_workers, n_started_workers, true);
          std::thread([](HStoreExecutor * replica_worker){
            replica_worker->start();
          }, replica_worker).detach();
        }
      }
  }

  ~HStoreExecutor() = default;


  void search(std::size_t table_id, std::size_t partition_id,
                  const void *key, void *value) const {
//    DCHECK((int)partition_owner_cluster_worker(partition_id) == this_cluster_worker_id);
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
             std::vector<std::unique_ptr<Message>> &messages,
             bool write_cmd_buffer = false) {
    // assume all writes are updates
    if (!txn.is_single_partition()) {
      int partition_count = txn.get_partition_count();
      for (int i = 0; i < partition_count; ++i) {
        int partition_id = txn.get_partition(i);
        auto owner_cluster_worker = partition_owner_cluster_worker(partition_id, txn.ith_replica);
        if (owner_cluster_worker == this_cluster_worker_id) {
          if (owned_partition_locked_by[partition_id] == txn.transaction_id) {
            //LOG(INFO) << "Abort release lock MP partition " << partition_id << " by cluster worker" << this_cluster_worker_id << " " << tid_to_string(txn.transaction_id);
            owned_partition_locked_by[partition_id] = -1; // unlock partitions
          }
        } else {
          // send messages to other partitions to abort and unlock partitions
          // No need to wait for the response.
          //txn.pendingResponses++;
          auto tableId = 0;
          auto table = this->db.find_table(tableId, partition_id);
          messages[owner_cluster_worker]->set_transaction_id(txn.transaction_id);
          txn.network_size += MessageFactoryType::new_release_partition_lock_message(
              *messages[owner_cluster_worker], *table, this_cluster_worker_id, false, txn.ith_replica, write_cmd_buffer, "");
          //LOG(INFO) << "Abort release lock MP partition " << partition_id << " by cluster worker" << this_cluster_worker_id << " " << tid_to_string(txn.transaction_id) << " request sent";
        }
      }
      txn.message_flusher();
    } else {
      DCHECK(txn.pendingResponses == 0);
      DCHECK(txn.get_partition_count() == 1);
      auto partition_id = txn.get_partition(0);
      DCHECK(owned_partition_locked_by[partition_id] == txn.transaction_id);
      //LOG(INFO) << "Abort release lock local partition " << partition_id << " by cluster worker" << this_cluster_worker_id<< " " << tid_to_string(txn.transaction_id);
      owned_partition_locked_by[partition_id] = -1;
    }
  }

  bool commit_sp(TransactionType &txn,
              std::vector<std::unique_ptr<Message>> &messages) {
    // if (is_replica_worker) { // Should always succeed for replica
    //   CHECK(txn.abort_lock == false);
    // }
    DCHECK(this->context.hstore_command_logging == true);
    if (txn.abort_lock) {
      return false;
    }

    uint64_t commit_tid = generate_tid(txn);
    DCHECK(txn.get_logger());

    ScopedTimer t([&, this](uint64_t us) {
      txn.record_commit_write_back_time(us);
    });
    write_back_command_logging(txn, commit_tid, messages);
    return true;
  }

  bool commit_mp(TransactionType &txn,
              std::vector<std::unique_ptr<Message>> &messages) {
    // if (is_replica_worker) { // Should always succeed for replica
    //   CHECK(txn.abort_lock == false);
    // }
    if (txn.abort_lock) {
      abort(txn, messages);
      return false;
    }
    DCHECK(this->context.hstore_command_logging == true);

    uint64_t commit_tid = generate_tid(txn);
    DCHECK(txn.get_logger());

    auto txn_command_data = txn.serialize(0);

    command_buffer.push_back({txn.transaction_id, this_cluster_worker_id, txn_command_data});

    ScopedTimer t([&, this](uint64_t us) {
      txn.record_commit_write_back_time(us);
    });
    write_back_command_logging(txn, commit_tid, messages);
    release_partition_locks_async(txn, messages, true);
    return true;
  }


  bool commit(TransactionType &txn,
              std::vector<std::unique_ptr<Message>> &messages) {
    // if (is_replica_worker) { // Should always succeed for replica
    //   CHECK(txn.abort_lock == false);
    // }
    if (txn.abort_lock) {
      abort(txn, messages);
      return false;
    }
    DCHECK(this->context.hstore_command_logging == true);
    // all locks are acquired and execution is complete
    // for command logging, log input transaction and ship to replica nodes
    DCHECK(txn.get_logger());
    uint64_t commit_tid = generate_tid(txn);
    if (txn.ith_replica == 0) { // When executing on master replica, do replications to the rest of the replicas
      DCHECK(txn.initiating_cluster_worker_id == -1);
      DCHECK(is_replica_worker == false);
      bool has_replicas = this->partitioner->replica_num() > 1;
      {
        ScopedTimer t([&, this](uint64_t us) {
          if (has_replicas && txn.ith_replica == 0) {
            txn.record_commit_replication_time(us);
          }
        });
        for (size_t i = 1; i < this->partitioner->replica_num(); ++i) {
          auto partition_id = txn.partition_id;
          DCHECK(partition_id < this->context.partition_num);
          auto txn_command_data = txn.serialize(i);
          auto replica_idx = i;
          auto replica_inititing_cluster_worker_id = partition_owner_cluster_worker(partition_id, i);
          
          // //do {
          //   bool in_set = false;
          //   for (auto j = 0; j < txn.get_partition_count(); ++j) {
          //     txn.pendingResponses++;
          //     auto partition_id_j = txn.get_partition(j);
          //     if (partition_id == partition_id_j) {
          //       in_set = true;
          //     }
          //     auto owner_cluster_worker = partition_owner_cluster_worker(partition_id_j, replica_idx);  
            //     auto owner_cluster_worker = partition_owner_cluster_worker(partition_id_j, replica_idx);  
          //     auto owner_cluster_worker = partition_owner_cluster_worker(partition_id_j, replica_idx);  
            //     auto owner_cluster_worker = partition_owner_cluster_worker(partition_id_j, replica_idx);  
          //     auto owner_cluster_worker = partition_owner_cluster_worker(partition_id_j, replica_idx);  
            //     auto owner_cluster_worker = partition_owner_cluster_worker(partition_id_j, replica_idx);  
          //     auto owner_cluster_worker = partition_owner_cluster_worker(partition_id_j, replica_idx);  
            //     auto owner_cluster_worker = partition_owner_cluster_worker(partition_id_j, replica_idx);  
          //     auto owner_cluster_worker = partition_owner_cluster_worker(partition_id_j, replica_idx);  
          //     txn.network_size += MessageFactoryType::new_acquire_partition_lock_message(
          //       *messages[owner_cluster_worker], partition_id_j, this_cluster_worker_id, replica_inititing_cluster_worker_id, owner_cluster_worker, replica_idx);
          //   }
          //   DCHECK(in_set);
          //   sync_messages(txn, true);
          // //}
          DCHECK(txn.abort_lock == false);
          txn.pendingResponses++;
          // LOG(INFO) << "this_cluster_worker " << this_cluster_worker_id << " issue command replication on remote worker "
          //           << replica_inititing_cluster_worker_id << " on partition " << txn.partition_id << " is sp " << txn.is_single_partition();
          messages[replica_inititing_cluster_worker_id]->set_transaction_id(txn.transaction_id);
          txn.network_size += MessageFactoryType::new_command_replication(
            *messages[replica_inititing_cluster_worker_id], i, txn_command_data, this_cluster_worker_id);
        }
        txn.message_flusher();
        auto txn_command_data = txn.serialize(0);
        ScopedTimer t2([&, this](uint64_t us) {
          txn.record_commit_persistence_time(us);
        });
        txn.txn_cmd_log_lsn = this->logger->write(txn_command_data.c_str(), txn_command_data.size(), false);
        txn.get_logger()->sync(txn.txn_cmd_log_lsn, [&, this]() {txn.remote_request_handler();});
        DCHECK(txn.abort_lock == false);
      }
      ScopedTimer t([&, this](uint64_t us) {
        txn.record_commit_write_back_time(us);
      });
      write_back_command_logging(txn, commit_tid, messages);
      if (txn.is_single_partition() == true) {
        sync_messages(txn, true);
      }
      release_partition_locks_async(txn, messages, false);
    } else {
      DCHECK(txn.initiating_cluster_worker_id != -1);
      DCHECK(txn.initiating_transaction_id != 0);
      auto txn_command_data = txn.serialize(txn.ith_replica);
      if (txn.is_single_partition()) {
        DCHECK(txn.replicated_sp == false);
        txn.get_logger()->write(txn_command_data.c_str(), txn_command_data.size(), true, [&, this]() {txn.remote_request_handler();});
        messages[txn.initiating_cluster_worker_id]->set_transaction_id(txn.initiating_transaction_id);
        txn.network_size += MessageFactoryType::new_command_replication_response_message(
          *messages[txn.initiating_cluster_worker_id]);
        txn.message_flusher();
        write_back_command_logging(txn, commit_tid, messages);
        release_partition_locks_async(txn, messages, false);
      } else {
        txn.get_logger()->write(txn_command_data.c_str(), txn_command_data.size(), true, [&, this]() {txn.remote_request_handler();});
        messages[txn.initiating_cluster_worker_id]->set_transaction_id(txn.initiating_transaction_id);
        txn.network_size += MessageFactoryType::new_command_replication_response_message(
          *messages[txn.initiating_cluster_worker_id]);
        txn.message_flusher();
        write_back_command_logging(txn, commit_tid, messages);
        release_partition_locks_async(txn, messages, false);
      }
    }
    return true;
  }

  void prepare_and_redo_for_commit(TransactionType &txn,
                           std::vector<std::unique_ptr<Message>> &messages) {

    auto &readSet = txn.readSet;
    auto &writeSet = txn.writeSet;
    std::vector<TwoPLRWKey> emptyWirteSet;
    if (txn.is_single_partition()) {
      if (this->context.hstore_command_logging == false) {
        // Redo logging
        for (size_t j = 0; j < writeSet.size(); ++j) {
          auto &writeKey = writeSet[j];
          auto tableId = writeKey.get_table_id();
          auto partitionId = writeKey.get_partition_id();
          auto owner_cluster_worker = partition_owner_cluster_worker(partitionId, txn.ith_replica);
          DCHECK(owner_cluster_worker == this_cluster_worker_id);
          auto table = this->db.find_table(tableId, partitionId);
          auto key_size = table->key_size();
          auto value_size = table->value_size();
          auto key = writeKey.get_key();
          auto value = writeKey.get_value();
          DCHECK(key);
          DCHECK(value);
          std::ostringstream ss;
          ss << tableId << partitionId << key_size << std::string((char*)key, key_size) << value_size << std::string((char*)value, value_size);
          auto output = ss.str();
          txn.get_logger()->write(output.c_str(), output.size(), false);
        }
      }
    } else {
      std::vector<std::vector<TwoPLRWKey>> writeSetGroupByClusterWorkers(this->context.worker_num * this->context.coordinator_num);
      std::vector<bool> workerShouldPersistLog(this->context.worker_num * this->context.coordinator_num, false);
      std::vector<bool> coordinatorCovered(this->context.coordinator_num, false);
      for (auto i = (int)writeSet.size() - 1; i >= 0; --i) {
        auto &writeKey = writeSet[i];
        auto tableId = writeKey.get_table_id();
        auto partitionId = writeKey.get_partition_id();
        auto table = this->db.find_table(tableId, partitionId);
        auto coordinatorId = this->partitioner->master_coordinator(partitionId);
        auto owner_cluster_worker = partition_owner_cluster_worker(partitionId, txn.ith_replica);
        writeSetGroupByClusterWorkers[owner_cluster_worker].push_back(writeKey);
        if (coordinatorCovered[coordinatorId] == false) {
          workerShouldPersistLog[owner_cluster_worker] = true;
          coordinatorCovered[coordinatorId] = true;
        }
      }

      for (int i = 0; i < (int)writeSetGroupByClusterWorkers.size(); ++i) {
        auto & writeSet = writeSetGroupByClusterWorkers[i];
        if (writeSet.empty())
          continue;
        auto owner_cluster_worker = i;
        if (owner_cluster_worker == this_cluster_worker_id) {
          if (this->context.hstore_command_logging == false) {
            // Redo logging
            for (size_t j = 0; j < writeSet.size(); ++j) {
              auto &writeKey = writeSet[j];
              auto tableId = writeKey.get_table_id();
              auto partitionId = writeKey.get_partition_id();
              auto table = this->db.find_table(tableId, partitionId);
              auto key_size = table->key_size();
              auto value_size = table->value_size();
              auto key = writeKey.get_key();
              auto value = writeKey.get_value();
              DCHECK(key);
              DCHECK(value);
              std::ostringstream ss;
              ss << tableId << partitionId << key_size << std::string((char*)key, key_size) << value_size << std::string((char*)value, value_size);
              auto output = ss.str();
              txn.get_logger()->write(output.c_str(), output.size(), false);
            }
          }
        } else {
          txn.pendingResponses++;
          messages[owner_cluster_worker]->set_transaction_id(txn.transaction_id);
          txn.network_size += MessageFactoryType::new_prepare_and_redo_message(
              *messages[owner_cluster_worker],this->context.hstore_command_logging == false ? writeSet : emptyWirteSet, this->db, workerShouldPersistLog[i], txn.ith_replica);
        }
      }
      sync_messages(txn);
    }
  }

  void prepare_for_commit(TransactionType &txn,
                           std::vector<std::unique_ptr<Message>> &messages) {
    int partitionCount = txn.get_partition_count();
    for (int i = 0; i < partitionCount; ++i) {
      int partitionId = txn.get_partition(i);
      auto owner_cluster_worker = partition_owner_cluster_worker(partitionId, txn.ith_replica);
      if (owner_cluster_worker == this_cluster_worker_id) {
      } else {
          txn.pendingResponses++;
          auto tableId = 0;
          auto table = this->db.find_table(tableId, partitionId);
          // send messages to other partitions to unlock partitions;
          messages[owner_cluster_worker]->set_transaction_id(txn.transaction_id);
          txn.network_size += MessageFactoryType::new_prepare_message(
              *messages[owner_cluster_worker], *table, this_cluster_worker_id);
          //LOG(INFO) << "Partition worker " << this_cluster_worker_id << " issueed lock release request on partition " << partitionId;
      }
    }
    sync_messages(txn);
  }

  void release_partition_locks_async(TransactionType &txn, std::vector<std::unique_ptr<Message>> &messages, bool write_cmd_buffer) {
    std::string txn_command_data;
    if (write_cmd_buffer) {
      txn_command_data = txn.serialize(0);
    }
    if (txn.is_single_partition() == false) {
      int partition_count = txn.get_partition_count();
      for (int i = 0; i < partition_count; ++i) {
        int partitionId = txn.get_partition(i);
        auto owner_cluster_worker = partition_owner_cluster_worker(partitionId, txn.ith_replica);
        if (owner_cluster_worker == this_cluster_worker_id) {
          DCHECK(owned_partition_locked_by[partitionId] != -1);
          DCHECK(owned_partition_locked_by[partitionId] == txn.transaction_id);
          //LOG(INFO) << "Commit MP release lock partition " << partitionId << " by cluster worker" << this_cluster_worker_id << " ith_replica " << txn.ith_replica << " txn " << tid_to_string(txn.transaction_id);;
          owned_partition_locked_by[partitionId] = -1; // unlock partitions
        } else {
            auto tableId = 0;
            auto table = this->db.find_table(tableId, partitionId);
            // send messages to other partitions to unlock partitions;
            messages[owner_cluster_worker]->set_transaction_id(txn.transaction_id);
            txn.network_size += MessageFactoryType::new_release_partition_lock_message(
                *messages[owner_cluster_worker], *table, this_cluster_worker_id, false, txn.ith_replica, write_cmd_buffer, txn_command_data);
            //LOG(INFO) << "Partition worker " << this_cluster_worker_id << " issueed lock release request on partition " << partitionId << " ith_replica " << txn.ith_replica << " txn " << tid_to_string(txn.transaction_id);;
        }
      }
      txn.message_flusher();
    } else {
      DCHECK(txn.get_partition_count() == 1);
      auto partition_id = txn.get_partition(0);
      DCHECK(owned_partition_locked_by[partition_id] == txn.transaction_id);
      //LOG(INFO) << "Commit release lock partition " << partition_id << " by cluster worker " << this_cluster_worker_id << " ith_replica " << txn.ith_replica << " txn " << tid_to_string(txn.transaction_id);;
      owned_partition_locked_by[partition_id] = -1;
    }
  }

  void write_back_command_logging(TransactionType &txn, uint64_t commit_tid,
                  std::vector<std::unique_ptr<Message>> &messages) {
    //auto &readSet = txn.readSet;
    auto &writeSet = txn.writeSet;

    std::vector<bool> persist_commit_record(writeSet.size(), false);
    std::vector<bool> coordinator_covered(this->context.coordinator_num, false);
    
    if (txn.get_logger()) {
      // We set persist_commit_record[i] to true if it is the last write to the coordinator
      // We traverse backwards and set the sync flag for the first write whose coordinator_covered is not true
      for (auto i = (int)writeSet.size() - 1; i >= 0; i--) {
        auto &writeKey = writeSet[i];
        auto tableId = writeKey.get_table_id();
        auto partitionId = writeKey.get_partition_id();
        auto table = this->db.find_table(tableId, partitionId);
        auto key_size = table->key_size();
        auto field_size = table->field_size();
        auto owner_cluster_worker = partition_owner_cluster_worker(partitionId, txn.ith_replica);
        if (owner_cluster_worker == this_cluster_worker_id)
          continue;
        auto coordinatorId = this->partitioner->master_coordinator(partitionId);
        if (coordinator_covered[coordinatorId] == false) {
          coordinator_covered[coordinatorId] = true;
          //persist_commit_record[i] = true;
        }
      }
    }
    for (auto i = 0u; i < writeSet.size(); i++) {
      auto &writeKey = writeSet[i];
      auto tableId = writeKey.get_table_id();
      auto partitionId = writeKey.get_partition_id();
      auto owner_cluster_worker = partition_owner_cluster_worker(partitionId, txn.ith_replica);
      auto table = this->db.find_table(tableId, partitionId);

      // write
      if ((int)owner_cluster_worker == this_cluster_worker_id) {
        auto key = writeKey.get_key();
        auto value = writeKey.get_value();
        table->update(key, value);
      } else {
        //txn.pendingResponses++;
        messages[owner_cluster_worker]->set_transaction_id(txn.transaction_id);
        txn.network_size += MessageFactoryType::new_write_back_message(
            *messages[owner_cluster_worker], *table, writeKey.get_key(),
            writeKey.get_value(), this_cluster_worker_id, commit_tid, txn.ith_replica, persist_commit_record[i]);
        //LOG(INFO) << "Partition worker " << this_cluster_worker_id << " issueed write request on partition " << partitionId;
      }
    }
    if (txn.is_single_partition() == false) {
      txn.message_flusher();
      sync_messages(txn, true);
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
        txn.remote_request_handler();
        if (this->is_replica_worker) {
          std::this_thread::sleep_for(std::chrono::microseconds(10));
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
      if (local_index_read || txn.is_single_partition()) {
        success = true;
        remote = false;
        this->search(table_id, partition_id, key, value);
        return ;
      }
      this->parts_touched[partition_id] = true;
      this->parts_touched_tables[partition_id] = table_id;
      int owner_cluster_worker = partition_owner_cluster_worker(partition_id, txn.ith_replica);
      if ((int)owner_cluster_worker == this_cluster_worker_id) {
        remote = false;
        if (owned_partition_locked_by[partition_id] != -1 && owned_partition_locked_by[partition_id] != txn.transaction_id) {
          success = false;
          return;
        }
        // if (owned_partition_locked_by[partition_id] == -1 && is_replica_worker)
        //    LOG(INFO) << "Tranasction from worker " << this_cluster_worker_id << " locked partition " << partition_id << " txn " << tid_to_string(txn.transaction_id);;
        if (owned_partition_locked_by[partition_id] == -1)
          owned_partition_locked_by[partition_id] = txn.transaction_id;

        success = true;

        this->search(table_id, partition_id, key, value);

      } else {
        // bool in_parts = false;
        // for (auto i = 0; i < txn.get_partition_count(); ++i) {
        //   if ((int)partition_id == txn.get_partition(i)) {
        //     in_parts = true;
        //     break;
        //   }
        // }
        // DCHECK(in_parts);
        ITable *table = this->db.find_table(table_id, partition_id);

        remote = true;

        // LOG(INFO) << "Requesting locking partition " << partition_id << " by cluster worker" << this_cluster_worker_id << " on owner_cluster_worker " << owner_cluster_worker; 
        cluster_worker_messages[owner_cluster_worker]->set_transaction_id(txn.transaction_id);
        txn.network_size += MessageFactoryType::new_acquire_partition_lock_and_read_message(
              *(cluster_worker_messages[owner_cluster_worker]), *table, key, key_offset, this_cluster_worker_id, txn.ith_replica);
        txn.distributed_transaction = true;
        txn.pendingResponses++;
      }
    };

    txn.remote_request_handler = [this]() { return this->process_request(); };
    txn.message_flusher = [this]() { this->flush_messages(); };
    txn.get_table = [this](std::size_t tableId, std::size_t partitionId) { return this->db.find_table(tableId, partitionId); };
    txn.set_logger(this->logger);
  };

  using Transaction = TransactionType;


  void prepare_and_redo_request_handler(const Message & inputMessage, MessagePiece inputPiece,
                                    Message &responseMessage, ITable &table,
                                    Transaction *txn) {
    std::size_t lsn = 0;
    DCHECK(inputPiece.get_message_type() ==
           static_cast<uint32_t>(HStoreMessage::PREPARE_REDO_REQUEST));

    auto stringPiece = inputPiece.toStringPiece();
    Decoder dec(stringPiece);
    std::size_t redoWriteSetSize;
    std::size_t ith_replica;

    bool success = true;

    bool persist_log = false;
    dec >> persist_log >> ith_replica >> redoWriteSetSize;

    DCHECK(this->logger);
    if (ith_replica > 0)
      DCHECK(is_replica_worker);

    for (size_t i = 0; i < redoWriteSetSize; ++i) {
      uint64_t tableId;
      uint64_t partitionId;
      dec >> tableId >> partitionId;
      auto table = this->db.find_table(tableId, partitionId);
      std::size_t key_size, value_size;
      uint64_t tid;
      dec >> key_size;
      DCHECK(key_size == table->key_size());
      const void * key = dec.get_raw_ptr();
      dec.remove_prefix(key_size);
      dec >> value_size;
      DCHECK(value_size == table->value_size());
      const void * value = dec.get_raw_ptr();
      dec.remove_prefix(value_size);

      std::ostringstream ss;
      ss << tableId << partitionId << key_size << std::string((char*)key, key_size) << value_size << std::string((char*)value, value_size);
      auto output = ss.str();
      lsn = this->logger->write(output.c_str(), output.size(), false);
    }

    // prepare response message header
    auto message_size =
        MessagePiece::get_header_size() + sizeof(bool);
    auto message_piece_header = MessagePiece::construct_message_piece_header(
        static_cast<uint32_t>(HStoreMessage::PREPARE_REDO_RESPONSE),
        message_size, 0, 0);

    star::Encoder encoder(responseMessage.data);
    encoder << message_piece_header;
    encoder << success;
    if (ith_replica > 0)
      responseMessage.set_is_replica(true);
    responseMessage.set_transaction_id(inputMessage.get_transaction_id());
    responseMessage.flush();

    
    if (this->logger) {
      // write the vote
      std::ostringstream ss;
      ss << success;
      auto output = ss.str();
      lsn = this->logger->write(output.c_str(), output.size(), persist_log, [&, this](){ process_request(); });
    }

    if (persist_log && this->logger) {
      // sync the vote and redo
      // On recovery, the txn is considered prepared only if all votes are true // passed all validation
      //txn->get_logger()->sync(lsn, );
    }
  }

  static void prepare_and_redo_response_handler(const Message & inputMessage, MessagePiece inputPiece,
                                               Message &responseMessage,
                                               ITable &table,
                                               Transaction *txn) {
    DCHECK(inputPiece.get_message_type() ==
           static_cast<uint32_t>(HStoreMessage::PREPARE_REDO_RESPONSE));

    bool success;

    Decoder dec(inputPiece.toStringPiece());

    dec >> success;

    txn->pendingResponses--;
    txn->network_size += inputPiece.get_message_length();

    DCHECK(success);
  }

  static void prepare_request_handler(const Message & inputMessage, MessagePiece inputPiece,
                                    Message &responseMessage, ITable &table,
                                    Transaction *txn) {

    DCHECK(inputPiece.get_message_type() ==
           static_cast<uint32_t>(HStoreMessage::PREPARE_REQUEST));
    auto table_id = inputPiece.get_table_id();
    auto partition_id = inputPiece.get_partition_id();
    DCHECK(table_id == table.tableID());
    DCHECK(partition_id == table.partitionID());
    auto key_size = table.key_size();
    auto field_size = table.field_size();

    /*
     * The structure of a write request: (primary key, field value)
     * The structure of a write response: ()
     */

    DCHECK(inputPiece.get_message_length() ==
           MessagePiece::get_header_size() + sizeof(uint32_t));

    // prepare response message header
    auto message_size = MessagePiece::get_header_size();
    auto message_piece_header = MessagePiece::construct_message_piece_header(
        static_cast<uint32_t>(HStoreMessage::PREPARE_RESPONSE), message_size,
        table_id, partition_id);

    star::Encoder encoder(responseMessage.data);
    encoder << message_piece_header;
    responseMessage.set_transaction_id(inputMessage.get_transaction_id());
    responseMessage.flush();
    responseMessage.set_gen_time(Time::now());
  }

  static void prepare_response_handler(const Message & inputMessage, MessagePiece inputPiece,
                                    Message &responseMessage, ITable &table,
                                    Transaction *txn) {

    DCHECK(inputPiece.get_message_type() ==
           static_cast<uint32_t>(HStoreMessage::PREPARE_RESPONSE));
    auto table_id = inputPiece.get_table_id();
    auto partition_id = inputPiece.get_partition_id();
    DCHECK(table_id == table.tableID());
    DCHECK(partition_id == table.partitionID());
    auto key_size = table.key_size();
    auto field_size = table.field_size();

    /*
     * The structure of a write request: (primary key, field value)
     * The structure of a write response: ()
     */

    DCHECK(inputPiece.get_message_length() ==
           MessagePiece::get_header_size());
    DCHECK(txn->pendingResponses > 0);
    txn->pendingResponses--;
  }

  void acquire_partition_lock_request_handler(const Message & inputMessage, MessagePiece inputPiece,
                                          Message &responseMessage,
                                          ITable &table, Transaction *txn) {
    DCHECK(inputPiece.get_message_type() ==
           static_cast<uint32_t>(HStoreMessage::ACQUIRE_PARTITION_LOCK_REQUEST));
    auto table_id = inputPiece.get_table_id();
    auto partition_id = inputPiece.get_partition_id();
    DCHECK(table_id == table.tableID());
    DCHECK(partition_id == table.partitionID());
    int64_t tid = inputMessage.get_transaction_id();
    /*
     * The structure of a write lock request: (request_remote_worker_id, ith_replica)
     * The structure of a write lock response: (success?)
     */
    uint32_t source_remote_worker_id, request_remote_worker_id;
    auto stringPiece = inputPiece.toStringPiece();
    std::size_t ith_replica;

    DCHECK(inputPiece.get_message_length() ==
           MessagePiece::get_header_size() + sizeof(uint32_t) * 2 + sizeof(std::size_t));

    star::Decoder dec(stringPiece);
    dec >> source_remote_worker_id >> request_remote_worker_id >> ith_replica;
    if (ith_replica > 0)
      DCHECK(is_replica_worker);

    DCHECK((int)partition_owner_cluster_worker(partition_id, ith_replica) == this_cluster_worker_id);

    DCHECK(dec.size() == 0);
    bool success = false;
    if (owned_partition_locked_by[partition_id] == -1 || owned_partition_locked_by[partition_id] == tid) {
      //lock it;
      // if (owned_partition_locked_by[partition_id] == -1)
      //    LOG(INFO) << "Partition " << partition_id << " locked by remote cluster worker " << request_remote_worker_id << " from source_remote_worker_id " << source_remote_worker_id << " by this_cluster_worker_id " << this_cluster_worker_id << " txn " << tid_to_string(tid);
      owned_partition_locked_by[partition_id] = tid;
      success = true;
    } else {
      //  LOG(INFO) << "Partition " << partition_id << " was failed to be locked by cluster worker " 
      //            << request_remote_worker_id  << " from source_remote_worker_id " << source_remote_worker_id
      //            << " already locked by " << tid_to_string(owned_partition_locked_by[partition_id]);
    }
    // prepare response message header
    auto message_size =
        MessagePiece::get_header_size() + sizeof(bool);

    auto message_piece_header = MessagePiece::construct_message_piece_header(
        static_cast<uint32_t>(HStoreMessage::ACQUIRE_PARTITION_LOCK_RESPONSE), message_size,
        table_id, partition_id);

    star::Encoder encoder(responseMessage.data);
    encoder << message_piece_header;
    encoder << success;

    responseMessage.set_is_replica(ith_replica > 0);
    responseMessage.set_transaction_id(inputMessage.get_transaction_id());
    responseMessage.flush();
    responseMessage.set_gen_time(Time::now());
  }


  void acquire_partition_lock_and_read_request_handler(const Message & inputMessage, MessagePiece inputPiece,
                                          Message &responseMessage,
                                          ITable &table, Transaction *txn) {
    DCHECK(inputPiece.get_message_type() ==
           static_cast<uint32_t>(HStoreMessage::ACQUIRE_PARTITION_LOCK_AND_READ_REQUEST));
    auto table_id = inputPiece.get_table_id();
    auto partition_id = inputPiece.get_partition_id();
    DCHECK(table_id == table.tableID());
    DCHECK(partition_id == table.partitionID());
    auto key_size = table.key_size();
    auto value_size = table.value_size();
    int64_t tid = inputMessage.get_transaction_id();
    
    /*
     * The structure of a write lock request: (primary key, key offset, request_remote_worker_id, ith_replica)
     * The structure of a write lock response: (success?, key offset, value?)
     */
    uint32_t request_remote_worker_id;
    auto stringPiece = inputPiece.toStringPiece();
    uint32_t key_offset;
    std::size_t ith_replica;

    DCHECK(inputPiece.get_message_length() ==
           MessagePiece::get_header_size() + key_size + sizeof(key_offset) + sizeof(uint32_t) + sizeof(std::size_t));

    const void *key = stringPiece.data();
    auto row = table.search(key);

    stringPiece.remove_prefix(key_size);
    star::Decoder dec(stringPiece);
    dec >> key_offset >> request_remote_worker_id >> ith_replica;

    if (ith_replica > 0)
      DCHECK(is_replica_worker);

    DCHECK((int)partition_owner_cluster_worker(partition_id, ith_replica) == this_cluster_worker_id);

    DCHECK(dec.size() == 0);
    bool success = false;
    if (owned_partition_locked_by[partition_id] == -1 || owned_partition_locked_by[partition_id] == tid) {
      //lock it;
      // if (owned_partition_locked_by[partition_id] == -1)
      //    LOG(INFO) << "Partition " << partition_id << " locked and read by remote cluster worker " << request_remote_worker_id << " by this_cluster_worker_id " << this_cluster_worker_id << " ith_replica "  << ith_replica << " txn " << tid_to_string(tid);
      owned_partition_locked_by[partition_id] = tid;
      success = true;
    } else {
      //  LOG(INFO) << "Partition " << partition_id << " was failed to be locked by cluster worker " << request_remote_worker_id << " and txn " << tid_to_string(tid)
//                 << " already locked by " << tid_to_string(owned_partition_locked_by[partition_id]) << " ith_replica" << ith_replica;
    }
    // prepare response message header
    auto message_size =
        MessagePiece::get_header_size() + sizeof(bool) + sizeof(key_offset);

    if (success) {
      message_size += value_size;
    } else{
      // LOG(INFO) << "acquire_partition_lock_request from cluster worker " << request_remote_worker_id
      //            << " on partition " << partition_id
      //            << " partition locked acquired faliled, lock owned by " << owned_partition_locked_by[partition_id];
    }
    
    auto message_piece_header = MessagePiece::construct_message_piece_header(
        static_cast<uint32_t>(HStoreMessage::ACQUIRE_PARTITION_LOCK_AND_READ_RESPONSE), message_size,
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

    responseMessage.set_is_replica(ith_replica > 0);
    responseMessage.set_transaction_id(inputMessage.get_transaction_id());
    responseMessage.flush();
    responseMessage.set_gen_time(Time::now());
  }


  void command_replication_request_handler(const Message & inputMessage, MessagePiece inputPiece,
                                   Message &responseMessage,
                                   ITable &table, Transaction *txn) {
    DCHECK(is_replica_worker);
    DCHECK(inputPiece.get_message_type() ==
           static_cast<uint32_t>(HStoreMessage::COMMAND_REPLICATION_REQUEST));

    auto key_size = table.key_size();
    auto value_size = table.value_size();
    
    /*
     * The structure of a write command replication request: (ith_replica, txn data)
     * The structure of a write lock response: (success?, key offset, value?)
     */
    std::size_t ith_replica;
    int initiating_cluster_worker_id;
    auto stringPiece = inputPiece.toStringPiece();
    Decoder dec(stringPiece);
    dec >> ith_replica >> initiating_cluster_worker_id;
    std::string data = dec.bytes.toString();
    DCHECK(inputPiece.get_message_length() ==
             MessagePiece::get_header_size() + sizeof(std::size_t) + data.size() + sizeof(initiating_cluster_worker_id));
    std::size_t data_sz = data.size();
    std::unique_ptr<TransactionType> new_txn = this->workload.deserialize_from_raw(this->context, data);
    new_txn->initiating_cluster_worker_id = initiating_cluster_worker_id;
    new_txn->initiating_transaction_id = inputMessage.get_transaction_id();
    new_txn->transaction_id = WorkloadType::next_transaction_id(this->context.coordinator_id);
    auto partition_id = new_txn->partition_id;
    DCHECK((int)partition_owner_cluster_worker(partition_id, ith_replica) == this_cluster_worker_id);

    // auto txn_command_data = new_txn->serialize(ith_replica);
    // // Persist txn command
    // this->logger->write(txn_command_data.c_str(), txn_command_data.size(), true, [&, this]() {process_request();});

    queuedTxns.emplace_back(new_txn.release());

    // auto message_size = MessagePiece::get_header_size();

    // auto message_piece_header = MessagePiece::construct_message_piece_header(
    //     static_cast<uint32_t>(HStoreMessage::COMMAND_REPLICATION_RESPONSE), message_size,
    //     0, 0);

    // star::Encoder encoder(responseMessage.data);
    // encoder << message_piece_header;
  
    // responseMessage.flush();
    // responseMessage.set_gen_time(Time::now());
  }

  void process_batch_of_replicated_sp_transactions(const std::vector<TransactionType*> & txns, 
                                                   const std::string & command_data_all) {
    retry:
    process_request(false);
    for (std::size_t i = 0; i < txns.size(); ++i) {
      DCHECK(txns[i]->is_single_partition());
      int partition_id = txns[i]->get_partition(0);
      if (owned_partition_locked_by[partition_id] != -1) {
        goto retry;
      }
    }

    for (size_t i = 0; i < txns.size(); ++i) {
      int partition_id = txns[i]->get_partition(0);
      owned_partition_locked_by[partition_id] = txns[i]->transaction_id;
    }

    auto txn_cmd_log_lsn = this->logger->write(command_data_all.c_str(), command_data_all.size(), false);
    this->logger->sync(txn_cmd_log_lsn, [&, this]() {process_request(false);});

    // Execution
    execute_sp_transaction_batch(txns);

    // Clean-up
    for (size_t i = 0; i < txns.size(); ++i) {
      DCHECK(txns[i]->is_single_partition());
      int partition_id = txns[i]->get_partition(0);
      owned_partition_locked_by[partition_id] = -1;
    }
  }

  void command_replication_sp_request_handler(const Message & inputMessage, MessagePiece inputPiece,
                                   Message &responseMessage,
                                   ITable &table, Transaction *txn) {
    DCHECK(is_replica_worker);
    DCHECK(inputPiece.get_message_type() ==
           static_cast<uint32_t>(HStoreMessage::COMMAND_REPLICATION_SP_REQUEST));

    auto key_size = table.key_size();
    auto value_size = table.value_size();

    /*
     * The structure of a write command replication request: (ith_replica, txn data)
     * The structure of a write lock response: (success?, key offset, value?)
     */
    std::size_t ith_replica;
    int initiating_cluster_worker_id;
    auto stringPiece = inputPiece.toStringPiece();
    std::size_t num_commands = 0;
    Decoder dec(stringPiece);
    dec >> ith_replica >> initiating_cluster_worker_id >> num_commands;
    std::vector<TransactionType*> new_txns;
    std::string command_data_all;
    for (std::size_t i = 0; i < num_commands; ++i) {
      std::size_t command_size;
      dec >> command_size;
      std::string command_data(dec.bytes.data(), command_size);
      dec.remove_prefix(command_size);
      command_data_all += command_data;
      auto new_txn = this->workload.deserialize_from_raw(this->context, command_data);
      new_txn->initiating_cluster_worker_id = initiating_cluster_worker_id;
      new_txn->initiating_transaction_id = inputMessage.get_transaction_id();
      new_txn->transaction_id = WorkloadType::next_transaction_id(this->context.coordinator_id);
      auto partition_id = new_txn->partition_id;
      DCHECK((int)partition_owner_cluster_worker(partition_id, ith_replica) == this_cluster_worker_id);
      new_txn->replicated_sp = true;
      queuedTxns.emplace_back(new_txn.release());
    }

    //process_batch_of_replicated_sp_transactions(new_txns, command_data_all);

    // auto message_size = MessagePiece::get_header_size();

    // auto message_piece_header = MessagePiece::construct_message_piece_header(
    //     static_cast<uint32_t>(HStoreMessage::COMMAND_REPLICATION_SP_RESPONSE), message_size,
    //     0, 0);

    // star::Encoder encoder(responseMessage.data);
    // encoder << message_piece_header;
    // responseMessage.set_transaction_id(new_txns[0]->transaction_id);

    // responseMessage.flush();
    // responseMessage.set_gen_time(Time::now());
  }


  void command_replication_sp_response_handler(const Message & inputMessage, MessagePiece inputPiece,
                                            Message &responseMessage,
                                            ITable &table, Transaction *txn) {
    DCHECK(inputPiece.get_message_type() ==
           static_cast<uint32_t>(HStoreMessage::COMMAND_REPLICATION_SP_RESPONSE));
    DCHECK(is_replica_worker == false);
    received_sp_replication_responses++;
  }

  void command_replication_response_handler(const Message & inputMessage, MessagePiece inputPiece,
                                            Message &responseMessage,
                                            ITable &table, Transaction *txn) {
    DCHECK(inputPiece.get_message_type() ==
           static_cast<uint32_t>(HStoreMessage::COMMAND_REPLICATION_RESPONSE));
    DCHECK(is_replica_worker == false);
    txn->pendingResponses--;
    txn->network_size += inputPiece.get_message_length();
  }

  void acquire_partition_lock_and_read_response_handler(const Message & inputMessage, MessagePiece inputPiece,
                                          Message &responseMessage,
                                          ITable &table, Transaction *txn) {
    DCHECK(inputPiece.get_message_type() ==
           static_cast<uint32_t>(HStoreMessage::ACQUIRE_PARTITION_LOCK_AND_READ_RESPONSE));
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

  void acquire_partition_lock_response_handler(const Message & inputMessage, MessagePiece inputPiece,
                                          Message &responseMessage,
                                          ITable &table, Transaction *txn) {
    DCHECK(inputPiece.get_message_type() ==
           static_cast<uint32_t>(HStoreMessage::ACQUIRE_PARTITION_LOCK_RESPONSE));
    auto table_id = inputPiece.get_table_id();
    auto partition_id = inputPiece.get_partition_id();
    DCHECK(table_id == table.tableID());
    DCHECK(partition_id == table.partitionID());

    /*
     * The structure of a read lock response: (success?)
     */

    bool success;

    StringPiece stringPiece = inputPiece.toStringPiece();
    Decoder dec(stringPiece);
    dec >> success;

    if (success) {
      uint32_t msg_length = inputPiece.get_message_length();
      auto header_size = MessagePiece::get_header_size() ;
      uint32_t exp_length = header_size + sizeof(success);
      DCHECK(msg_length == exp_length);
    } else {
      DCHECK(inputPiece.get_message_length() ==
             MessagePiece::get_header_size() + sizeof(success));

      txn->abort_lock = true;
    }
    txn->pendingResponses--;
    txn->network_size += inputPiece.get_message_length();
    // LOG(INFO) << "acquire_partition_lock_response for worker " << this_cluster_worker_id
    //           << " on partition " << partition_id
    //           << " partition locked acquired " << success 
    //           << " pending responses " << txn->pendingResponses;
  }

  void write_back_request_handler(const Message & inputMessage, MessagePiece inputPiece,
                                  Message &responseMessage,
                                  ITable &table, Transaction *txn) {
    DCHECK(inputPiece.get_message_type() ==
           static_cast<uint32_t>(HStoreMessage::WRITE_BACK_REQUEST));
    auto table_id = inputPiece.get_table_id();
    auto partition_id = inputPiece.get_partition_id();
    DCHECK(table_id == table.tableID());
    DCHECK(partition_id == table.partitionID());
    auto key_size = table.key_size();
    auto field_size = table.field_size();
    int64_t tid = inputMessage.get_transaction_id();
    /*
     * The structure of a write request: (request_remote_worker, primary key, field value)
     * The structure of a write response: (success?)
     */
    uint32_t request_remote_worker;

    auto stringPiece = inputPiece.toStringPiece();
    uint64_t commit_tid;
    bool persist_commit_record;
    std::size_t ith_replica;

    Decoder dec(stringPiece);
    dec >> commit_tid >> persist_commit_record >> request_remote_worker >> ith_replica;
    if (ith_replica)
      DCHECK(is_replica_worker);
    bool success = false;
    DCHECK(this_cluster_worker_id == (int)partition_owner_cluster_worker(partition_id, ith_replica));

    // Make sure the partition is currently owned by request_remote_worker
    if (owned_partition_locked_by[partition_id] == tid) {
      success = true;
    }

    // LOG(INFO) << "write_back_request_handler for worker " << request_remote_worker
    //   << " on partition " << partition_id
    //   << " partition locked acquired " << success
    //   << " current partition owner " << tid_to_string(owned_partition_locked_by[partition_id])
    //   << " ith_replica " << ith_replica << " txn " << tid_to_string(inputMessage.get_transaction_id());

    DCHECK(owned_partition_locked_by[partition_id] == tid);

    if (success) {
      stringPiece = dec.bytes;
      DCHECK(inputPiece.get_message_length() ==
      MessagePiece::get_header_size() + sizeof(ith_replica) + sizeof(commit_tid) + sizeof(persist_commit_record) + key_size + field_size + sizeof(uint32_t));
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
    if (ith_replica > 0)
      responseMessage.set_is_replica(true);
    responseMessage.set_transaction_id(inputMessage.get_transaction_id());
    responseMessage.flush();
    responseMessage.set_gen_time(Time::now());

    if (persist_commit_record) {
      DCHECK(this->logger);
      std::ostringstream ss;
      ss << commit_tid << true;
      auto output = ss.str();
      auto lsn = this->logger->write(output.c_str(), output.size(), false, [&, this](){ process_request(); });
      //txn->get_logger()->sync(lsn, );
    }
  }


  void write_back_response_handler(const Message & inputMessage, MessagePiece inputPiece,
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

    //txn->pendingResponses--;
    //txn->network_size += inputPiece.get_message_length();
    // LOG(INFO) << "write_back_response_handler for worker " << this_cluster_worker_id
    //   << " on partition " << partition_id
    //   << " remote partition locked released " << success 
    //   << " pending responses " << txn->pendingResponses;
  }

  void persist_cmd_buffer_request_handler(const Message & inputMessage, MessagePiece inputPiece,
                                          Message &responseMessage,
                                          ITable &table, Transaction *txn) {
    DCHECK(inputPiece.get_message_type() ==
           static_cast<uint32_t>(HStoreMessage::PERSIST_CMD_BUFFER_REQUEST));
    DCHECK(inputPiece.get_message_length() ==
             MessagePiece::get_header_size() + sizeof(int) + sizeof(int));
    auto table_id = inputPiece.get_table_id();
    auto partition_id = inputPiece.get_partition_id();
    persist_and_clear_command_buffer();
    int cluster_worker_id, ith_replica;
    auto stringPiece = inputPiece.toStringPiece();

    Decoder dec(stringPiece);
    dec >> cluster_worker_id >> ith_replica;
    //LOG(INFO) << "cluster_worker " << cluster_worker_id << " called persist_cmd_buffer_request_handler on cluster worker " << this_cluster_worker_id;
    auto message_size = MessagePiece::get_header_size() + sizeof(this_cluster_worker_id);
    auto message_piece_header = MessagePiece::construct_message_piece_header(
        static_cast<uint32_t>(HStoreMessage::PERSIST_CMD_BUFFER_RESPONSE), message_size,
        table_id, partition_id);

    star::Encoder encoder(responseMessage.data);
    encoder << message_piece_header << this_cluster_worker_id;
    responseMessage.set_transaction_id(inputMessage.get_transaction_id());
    responseMessage.set_is_replica(ith_replica > 0);
    responseMessage.flush();
    responseMessage.set_gen_time(Time::now());
  }

  void persist_cmd_buffer_response_handler(const Message & inputMessage, MessagePiece inputPiece,
                                          Message &responseMessage,
                                          ITable &table, Transaction *txn) {
    DCHECK(inputPiece.get_message_type() ==
           static_cast<uint32_t>(HStoreMessage::PERSIST_CMD_BUFFER_RESPONSE));
    DCHECK(inputPiece.get_message_length() ==
             MessagePiece::get_header_size() + sizeof(int));
    auto table_id = inputPiece.get_table_id();
    auto partition_id = inputPiece.get_partition_id();
    auto stringPiece = inputPiece.toStringPiece();
    int cluster_worker_id;
    Decoder dec(stringPiece);
    dec >> cluster_worker_id;
    this->received_persist_cmd_buffer_response++;
    //LOG(INFO) << "cluster_worker " << this_cluster_worker_id << " received response from persist_cmd_buffer_request_handler on cluster worker " << cluster_worker_id << ", received_persist_cmd_buffer_response " << received_persist_cmd_buffer_response;
  }

  void release_partition_lock_request_handler(const Message & inputMessage, MessagePiece inputPiece,
                                          Message &responseMessage,
                                          ITable &table, Transaction *txn) {
    DCHECK(inputPiece.get_message_type() ==
           static_cast<uint32_t>(HStoreMessage::RELEASE_PARTITION_LOCK_REQUEST));
    auto table_id = inputPiece.get_table_id();
    auto partition_id = inputPiece.get_partition_id();
    DCHECK(table_id == table.tableID());
    DCHECK(partition_id == table.partitionID());
    auto key_size = table.key_size();
    auto field_size = table.field_size();

    int64_t tid = inputMessage.get_transaction_id();
    /*
     * The structure of a release partition lock request: (request_remote_worker, sync, ith_replica, write_cmd_buffer)
     * No response.
     */
    uint32_t request_remote_worker;
    bool sync, write_cmd_buffer;
    std::size_t ith_replica;

    auto stringPiece = inputPiece.toStringPiece();

    Decoder dec(stringPiece);
    dec >> request_remote_worker >> sync >> ith_replica >> write_cmd_buffer;
    if (write_cmd_buffer) {
      std::string txn_command_data;
      std::size_t txn_command_data_size;

      dec >> txn_command_data_size;
      if (txn_command_data_size) {
        txn_command_data = std::string(dec.bytes.data(), txn_command_data_size);
      }
      command_buffer.push_back({inputMessage.get_transaction_id(), request_remote_worker, txn_command_data});
    }
  
    DCHECK(this_cluster_worker_id == (int)partition_owner_cluster_worker(partition_id, ith_replica));
    if (ith_replica > 0)
      DCHECK(is_replica_worker);
    bool success;
    if (owned_partition_locked_by[partition_id] != tid) {
      success = false;
    } else {
      // if (owned_partition_locked_by[partition_id] != -1)
      //   LOG(INFO) << "Partition " << partition_id << " unlocked by cluster worker" << request_remote_worker << " by this_cluster_worker_id " << this_cluster_worker_id << " ith_replica " << ith_replica << " txn " << tid_to_string(tid);
      owned_partition_locked_by[partition_id] = -1;
      success = true;
    }
    // LOG(INFO) << "release_partition_lock_request_handler from worker " << this_cluster_worker_id
    //   << " on partition " << partition_id << " by " << tid_to_string(tid)
    //   << ", lock released " << success;
    if (!sync)
      return;
    // prepare response message header
    auto message_size = MessagePiece::get_header_size() + sizeof(bool);
    auto message_piece_header = MessagePiece::construct_message_piece_header(
        static_cast<uint32_t>(HStoreMessage::RELEASE_PARTITION_LOCK_RESPONSE), message_size,
        table_id, partition_id);

    star::Encoder encoder(responseMessage.data);
    encoder << message_piece_header << success;
    if (ith_replica > 0)
      responseMessage.set_is_replica(true);
    responseMessage.set_transaction_id(inputMessage.get_transaction_id());
    responseMessage.flush();
    responseMessage.set_gen_time(Time::now());
  }

  void release_partition_lock_response_handler(const Message & inputMessage, MessagePiece inputPiece,
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
  void fill_pending_txns(std::size_t limit) {
    while (pending_txns.size() < limit) {
      auto t = get_next_transaction();
      if (t == nullptr)
        break;
      pending_txns.push_back(t);
    }
  }

  void reorder_pending_txns() {
    if (pending_txns[0]->reordered_in_the_queue)
      return;
    size_t until_ith = 0;
    for (; until_ith < pending_txns.size(); ++until_ith) {
      if (pending_txns[until_ith]->reordered_in_the_queue) {
        break;
      }
      pending_txns[until_ith]->reordered_in_the_queue = true;
    }
    if (until_ith == 2)
      return;
    size_t sp_idx = 0;
    // This loop rearranges the single partitions transactions ending at until_ith to the front of the queue
    for (size_t i = sp_idx; i < until_ith; ++i) {
      if (pending_txns[i]->is_single_partition()) {
        std::swap(pending_txns[sp_idx++], pending_txns[i]);
      }
    }
    // size_t sp_and_unlocked_idx = 0;
    // // This loop rearranges all the unlocked single partitions transactions ending at sp_idx to the front of the queue
    // for (size_t i = sp_and_unlocked_idx; i < sp_idx; ++i) {
    //   if (pending_txns[i]->is_single_partition() && owned_partition_locked_by[pending_txns[i]->get_partition(0)] == -1) {
    //     std::swap(pending_txns[sp_and_unlocked_idx++], pending_txns[i]);
    //   }
    // }
  }

  void execute_sp_transaction_batch(const std::vector<TransactionType*> & txns) {
    for (size_t i = 0; i < txns.size(); ++i) {
      auto txn = txns[i];
      DCHECK(txn->is_single_partition());
      int partition_id = txn->get_partition(0);
      setupHandlers(*txn);
      active_txns[txn->transaction_id] = txn;
      auto ltc =
      std::chrono::duration_cast<std::chrono::nanoseconds>(
          std::chrono::steady_clock::now() - txn->startTime)
          .count();
      txn->set_stall_time(ltc);
      DCHECK((int)partition_owner_cluster_worker(partition_id, txn->ith_replica) == this_cluster_worker_id);

      auto result = txn->execute(this->id);

      if (result == TransactionResult::READY_TO_COMMIT) {
        bool commit;
        {
          ScopedTimer t([&, this](uint64_t us) {
            if (commit) {
              txn->record_commit_work_time(us);
            } else {
              auto ltc =
              std::chrono::duration_cast<std::chrono::nanoseconds>(
                  std::chrono::steady_clock::now() - txn->startTime)
                  .count();
              txn->set_stall_time(ltc);
            }
          });
          commit = this->commit_sp(*txn, cluster_worker_messages);
        }
        DCHECK(txn->abort_lock == false);
        ////LOG(INFO) << "Txn Execution result " << (int)result << " commit " << commit;
        this->n_network_size.fetch_add(txn->network_size);
        if (commit) {
          ++worker_commit;
          this->n_commit.fetch_add(1);
          if (txn->si_in_serializable) {
            this->n_si_in_serializable.fetch_add(1);
          }
          auto latency =
              std::chrono::duration_cast<std::chrono::microseconds>(
                  std::chrono::steady_clock::now() - txn->startTime)
                  .count();
          this->percentile.add(latency);
          if (txn->distributed_transaction) {
            this->dist_latency.add(latency);
          } else {
            this->local_latency.add(latency);
          }
          // Make sure it is unlocked.
          // if (txn->is_single_partition())
          //   DCHECK(owned_partition_locked_by[partition_id] == -1);
          this->record_txn_breakdown_stats(*txn);
          active_txns.erase(txn->transaction_id);
        } else {
          DCHECK(false);
          //LOG(INFO) << "Txn Execution result " << (int)result << " abort ";
          // Txns on slave replicas won't abort due to locking failure.
          if (txn->abort_lock) {
            this->n_abort_lock.fetch_add(1);
          } else {
            DCHECK(txn->abort_read_validation);
            this->n_abort_read_validation.fetch_add(1);
          }
          if (this->context.sleep_on_retry) {
            std::this_thread::sleep_for(std::chrono::microseconds(
                this->random.uniform_dist(0, 10)));
          }
          this->random.set_seed(0);
          //retry_transaction = true;
          active_txns.erase(txn->transaction_id);
        }
      } else {
        //LOG(INFO) << "Txn Execution result " << (int)result << " abort ";
        //this->abort(*txn, cluster_worker_messages);
        this->n_abort_no_retry.fetch_add(1);
        //retry_transaction = false;
        // Make sure it is unlocked.
        // if (txn->is_single_partition())
        //   DCHECK(owned_partition_locked_by[partition_id] == -1);
        active_txns.erase(txn->transaction_id);
      }
    }
  }

  void persist_and_clear_command_buffer() {
    if (command_buffer.empty())
      return;
    std::string data;
    Encoder encoder(data);
    for (size_t i = 0; i < command_buffer.size(); ++i) {
      encoder << command_buffer[i].tid;
      encoder << command_buffer[i].coordinator_worker;
      encoder << command_buffer[i].command_data.size();
      encoder.write_n_bytes(command_buffer[i].command_data.data(), command_buffer[i].command_data.size());
    }
    command_buffer.clear();
    this->logger->write(encoder.toStringPiece().data(), encoder.toStringPiece().size(), true);
  }

  void execute_mp_transaction_batch(const std::vector<TransactionType*> & txns) {
    std::string txn_command_data_all;
    std::vector<bool> workers_need_persist_cmd_buffer(this->cluster_worker_num);
    int64_t txn_id;
    for (size_t i = 0; i < txns.size(); ++i) {
      auto txn = txns[i];
      txn_id = txn->transaction_id;
      DCHECK(!txn->is_single_partition());
      setupHandlers(*txn);
      active_txns[txn->transaction_id] = txn;
      auto ltc =
      std::chrono::duration_cast<std::chrono::nanoseconds>(
          std::chrono::steady_clock::now() - txn->startTime)
          .count();
      txn->set_stall_time(ltc);
      
      size_t partition_count = txn->get_partition_count();
      
      for (size_t j = 0; j < partition_count; ++j) {
        workers_need_persist_cmd_buffer[partition_owner_cluster_worker(txn->get_partition(j), 0)] = true;
      }

      auto result = txn->execute(this->id);

      if (result == TransactionResult::READY_TO_COMMIT) {
        bool commit;
        {
          ScopedTimer t([&, this](uint64_t us) {
            if (commit) {
              txn->record_commit_work_time(us);
            } else {
              auto ltc =
              std::chrono::duration_cast<std::chrono::nanoseconds>(
                  std::chrono::steady_clock::now() - txn->startTime)
                  .count();
              txn->set_stall_time(ltc);
            }
          });
          commit = this->commit_mp(*txn, cluster_worker_messages);
        }
        ////LOG(INFO) << "Txn Execution result " << (int)result << " commit " << commit;
        this->n_network_size.fetch_add(txn->network_size);
        if (commit) {
          ++worker_commit;
          this->n_commit.fetch_add(1);
          if (txn->si_in_serializable) {
            this->n_si_in_serializable.fetch_add(1);
          }
          active_txns.erase(txn->transaction_id);
          command_buffer.push_back({txn->transaction_id, this_cluster_worker_id, txn->serialize(0)});
        } else {
          //LOG(INFO) << "Txn Execution result " << (int)result << " abort ";
          // Txns on slave replicas won't abort due to locking failure.
          if (txn->abort_lock) {
            this->n_abort_lock.fetch_add(1);
          } else {
            DCHECK(txn->abort_read_validation);
            this->n_abort_read_validation.fetch_add(1);
          }
          if (this->context.sleep_on_retry) {
            // std::this_thread::sleep_for(std::chrono::microseconds(
            //     this->random.uniform_dist(1000, 10000)));
            process_request();
          }
          //retry_transaction = true;
          active_txns.erase(txn->transaction_id);
          --i;
          txn->reset();
        }
      } else {
        //LOG(INFO) << "Txn Execution result " << (int)result << " abort ";
        this->abort(*txn, cluster_worker_messages, true);
        this->n_abort_no_retry.fetch_add(1);
        //retry_transaction = false;
        // Make sure it is unlocked.
        // if (txn->is_single_partition())
        //   DCHECK(owned_partition_locked_by[partition_id] == -1);
        active_txns.erase(txn->transaction_id);
      }
    }

    DCHECK((int)workers_need_persist_cmd_buffer.size() == this->cluster_worker_num);
    for (int i = 0; i < (int)workers_need_persist_cmd_buffer.size(); ++i) {
      if (!workers_need_persist_cmd_buffer[i] || i == this_cluster_worker_id)
        continue;
      cluster_worker_messages[i]->set_transaction_id(txn_id);
      MessageFactoryType::new_persist_cmd_buffer_message(*cluster_worker_messages[i], 0, this_cluster_worker_id);
      sent_persist_cmd_buffer_requests++;
    }
    flush_messages();
    persist_and_clear_command_buffer();
    // while (received_persist_cmd_buffer_response < sent_persist_cmd_buffer_requests) {
    //   process_request(false);
    // }

    for (size_t i = 0; i < txns.size(); ++i) {
      auto txn = txns[i];
      DCHECK(!txn->is_single_partition());
      auto latency =
          std::chrono::duration_cast<std::chrono::microseconds>(
              std::chrono::steady_clock::now() - txn->startTime)
              .count();
      this->percentile.add(latency);
      if (txn->distributed_transaction) {
        this->dist_latency.add(latency);
      } else {
        this->local_latency.add(latency);
      }
      // Make sure it is unlocked.
      // if (txn->is_single_partition())
      //   DCHECK(owned_partition_locked_by[partition_id] == -1);
      this->record_txn_breakdown_stats(*txn);
    }
    this->mp_round_concurrency.add(txns.size());
  }

  void process_batch_of_mp_transactions() {
    if (pending_txns.empty())
      return;
    std::size_t until_ith = 0;
    for (; until_ith < pending_txns.size(); ++until_ith) {
      if (pending_txns[until_ith]->is_single_partition()) {
        break;
      }
    }
    if (until_ith <= 0)
      return;

    // Execution
    execute_mp_transaction_batch(std::vector<TransactionType*>(pending_txns.begin(), pending_txns.begin() + until_ith));

    DCHECK(until_ith <= pending_txns.size());
    for (std::size_t i = 0; i < until_ith; ++i) {
      std::unique_ptr<TransactionType> txn(pending_txns.front());
      pending_txns.pop_front();
    }
  }

  void process_batch_of_sp_transactions() {
    if (pending_txns.empty())
      return;
    if (is_replica_worker == false) {
      reorder_pending_txns();
    }
    std::size_t until_ith = 0;
    for (; until_ith < pending_txns.size(); ++until_ith) {
      if (pending_txns[until_ith]->is_single_partition()) {
        int partition_id = pending_txns[until_ith]->get_partition(0);
        if (owned_partition_locked_by[partition_id] != -1) {
          break;
        }
      } else {
        break;
      }
    }

    if (until_ith <= 0)
      return;
    //LOG(INFO) << " batch_size " << until_ith;
    // Locking & Logging
    std::string txn_command_data_all;

    for (size_t i = 0; i < until_ith; ++i) {
      int partition_id = pending_txns[i]->get_partition(0);
      owned_partition_locked_by[partition_id] = pending_txns[i]->transaction_id;
      auto command_data = pending_txns[i]->serialize(0);
      txn_command_data_all += command_data;
    }

    if (this->partitioner->replica_num() > 1) {
      if (is_replica_worker == false) {
        std::vector<std::string> commands_data;
        for (size_t i = 0; i < until_ith; ++i) {
          auto command_data = pending_txns[i]->serialize(1);
          commands_data.push_back(command_data);
        }
        auto partition_id_txn_0 = pending_txns[0]->get_partition(0);
        auto txn0_tid = pending_txns[0]->transaction_id;
        auto replica_target_cluster_worker_id = partition_owner_cluster_worker(partition_id_txn_0, 1);
        for (size_t i = 0; i < until_ith; ++i) {
          int partition_id = pending_txns[i]->get_partition(0);
          DCHECK(partition_owner_cluster_worker(partition_id, 1) == replica_target_cluster_worker_id);
        }
        cluster_worker_messages[replica_target_cluster_worker_id]->set_transaction_id(txn0_tid);
        MessageFactoryType::new_command_replication_sp(
                *cluster_worker_messages[replica_target_cluster_worker_id], 1, commands_data, this_cluster_worker_id);
        flush_messages();
        sent_sp_replication_requests += until_ith;
      } else {
        for (size_t i = 0; i < until_ith; ++i) {
          auto partition_id = pending_txns[i]->get_partition(0);
          DCHECK(pending_txns[i]->replicated_sp);
          auto txn_id = pending_txns[i]->transaction_id;
          auto initiating_cluster_worker_id = pending_txns[i]->initiating_cluster_worker_id;
          cluster_worker_messages[initiating_cluster_worker_id]->set_transaction_id(txn_id);
          pending_txns[i]->network_size += MessageFactoryType::new_command_replication_sp_response_message(
              *cluster_worker_messages[initiating_cluster_worker_id]);
          flush_messages();
        }
      }
    }

    auto txn_cmd_log_lsn = this->logger->write(txn_command_data_all.c_str(), txn_command_data_all.size(), false);
    this->logger->sync(txn_cmd_log_lsn, [&, this]() {});

    // Execution
    execute_sp_transaction_batch(std::vector<TransactionType*>(pending_txns.begin(), pending_txns.begin() + until_ith));

    if (this->partitioner->replica_num() > 1) {
      if (is_replica_worker == false) {
        while (received_sp_replication_responses < sent_sp_replication_requests) {
          process_request(false);
        }
      }
    }

    // Clean-up
    for (size_t i = 0; i < until_ith; ++i) {
      DCHECK(pending_txns[i]->is_single_partition());
      int partition_id = pending_txns[i]->get_partition(0);
      owned_partition_locked_by[partition_id] = -1;
    }

    DCHECK(until_ith <= pending_txns.size());
    for (std::size_t i = 0; i < until_ith; ++i) {
      std::unique_ptr<TransactionType> txn(pending_txns.front());
      pending_txns.pop_front();
    }

    this->sp_round_concurrency.add(until_ith);
    return;
  }

  void process_new_transactions() {
    if (active_txns.size() >= 1) {
      return;
    }
    if (pending_txns.size() < batch_per_worker) {
      fill_pending_txns(batch_per_worker);
    }
    if (pending_txns.empty())
      return;
    //if (pending_txns[0]->is_single_partition()) {
      process_batch_of_sp_transactions();
      process_batch_of_mp_transactions();
      return;
    //}
    std::unique_ptr<TransactionType> txn;
    txn.reset(pending_txns.front());
    pending_txns.pop_front();

//    DCHECK(txn->is_single_partition() == false);
    int partition_id;
    if (is_replica_worker) {
      DCHECK(txn->ith_replica > 0);
    }
    partition_id = txn->partition_id;

    // if (this->transaction->ith_replica) {
    //   // for slave replicas, partition should be unlocked.
    //   DCHECK(owned_partition_locked_by[partition_id] == -1);
    // }
    if (owned_partition_locked_by[partition_id] == -1) {
      txn->reset();

      active_txns[txn->transaction_id] = txn.get();

      DCHECK((int)partition_owner_cluster_worker(partition_id, txn->ith_replica) == this_cluster_worker_id);
      DCHECK(owned_partition_locked_by[partition_id] == -1 || owned_partition_locked_by[partition_id] == txn->transaction_id);
      //startTime = std::chrono::steady_clock::now();
      setupHandlers(*txn);
      if (txn->is_single_partition()) {
        // LOG(INFO) << "Local txn";
        // This executor owns this partition for now.
        // if (is_replica_worker)
        //   LOG(INFO) << "Local tranasction from worker " << this_cluster_worker_id << " locked partition " << partition_id << " ith_replica " << txn->ith_replica << " initiating worker " << txn->initiating_cluster_worker_id;
        owned_partition_locked_by[partition_id] = txn->transaction_id;
      } else {
        ////LOG(INFO) << "Dist txn";
        //std::fill(parts_touched.begin(), parts_touched.end(), false);
      }

      auto ltc =
      std::chrono::duration_cast<std::chrono::nanoseconds>(
          std::chrono::steady_clock::now() - txn->startTime)
          .count();
      txn->set_stall_time(ltc);

      auto result = txn->execute(this->id);

      if (result == TransactionResult::READY_TO_COMMIT) {
        bool commit;
        {
          ScopedTimer t([&, this](uint64_t us) {
            if (commit) {
              txn->record_commit_work_time(us);
            } else {
              auto ltc =
              std::chrono::duration_cast<std::chrono::nanoseconds>(
                  std::chrono::steady_clock::now() - txn->startTime)
                  .count();
              txn->set_stall_time(ltc);
            }
          });
          commit = this->commit(*txn, cluster_worker_messages);
        }
        ////LOG(INFO) << "Txn Execution result " << (int)result << " commit " << commit;
        this->n_network_size.fetch_add(txn->network_size);
        if (commit) {
          ++worker_commit;
          this->n_commit.fetch_add(1);
          if (txn->si_in_serializable) {
            this->n_si_in_serializable.fetch_add(1);
          }
          auto latency =
              std::chrono::duration_cast<std::chrono::microseconds>(
                  std::chrono::steady_clock::now() - txn->startTime)
                  .count();
          this->percentile.add(latency);
          if (txn->distributed_transaction) {
            this->dist_latency.add(latency);
          } else {
            this->local_latency.add(latency);
          }
          // Make sure it is unlocked.
          // if (txn->is_single_partition())
          //   DCHECK(owned_partition_locked_by[partition_id] == -1);
          this->record_txn_breakdown_stats(*txn.get());
          active_txns.erase(txn->transaction_id);
          txn.release();
        } else {
          //LOG(INFO) << "Txn Execution result " << (int)result << " abort ";
          // Txns on slave replicas won't abort due to locking failure.
          if (txn->abort_lock) {
            this->n_abort_lock.fetch_add(1);
          } else {
            DCHECK(txn->abort_read_validation);
            this->n_abort_read_validation.fetch_add(1);
          }
          if (this->context.sleep_on_retry) {
            std::this_thread::sleep_for(std::chrono::microseconds(
                this->random.uniform_dist(0, 10)));
          }
          this->random.set_seed(0);
          //retry_transaction = true;
          active_txns.erase(txn->transaction_id);
          pending_txns.push_front(txn.release());
        }
      } else {
        ////LOG(INFO) << "Txn Execution result " << (int)result << " abort ";
        this->abort(*txn, cluster_worker_messages);
        this->n_abort_no_retry.fetch_add(1);
        //retry_transaction = false;
        // Make sure it is unlocked.
        // if (txn->is_single_partition())
        //   DCHECK(owned_partition_locked_by[partition_id] == -1);
        active_txns.erase(txn->transaction_id);
        txn.release();
      }
    } else {
      pending_txns.push_front(txn.release());
      txn.reset(nullptr);
    }
  }

  std::size_t process_request(bool new_transaction = true) {

    std::size_t size = 0;
    int times = 0;
    while (!this->in_queue.empty()) {
      ++size;
      std::unique_ptr<Message> message(this->in_queue.front());
      bool ok = this->in_queue.pop();
      CHECK(ok);
      DCHECK(message->get_worker_id() == this->id);
      if (message->get_is_replica() && is_replica_worker == false) {
        DCHECK(replica_worker);
        replica_worker->push_message(message.release());
        continue;
      }
      auto msg_cnt = message->get_message_count();
      int msg_idx = 0;
      for (auto it = message->begin(); it != message->end(); it++, ++msg_idx) {

        MessagePiece messagePiece = *it;
        auto type = messagePiece.get_message_type();
        //LOG(INFO) << "Message type " << type;
        auto message_partition_id = messagePiece.get_partition_id();
        // auto message_partition_owner_cluster_worker_id = partition_owner_cluster_worker(message_partition_id);
        
        // if (type != (int)HStoreMessage::MASTER_UNLOCK_PARTITION_RESPONSE && type != (int)HStoreMessage::MASTER_LOCK_PARTITION_RESPONSE
        //     && type != (int)HStoreMessage::ACQUIRE_PARTITION_LOCK_AND_READ_RESPONSE && type != (int) HStoreMessage::WRITE_BACK_RESPONSE && type != (int)HStoreMessage::RELEASE_READ_LOCK_RESPONSE
        //     && type != (int)HStoreMessage::RELEASE_PARTITION_LOCK_RESPONSE && type != (int)HStoreMessage::PREPARE_REQUEST && type != (int)HStoreMessage::PREPARE_RESPONSE && type != (int)HStoreMessage::PREPARE_REDO_REQUEST && type != (int)HStoreMessage::PREPARE_REDO_RESPONSE) {
        //   CHECK(message_partition_owner_cluster_worker_id == this_cluster_worker_id);
        // }
        ITable *table = this->db.find_table(messagePiece.get_table_id(),
                                      messagePiece.get_partition_id());
//        DCHECK(message->get_source_cluster_worker_id() != this_cluster_worker_id);
        DCHECK(message->get_source_cluster_worker_id() < (int32_t)this->context.partition_num);
        auto tid = message->get_transaction_id();
        DCHECK(tid != 0);
        TransactionType * txn = nullptr;
        if (active_txns.count(tid) > 0) {
          txn = active_txns[tid];
        }
        if (type == (int)HStoreMessage::ACQUIRE_PARTITION_LOCK_REQUEST) {
          acquire_partition_lock_request_handler(*message, messagePiece,
                                                 *cluster_worker_messages[message->get_source_cluster_worker_id()], *table,
                                                 txn);
        } else if (type == (int)HStoreMessage::ACQUIRE_PARTITION_LOCK_RESPONSE) {
          acquire_partition_lock_response_handler(*message, messagePiece,
                                                 *cluster_worker_messages[message->get_source_cluster_worker_id()], *table,
                                                 txn);
        } else if (type == (int)HStoreMessage::ACQUIRE_PARTITION_LOCK_AND_READ_REQUEST) {
          acquire_partition_lock_and_read_request_handler(*message, messagePiece,
                                                 *cluster_worker_messages[message->get_source_cluster_worker_id()], *table,
                                                 txn);
        } else if (type == (int)HStoreMessage::ACQUIRE_PARTITION_LOCK_AND_READ_RESPONSE) {
          acquire_partition_lock_and_read_response_handler(*message, messagePiece,
                                                 *cluster_worker_messages[message->get_source_cluster_worker_id()], *table,
                                                 txn);
        } else if (type == (int)HStoreMessage::WRITE_BACK_REQUEST) {
          write_back_request_handler(*message, messagePiece,
                                    *cluster_worker_messages[message->get_source_cluster_worker_id()], *table,
                                    txn);
        } else if (type == (int)HStoreMessage::WRITE_BACK_RESPONSE) {
          write_back_response_handler(*message, messagePiece,
                                      *cluster_worker_messages[message->get_source_cluster_worker_id()], *table,
                                      txn);
        } else if (type == (int)HStoreMessage::RELEASE_PARTITION_LOCK_REQUEST) {
          release_partition_lock_request_handler(*message, messagePiece,
                                                 *cluster_worker_messages[message->get_source_cluster_worker_id()], *table,
                                                 txn);
        } else if (type == (int)HStoreMessage::RELEASE_PARTITION_LOCK_RESPONSE) {
          release_partition_lock_response_handler(*message, messagePiece,
                                                 *cluster_worker_messages[message->get_source_cluster_worker_id()], *table,
                                                 txn);
        } else if (type == (int)HStoreMessage::PREPARE_REQUEST) {
          prepare_request_handler(*message, messagePiece, *cluster_worker_messages[message->get_source_cluster_worker_id()],
                                                *table,
                                                txn);
        } else if (type == (int)HStoreMessage::PREPARE_RESPONSE) {
          prepare_response_handler(*message, messagePiece, *cluster_worker_messages[message->get_source_cluster_worker_id()],
                                                *table,
                                                txn);
        } else if (type == (int)HStoreMessage::PREPARE_REDO_REQUEST) {
          prepare_and_redo_request_handler(*message, messagePiece, *cluster_worker_messages[message->get_source_cluster_worker_id()],
                                                *table,
                                                txn);
        } else if (type == (int)HStoreMessage::PREPARE_REDO_RESPONSE) {
          prepare_and_redo_response_handler(*message, messagePiece, *cluster_worker_messages[message->get_source_cluster_worker_id()],
                                                *table,
                                                txn);
        } else if (type == (int)HStoreMessage::COMMAND_REPLICATION_REQUEST) {
          command_replication_request_handler(*message, messagePiece, *cluster_worker_messages[message->get_source_cluster_worker_id()], 
                                                *table, 
                                                txn);
        } else if (type == (int)HStoreMessage::COMMAND_REPLICATION_RESPONSE) {
          command_replication_response_handler(*message, messagePiece, *cluster_worker_messages[message->get_source_cluster_worker_id()], 
                                                *table, 
                                                txn);
        } else if (type == (int)HStoreMessage::COMMAND_REPLICATION_SP_REQUEST) {
          command_replication_sp_request_handler(*message, messagePiece, *cluster_worker_messages[message->get_source_cluster_worker_id()], 
                                                *table, 
                                                txn);
        } else if (type == (int)HStoreMessage::COMMAND_REPLICATION_SP_RESPONSE) {
          command_replication_sp_response_handler(*message, messagePiece, *cluster_worker_messages[message->get_source_cluster_worker_id()], 
                                                *table, 
                                                txn);
        } else if (type == (int)HStoreMessage::PERSIST_CMD_BUFFER_REQUEST) {
          persist_cmd_buffer_request_handler(*message, messagePiece, *cluster_worker_messages[message->get_source_cluster_worker_id()], 
                                                *table, 
                                                txn);
        } else if (type == (int)HStoreMessage::PERSIST_CMD_BUFFER_RESPONSE) {
          persist_cmd_buffer_response_handler(*message, messagePiece, *cluster_worker_messages[message->get_source_cluster_worker_id()], 
                                                *table, 
                                                txn);
        } else {
          CHECK(false);
        }

        this->message_stats[type]++;
        this->message_sizes[type] += messagePiece.get_message_length();
      }

      size += message->get_message_count();
      flush_messages();
    }
    if (new_transaction)
      process_new_transactions();
    return size;
  }
  
  virtual void push_master_special_message(Message *message) override {     
  }

  virtual void push_master_message(Message *message) override { 
    
  }

  Message* pop_message_internal(LockfreeQueue<Message *> & queue) {
    if (queue.empty())
      return nullptr;

    Message *message = queue.front();

    // if (this->delay->delay_enabled()) {
    //   auto now = std::chrono::steady_clock::now();
    //   if (std::chrono::duration_cast<std::chrono::microseconds>(now -
    //                                                             message->time)
    //           .count() < this->delay->message_delay()) {
    //     return nullptr;
    //   }
    // }

    bool ok = queue.pop();
    CHECK(ok);

    return message;
  }

  int out_queue_round = 0;
  Message *pop_message() override {
    if (is_replica_worker || this->partitioner->replica_num() <= 1) {
      return pop_message_internal(this->out_queue);
    } else {
      DCHECK(replica_worker);
      if (++out_queue_round % 2 == 1) {
        return pop_message_internal(this->out_queue);
      } else {
        return replica_worker->pop_message();
      }
    }
  }

  TransactionType * get_next_transaction() {
    if (is_replica_worker) {
      if (queuedTxns.empty() == false) {
        auto txn = queuedTxns.front().release();
        queuedTxns.pop_front();
        return txn;
      }
      // Sleep for a while to save cpu
      std::this_thread::sleep_for(std::chrono::microseconds(10));
      return nullptr;
    } else {
      auto partition_id = managed_partitions[this->random.next() % managed_partitions.size()];
      auto txn = this->workload.next_transaction(this->context, partition_id, this->id).release();
      return txn;
    }
  }

  void start() override {
    //LOG(INFO) << "Executor " << this->id << " starts.";

    uint64_t last_seed = 0;

    ExecutorStatus status;

    while ((status = static_cast<ExecutorStatus>(this->worker_status.load())) !=
           ExecutorStatus::START) {
      std::this_thread::yield();
    }

    if (is_replica_worker == false)
      this->n_started_workers.fetch_add(1);
    
    int cnt = 0;
    
    worker_commit = 0;
    int try_times = 0;
    //auto startTime = std::chrono::steady_clock::now();
    bool retry_transaction = false;
    int partition_id;
    bool is_sp = false;
    do {
      process_request();
      status = static_cast<ExecutorStatus>(this->worker_status.load());
    } while (status != ExecutorStatus::STOP);
    
    onExit();
    if (is_replica_worker == false)
      this->n_complete_workers.fetch_add(1);

    // once all workers are stop, we need to process the replication
    // requests

    while (static_cast<ExecutorStatus>(this->worker_status.load()) !=
           ExecutorStatus::CLEANUP) {
      process_request(false);
    }

    process_request(false);
    if (is_replica_worker == false)
      this->n_complete_workers.fetch_add(1);
    //LOG(INFO) << "Executor " << this->id << " exits.";
  }

  void onExit() override {

    LOG(INFO) << "Worker " << this->id << " commit: "<< this->worker_commit << ". sp batch concurrency: " << sp_round_concurrency.nth(50) << ". mp batch concurrency: " << this->mp_round_concurrency.nth(50) << ". latency: " << this->percentile.nth(50)
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
              << " (99%). \n"
              << " LOCAL txn stall " << this->local_txn_stall_time_pct.nth(50) << " us, "
              << " local_work " << this->local_txn_local_work_time_pct.nth(50) << " us, "
              << " remote_work " << this->local_txn_remote_work_time_pct.nth(50) << " us, "
              << " commit_work " << this->local_txn_commit_work_time_pct.nth(50) << " us, "
              << " commit_prepare " << this->local_txn_commit_prepare_time_pct.nth(50) << " us, "
              << " commit_persistence " << this->local_txn_commit_persistence_time_pct.nth(50) << " us, "
              << " commit_replication " << this->local_txn_commit_replication_time_pct.nth(50) << " us, "
              << " commit_write_back " << this->local_txn_commit_write_back_time_pct.nth(50) << " us, "
              << " commit_release_lock " << this->local_txn_commit_unlock_time_pct.nth(50) << " us \n"
              << " DIST txn stall " << this->dist_txn_stall_time_pct.nth(50) << " us, "
              << " local_work " << this->dist_txn_local_work_time_pct.nth(50) << " us, "
              << " remote_work " << this->dist_txn_remote_work_time_pct.nth(50) << " us, "
              << " commit_work " << this->dist_txn_commit_work_time_pct.nth(50) << " us, "
              << " commit_prepare " << this->dist_txn_commit_prepare_time_pct.nth(50) << " us, "
              << " commit_persistence " << this->dist_txn_commit_persistence_time_pct.nth(50) << " us, "
              << " commit_replication " << this->local_txn_commit_replication_time_pct.nth(50) << " us, "
              << " commit_write_back " << this->dist_txn_commit_write_back_time_pct.nth(50) << " us, "
              << " commit_release_lock " << this->dist_txn_commit_unlock_time_pct.nth(50) << " us \n";

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
      
      message->set_put_to_out_queue_time(Time::now());
      this->out_queue.push(message);
      
      cluster_worker_messages[i] = std::make_unique<Message>();
      init_message(cluster_worker_messages[i].get(), i);
    }
  }

  int partition_owner_worker_id_on_a_node(int partition_id) const {
    auto nth_partition_on_master_coord = partition_id / this->context.coordinator_num;
    auto node_worker_id_this_partition_belongs_to = nth_partition_on_master_coord % this->context.worker_num; // A worker could handle more than 1 partition
    return node_worker_id_this_partition_belongs_to;
  }

  int partition_owner_cluster_worker(int partition_id, std::size_t ith_replica) const {
    auto coord_id = ith_replica == 0 ? this->partitioner->master_coordinator(partition_id) : 
                    this->partitioner->get_ith_replica_coordinator(partition_id, ith_replica);
    auto cluster_worker_id_starts_at_this_node = coord_id * this->context.worker_num;

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
