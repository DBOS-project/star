//
// Created by Xinjing on 9/12/21.
//

#pragma once
#include <vector>

#include "common/DeferCode.h"
#include "core/Executor.h"
#include "protocol/H-Store/HStore.h"
#include <unistd.h>
#include <sys/syscall.h>
#include <sys/types.h>
#define gettid() syscall(SYS_gettid)
#include <unordered_set>
namespace star {

template <class Workload>
class HStoreExecutor
    : public Executor<Workload, HStore<typename Workload::DatabaseType>>

{
private:
  std::vector<std::unique_ptr<Message>> cluster_worker_messages;
  int cluster_worker_num;
  Percentile<uint64_t> txn_try_times;
  Percentile<int64_t> round_concurrency;
  Percentile<int64_t> effective_round_concurrency;
  Percentile<int64_t> replica_progress_query_latency;
  Percentile<int64_t> spread_time;
  Percentile<int64_t> replay_time;
  Percentile<int64_t> replication_gap_after_active_replica_execution;
  Percentile<int64_t> replication_sync_comm_rounds;
  Percentile<int64_t> replication_time;
  Percentile<int64_t> txn_retries;
  std::size_t batch_per_worker;
  std::atomic<bool> ended{false};
  uint64_t worker_commit = 0;
  uint64_t sent_sp_replication_requests = 0;
  uint64_t received_sp_replication_responses = 0;
  uint64_t sent_persist_cmd_buffer_requests = 0;
  uint64_t received_persist_cmd_buffer_responses = 0;
  Percentile<int64_t> commit_interval;
  std::chrono::steady_clock::time_point last_commit;
  Percentile<int64_t> cmd_queue_time;
  Percentile<int64_t> cmd_stall_time;
  Percentile<int64_t> execution_phase_time;
  Percentile<int64_t> execution_phase_mp_rounds;
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
  int replica_cluster_worker_id = -1;
  std::vector<int64_t> owned_partition_locked_by;
  std::vector<int> managed_partitions;
  std::deque<std::unique_ptr<TransactionType>> queuedTxns;
  bool is_replica_worker = false;
  HStoreExecutor* replica_worker = nullptr;
  std::unordered_map<long long, TransactionType*> active_txns;
  std::deque<TransactionType*> pending_txns;

  
  std::deque<TxnCommand> command_buffer;
  std::deque<TxnCommand> command_buffer_outgoing;
  
  

  /*
 *   min_replayed_log_position   <=      min_coord_txn_written_log_position   <=    next_position_in_command_log
 *               |                                        |                                       |
 *               |                                        |                                       |
 * +-------------+----------------------------------------+---------------------------------------+------------+
 * |             |                                        |                                       |            |
 * |             |                                        |                                       |            |
 * |             v                                        v                                       v            |
 * |                                                                                                           |
 * |                                                                                                           |
 * |                                                                                                           |
 * |                                           COMMAND LOG                                                     |
 * |                                                                                                           |
 * +-----------------------------------------------------------------------------------------------------------+
 */
  // For active replica
  // Next position to write to in the command log
  int64_t next_position_in_command_log = 0;
  // Commands initiated by this worker priori to this position are all written to command buffer
  int64_t minimum_coord_txn_written_log_position = -1;
  // Commands priori to this position are all executed on active/standby replica
  int64_t minimum_replayed_log_position = -1;
  // Invariant: minimum_replayed_log_position <= minimum_coord_txn_written_log_position <= next_position_in_command_log
  uint64_t get_replica_replay_log_position_requests = 0;
  uint64_t get_replica_replay_log_position_responses = 0;

  // For standby replica
  std::vector<std::deque<TxnCommand>> partition_command_queues; // Commands against individual partition
  std::vector<bool> partition_command_queue_processing;
  std::vector<int> partition_to_cmd_queue_index;
  // Records the last command's position_in_log (TxnCommand.position_in_log) that got replayed
  std::vector<int64_t> partition_replayed_log_index; 

  int64_t get_minimum_replayed_log_position() {
    int64_t minv = partition_replayed_log_index[0];
    for (auto v : partition_replayed_log_index) {
      minv = std::max(minv, v);
    }
    return minv;
  }

  std::string serialize_commands(std::deque<TxnCommand>::iterator it, const std::deque<TxnCommand>::iterator end) {
    std::string buffer;
    Encoder enc(buffer);
    for (; it != end; ++it) {
      const TxnCommand & cmd = *it;
      enc << cmd.tid;
      enc << cmd.is_coordinator;
      enc << cmd.is_mp;
      enc << cmd.position_in_log;
      enc << cmd.partition_id;
      enc << cmd.command_data.size();
      enc.write_n_bytes(cmd.command_data.data(), cmd.command_data.size());
      if (cmd.is_coordinator) {
        if (cmd.is_mp) {
          DCHECK(cmd.partition_id == -1);
        } else {
          DCHECK(cmd.partition_id != -1);
        }
      }
    }
    return buffer;
  }

  std::vector<TxnCommand> deserialize_commands(const std::string & buffer) {
    std::vector<TxnCommand> cmds;
    Decoder dec(buffer);
    while(dec.size() > 0) {
      TxnCommand cmd;
      dec >> cmd.tid;
      dec >> cmd.is_coordinator;
      dec >> cmd.is_mp;
      dec >> cmd.position_in_log;
      dec >> cmd.partition_id;
      std::size_t command_data_size;
      dec >> command_data_size;
      cmd.command_data = std::string(dec.get_raw_ptr(), command_data_size);
      dec.remove_prefix(command_data_size);
      cmds.emplace_back(std::move(cmd));
    }
    DCHECK(dec.size() == 0);
    return std::move(cmds);
  }

  std::deque<TxnCommand> & get_partition_cmd_queue(int partition) {
    DCHECK(is_replica_worker);
    DCHECK(partition_to_cmd_queue_index[partition] != -1);
    DCHECK(partition_to_cmd_queue_index[partition] >= 0);
    DCHECK(partition_to_cmd_queue_index[partition] < (int)partition_command_queues.size());
    return partition_command_queues[partition_to_cmd_queue_index[partition]];
  }

  int64_t & get_partition_last_replayed_position_in_log(int partition) {
    DCHECK(is_replica_worker);
    DCHECK(partition_to_cmd_queue_index[partition] != -1);
    DCHECK(partition_to_cmd_queue_index[partition] >= 0);
    DCHECK(partition_to_cmd_queue_index[partition] < (int)partition_command_queues.size());
    return partition_replayed_log_index[partition_to_cmd_queue_index[partition]];
  }

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
              if (this->partitioner->replica_num() > 1) {
                auto standby_replica_cluster_worker_id = partition_owner_cluster_worker(p, 1);
                DCHECK(replica_cluster_worker_id == -1 || standby_replica_cluster_worker_id == replica_cluster_worker_id);
                replica_cluster_worker_id = standby_replica_cluster_worker_id;
              }
            }
          }
          DCHECK(managed_partitions.empty() == false);
          managed_partitions_str.pop_back(); // Remove last ,
        }
        
        std::string managed_replica_partitions_str;
        if (is_replica_worker) {
          partition_to_cmd_queue_index.resize(this->context.partition_num, -1);
          DCHECK(managed_partitions.empty());
          size_t cmd_queue_idx = 0;
          for (int p = 0; p < (int)this->context.partition_num; ++p) {
            for (size_t i = 1; i < this->partitioner->replica_num(); ++i) {
              if (this_cluster_worker_id == partition_owner_cluster_worker(p, i)) {
                managed_partitions.push_back(p);
                partition_command_queues.push_back(std::deque<TxnCommand>());
                partition_command_queue_processing.push_back(false);
                partition_replayed_log_index.push_back(-1);
                partition_to_cmd_queue_index[p] = cmd_queue_idx++;
                DCHECK(cmd_queue_idx == partition_command_queues.size());
                DCHECK(cmd_queue_idx == partition_replayed_log_index.size());
                auto active_replica_cluster_worker_id = partition_owner_cluster_worker(p, 0);
                DCHECK(replica_cluster_worker_id == -1 || active_replica_cluster_worker_id == replica_cluster_worker_id);
                replica_cluster_worker_id = active_replica_cluster_worker_id;
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
      TxnCommand txn_cmd;
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
          
          txn_cmd.partition_id = partition_id;
          txn_cmd.command_data = "";
          txn_cmd.tid = txn.transaction_id;
          txn_cmd.is_mp = true;
          txn.network_size += MessageFactoryType::new_release_partition_lock_message(
              *messages[owner_cluster_worker], *table, this_cluster_worker_id, false, txn.ith_replica, write_cmd_buffer, txn_cmd);
          //LOG(INFO) << "Abort release lock MP partition " << partition_id << " by cluster worker" << this_cluster_worker_id << " " << tid_to_string(txn.transaction_id) << " request sent";
        }
      }
      txn.message_flusher();
    } else {
      DCHECK(txn.pendingResponses == 0);
      DCHECK(txn.get_partition_count() == 1);
      auto partition_id = txn.get_partition(0);
      if (owned_partition_locked_by[partition_id] == txn.transaction_id) {
        //LOG(INFO) << "Abort release lock local partition " << partition_id << " by cluster worker" << this_cluster_worker_id<< " " << tid_to_string(txn.transaction_id);
        owned_partition_locked_by[partition_id] = -1;
      }
    }
    txn.abort_lock_lock_released = true;
  }

  bool commit_mp(TransactionType &txn,
              std::vector<std::unique_ptr<Message>> &messages) {
    // if (is_replica_worker) { // Should always succeed for replica
    //   CHECK(txn.abort_lock == false);
    // }
    if (txn.abort_lock) {
      DCHECK(txn.is_single_partition() == false);
      if (is_replica_worker == false) {
        // We only release locks when executing on active replica
        abort(txn, messages);
      }
      
      return false;
    }
    DCHECK(this->context.hstore_command_logging == true);

    uint64_t commit_tid = generate_tid(txn);
    DCHECK(txn.get_logger());

    if (is_replica_worker == false) {
      auto txn_command_data = txn.serialize(1);
      TxnCommand cmd;
      cmd.command_data = txn_command_data;
      cmd.tid = txn.transaction_id;
      cmd.is_mp = txn.is_single_partition() == false;
      if (!cmd.is_mp) {
        DCHECK(txn.get_partition_count() == 1);
        cmd.partition_id = txn.get_partition(0);
        DCHECK(cmd.partition_id != -1);
      } else {
        cmd.partition_id = -1; // Partitions could be discovered in txn_command_data
      }
      cmd.is_coordinator = true;
      cmd.position_in_log = next_position_in_command_log++;
      minimum_coord_txn_written_log_position = cmd.position_in_log;
      command_buffer.push_back(cmd);
      command_buffer_outgoing.push_back(cmd);
    }

    {
      ScopedTimer t([&, this](uint64_t us) {
        txn.record_commit_write_back_time(us);
      });
      write_back_command_logging(txn, commit_tid, messages);
    }
    {
      ScopedTimer t([&, this](uint64_t us) {
        txn.record_commit_unlock_time(us);
      });
      release_partition_locks_async(txn, messages, is_replica_worker == false);
    }
    
    return true;
  }

  void release_partition_locks_async(TransactionType &txn, std::vector<std::unique_ptr<Message>> &messages, bool write_cmd_buffer) {
    if (is_replica_worker) {
      DCHECK(txn.ith_replica != 0);
    }
    if (txn.is_single_partition() == false) {
      TxnCommand txn_cmd;
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
            txn_cmd.partition_id = partitionId;
            txn_cmd.command_data = "";
            txn_cmd.tid = txn.transaction_id;
            txn_cmd.is_mp = true;
            // send messages to other partitions to unlock partitions;
            messages[owner_cluster_worker]->set_transaction_id(txn.transaction_id);
            txn.network_size += MessageFactoryType::new_release_partition_lock_message(
                *messages[owner_cluster_worker], *table, this_cluster_worker_id, false, txn.ith_replica, write_cmd_buffer, txn_cmd);
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
            writeKey.get_value(), this_cluster_worker_id, commit_tid, txn.ith_replica, false);
        //LOG(INFO) << "Partition worker " << this_cluster_worker_id << " issueed write request on partition " << partitionId;
      }
    }
    if (txn.is_single_partition() == false) {
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
      // if (local_index_read || txn.is_single_partition()) {
      //   remote = false;
      //   if (owned_partition_locked_by[partition_id] != txn.transaction_id) {
      //     if (owned_partition_locked_by[partition_id] != -1) {
      //       success = false;
      //       return;
      //     } else {
      //       owned_partition_locked_by[partition_id] = txn.transaction_id;
      //     }
      //   }
      //   if (is_replica_worker && owned_partition_locked_by[partition_id] != txn.transaction_id) {
      //     success = false;
      //     return;
      //   }
      //   success = true;
      //   this->search(table_id, partition_id, key, value);
      //   return;
      // }

      int owner_cluster_worker = partition_owner_cluster_worker(partition_id, txn.ith_replica);
      if ((int)owner_cluster_worker == this_cluster_worker_id) {
        remote = false;
        if (is_replica_worker && owned_partition_locked_by[partition_id] != txn.transaction_id) {
          success = false;
          return;
        }
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

    txn.remote_request_handler = [this]() { return this->handle_requests(); };
    txn.message_flusher = [this]() { this->flush_messages(); };
    txn.get_table = [this](std::size_t tableId, std::size_t partitionId) { return this->db.find_table(tableId, partitionId); };
    txn.set_logger(this->logger);
  };

  using Transaction = TransactionType;

  bool make_sure_partition_lock_is_acquired(const Message & inputMessage, MessagePiece inputPiece,
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

    stringPiece.remove_prefix(key_size);
    star::Decoder dec(stringPiece);
    dec >> key_offset >> request_remote_worker_id >> ith_replica;

    if (ith_replica > 0)
      DCHECK(is_replica_worker);

    DCHECK((int)partition_owner_cluster_worker(partition_id, ith_replica) == this_cluster_worker_id);

    DCHECK(dec.size() == 0);
    DCHECK(is_replica_worker);
    if (owned_partition_locked_by[partition_id] != tid) {
      //LOG(INFO) << "Transaction " << tid << " failed to lock partition " <<  partition_id << " locked by transaction " << owned_partition_locked_by[partition_id];
      replay_sp_commands(partition_id);
      if (owned_partition_locked_by[partition_id] == tid) 
      {
        return true;
      } else {
        return false;
      }
    }

    return true;
  }

  bool acquire_partition_lock_and_read_request_handler(const Message & inputMessage, MessagePiece inputPiece,
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
    if (is_replica_worker) {
      success = true;
      if (owned_partition_locked_by[partition_id] != tid) {
        //LOG(INFO) << "Transaction " << tid << " failed to lock partition " <<  partition_id << " locked by transaction " << owned_partition_locked_by[partition_id];
        replay_sp_commands(partition_id);
        if (owned_partition_locked_by[partition_id] == tid) 
        {
          success = true;
        } else {
          success = false;
        }
      }
    } else {
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
    return success;
  }

  void spread_replicated_commands(std::vector<TxnCommand> & cmds) {
    DCHECK(is_replica_worker);
    for (size_t i = 0; i < cmds.size(); ++i) {
      if (!cmds[i].is_coordinator) { // place into one partition command queue for replay
        DCHECK(cmds[i].partition_id != -1);
        auto & q = get_partition_cmd_queue(cmds[i].partition_id);
        cmds[i].queue_ts = std::chrono::steady_clock::now();
        q.push_back(cmds[i]);
      } else if (cmds[i].is_mp == false) { // place into one partition command queue for replay
        DCHECK(cmds[i].partition_id != -1);
        auto & q = get_partition_cmd_queue(cmds[i].partition_id);
        auto sp_txn = this->workload.deserialize_from_raw(this->context, cmds[i].command_data).release();
        DCHECK(sp_txn->ith_replica > 0);
        cmds[i].txn.reset(sp_txn);
        cmds[i].queue_ts = std::chrono::steady_clock::now();
        q.push_back(cmds[i]);
      } else { // place into multiple partition command queues for replay
        DCHECK(cmds[i].partition_id == -1);
        auto mp_txn = this->workload.deserialize_from_raw(this->context, cmds[i].command_data).release();
        auto partition_count = mp_txn->get_partition_count();
        cmds[i].txn.reset(mp_txn);
        cmds[i].queue_ts = std::chrono::steady_clock::now();
        DCHECK(mp_txn->ith_replica > 0);
        DCHECK(partition_count > 1);

        bool mp_queued = false;
        int first_partition_by_this_worker = -1;
        for (int32_t j = 0; j < partition_count; ++j) {
          auto partition_id = mp_txn->get_partition(j);
          auto partition_responsible_worker = partition_owner_cluster_worker(partition_id, 1);
          if (partition_responsible_worker == this_cluster_worker_id) {
            first_partition_by_this_worker = partition_id;
            auto & q = get_partition_cmd_queue(partition_id);
            TxnCommand cmd_mp = cmds[i];
            cmd_mp.is_coordinator = false;
            cmd_mp.partition_id = partition_id;
            cmd_mp.is_mp = true;
            cmd_mp.command_data = "";
            q.push_back(cmd_mp);
          }
        }
        DCHECK(first_partition_by_this_worker != -1);
        get_partition_cmd_queue(first_partition_by_this_worker).push_back(cmds[i]);
      }
    }
    //LOG(INFO) << "Spread " << cmds.size() << " commands to queues";
  }

  void command_replication_request_handler(const Message & inputMessage, MessagePiece inputPiece,
                                   Message &responseMessage,
                                   ITable &table, Transaction *txn) {
    DCHECK(is_replica_worker);
    DCHECK(inputPiece.get_message_type() ==
           static_cast<uint32_t>(HStoreMessage::COMMAND_REPLICATION_REQUEST));

    auto key_size = table.key_size();
    auto value_size = table.value_size();
    ScopedTimer t([&, this](uint64_t us) {
      spread_time.add(us);
    });
    /*
     * The structure of a write command replication request: (ith_replica, txn data)
     * The structure of a write lock response: (success?, key offset, value?)
     */
    std::size_t ith_replica;
    int initiating_cluster_worker_id;
    bool persist_cmd_buffer = false;
    auto stringPiece = inputPiece.toStringPiece();
    Decoder dec(stringPiece);
    dec >> ith_replica >> initiating_cluster_worker_id >> persist_cmd_buffer;
    std::string data = dec.bytes.toString();
    DCHECK(inputPiece.get_message_length() ==
             MessagePiece::get_header_size() + sizeof(std::size_t) + data.size() + sizeof(initiating_cluster_worker_id) + sizeof(persist_cmd_buffer));
    std::size_t data_sz = data.size();
    auto cmds = deserialize_commands(data);
    //command_buffer.insert(command_buffer.end(), cmds.begin(), cmds.end());
    spread_replicated_commands(cmds);
    // if (persist_cmd_buffer) {
    //   persist_and_clear_command_buffer(true);
    // }
    // std::unique_ptr<TransactionType> new_txn = this->workload.deserialize_from_raw(this->context, data);
    // new_txn->initiating_cluster_worker_id = initiating_cluster_worker_id;
    // new_txn->initiating_transaction_id = inputMessage.get_transaction_id();
    // new_txn->transaction_id = WorkloadType::next_transaction_id(this->context.coordinator_id);
    // auto partition_id = new_txn->partition_id;
    // DCHECK((int)partition_owner_cluster_worker(partition_id, ith_replica) == this_cluster_worker_id);

    // // auto txn_command_data = new_txn->serialize(ith_replica);
    // // // Persist txn command
    // // this->logger->write(txn_command_data.c_str(), txn_command_data.size(), true, [&, this]() {process_request();});

    // queuedTxns.emplace_back(new_txn.release());

    // // auto message_size = MessagePiece::get_header_size();

    // // auto message_piece_header = MessagePiece::construct_message_piece_header(
    // //     static_cast<uint32_t>(HStoreMessage::COMMAND_REPLICATION_RESPONSE), message_size,
    // //     0, 0);

    // // star::Encoder encoder(responseMessage.data);
    // // encoder << message_piece_header;
  
    // // responseMessage.flush();
    // // responseMessage.set_gen_time(Time::now());
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
      auto lsn = this->logger->write(output.c_str(), output.size(), false, [&, this](){ handle_requests(); });
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

  void get_replayed_log_position_request_handler(const Message & inputMessage, MessagePiece inputPiece,
                                          Message &responseMessage,
                                          ITable &table, Transaction *txn) {
    DCHECK(inputPiece.get_message_type() ==
           static_cast<uint32_t>(HStoreMessage::GET_REPLAYED_LOG_POSITION_REQUEST));
    DCHECK(inputPiece.get_message_length() ==
             MessagePiece::get_header_size() + sizeof(int) + sizeof(int));
    DCHECK(is_replica_worker);
    int cluster_worker_id, ith_replica;
    auto stringPiece = inputPiece.toStringPiece();

    Decoder dec(stringPiece);
    dec >> cluster_worker_id >> ith_replica;
    int64_t minimum_replayed_log_positition = get_minimum_replayed_log_position();
    //LOG(INFO) << "cluster_worker " << cluster_worker_id << " called persist_cmd_buffer_request_handler on cluster worker " << this_cluster_worker_id;
    
    auto message_size = MessagePiece::get_header_size() + sizeof(minimum_replayed_log_positition);
    auto message_piece_header = MessagePiece::construct_message_piece_header(
        static_cast<uint32_t>(HStoreMessage::GET_REPLAYED_LOG_POSITION_RESPONSE), message_size,
        0, 0);

    star::Encoder encoder(responseMessage.data);
    encoder << message_piece_header << minimum_replayed_log_positition;
    responseMessage.set_transaction_id(inputMessage.get_transaction_id());
    responseMessage.set_is_replica(false);
    responseMessage.flush();
    responseMessage.set_gen_time(Time::now());
  }
  
  void get_replayed_log_position_response_handler(const Message & inputMessage, MessagePiece inputPiece,
                                          Message &responseMessage,
                                          ITable &table, Transaction *txn) {
    DCHECK(inputPiece.get_message_type() ==
           static_cast<uint32_t>(HStoreMessage::GET_REPLAYED_LOG_POSITION_RESPONSE));
    DCHECK(inputPiece.get_message_length() ==
             MessagePiece::get_header_size() + sizeof(int64_t));
    DCHECK(is_replica_worker == false);
    auto stringPiece = inputPiece.toStringPiece();

    Decoder dec(stringPiece);
    int64_t min_replayed_log_position_in_replica;
    dec >> min_replayed_log_position_in_replica;
    DCHECK(minimum_replayed_log_position <= min_replayed_log_position_in_replica);
    minimum_replayed_log_position = min_replayed_log_position_in_replica;
    get_replica_replay_log_position_responses++;
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
    //persist_and_clear_command_buffer();
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
    this->received_persist_cmd_buffer_responses++;
    //LOG(INFO) << "cluster_worker " << this_cluster_worker_id << " received response from persist_cmd_buffer_request_handler on cluster worker " << cluster_worker_id << ", received_persist_cmd_buffer_responses " << received_persist_cmd_buffer_responses;
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
      int partition_id;
      bool is_mp;
      int64_t tid;
      dec >> partition_id;
      dec >> tid;
      dec >> is_mp;
      dec >> txn_command_data_size;
      if (txn_command_data_size) {
        txn_command_data = std::string(dec.bytes.data(), txn_command_data_size);
      }
      TxnCommand participant_cmd;
      participant_cmd.command_data = "";
      participant_cmd.is_coordinator = false;
      participant_cmd.is_mp = is_mp;
      DCHECK(is_mp);
      DCHECK(partition_id != -1);
      participant_cmd.partition_id = partition_id;
      participant_cmd.position_in_log = next_position_in_command_log++;
      participant_cmd.tid = tid;
      command_buffer.push_back(participant_cmd);
      command_buffer_outgoing.push_back(participant_cmd);
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

  void persist_and_clear_command_buffer(bool continue_process_request = false) {
    if (command_buffer.empty())
      return;
    std::string data;
    Encoder encoder(data);
    for (size_t i = 0; i < command_buffer.size(); ++i) {
      encoder << command_buffer[i].tid;
      encoder << command_buffer[i].partition_id;
      encoder << command_buffer[i].is_mp;
      encoder << command_buffer[i].position_in_log;
      encoder << command_buffer[i].command_data.size();
      encoder.write_n_bytes(command_buffer[i].command_data.data(), command_buffer[i].command_data.size());
    }
    command_buffer.clear();
    if (continue_process_request) {
      this->logger->write(encoder.toStringPiece().data(), encoder.toStringPiece().size(), true, [&, this](){handle_requests(false);});
    } else {
      this->logger->write(encoder.toStringPiece().data(), encoder.toStringPiece().size(), true, [&, this](){});
    }
  }

  bool process_single_transaction(TransactionType * txn) {
    auto txn_id = txn->transaction_id;
    DCHECK(txn->is_single_partition());
    if (txn->is_single_partition()) {
      if (owned_partition_locked_by[txn->get_partition(0)] == -1) {
        owned_partition_locked_by[txn->get_partition(0)] = txn_id;
      } else {
        return false;
      }
    }
    setupHandlers(*txn);
    DCHECK(txn->execution_phase == false);
    
    active_txns[txn->transaction_id] = txn;
    auto ltc =
    std::chrono::duration_cast<std::chrono::microseconds>(
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
            std::chrono::duration_cast<std::chrono::microseconds>(
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
        commit_interval.add(std::chrono::duration_cast<std::chrono::microseconds>(
              std::chrono::steady_clock::now() - last_commit)
              .count());
        last_commit = std::chrono::steady_clock::now();
        return true;
      } else {
        DCHECK(txn->is_single_partition() == false);
        // if (is_replica_worker)
        //   LOG(INFO) << "Txn " << txn_id << " Execution result " << (int)result << " abort by lock conflict on cluster worker " << this_cluster_worker_id;
        // Txns on slave replicas won't abort due to locking failure.
        if (txn->abort_lock) {
          this->n_abort_lock.fetch_add(1);
        } else {
          DCHECK(txn->abort_read_validation);
          this->n_abort_read_validation.fetch_add(1);
        }
        if (this->context.sleep_on_retry) {
          // std::this_thread::sleep_for(std::chrono::milliseconds(
          //     this->random.uniform_dist(100, 1000)));
        }
        //retry_transaction = true;
        active_txns.erase(txn->transaction_id);
        txn->reset();
        return false;
      }
    } else {
      DCHECK(false); // For now, assume there is not program aborts.
      return true;
    }
  }

  void process_to_commit(std::deque<TransactionType*> & to_commit) {
    while (!to_commit.empty()) {
      auto txn = to_commit.front();
      to_commit.pop_front();
      DCHECK(txn->pendingResponses == 0);
      if (txn->finished_commit_phase) {
        DCHECK(txn->pendingResponses == 0);
        continue;
      }
      process_single_txn_commit(txn);
    }
  }
  void process_execution_phase(const std::vector<TransactionType*> & txns) {
    std::deque<TransactionType*> to_commit;
    int cnt = 0;
    for (size_t i = 0; i < txns.size(); ++i) {
      setupHandlers(*txns[i]);
      txns[i]->reset();
      txns[i]->execution_phase = false;
      txns[i]->synchronous = false;
      DCHECK(txns[i]->is_single_partition() == false);
      // initiate read requests (could be remote)
      active_txns[txns[i]->transaction_id] = txns[i];
      auto res = txns[i]->execute(this->id);
      if (res == TransactionResult::ABORT_NORETRY) {
        txns[i]->abort_no_retry = true;
      }
      if (++cnt % 5 == 0) {
        handle_requests_and_collect_ready_to_commit_txns(to_commit);
      }
      if (to_commit.empty() == false) {
        auto txn = to_commit.front();
        to_commit.pop_front();
        DCHECK(txn->pendingResponses == 0);
        if (txn->finished_commit_phase) {
          DCHECK(txn->pendingResponses == 0);
          continue;
        }
        process_single_txn_commit(txn);
      }
    }
    process_to_commit(to_commit);
  }

  void process_single_txn_commit(TransactionType * txn) {
    DCHECK(txn->pendingResponses == 0);
    if (txn->abort_no_retry) {
      active_txns.erase(txn->transaction_id);
      txn->finished_commit_phase = true;
      return;
    }

    if (txn->abort_lock) {
      if (is_replica_worker == false && txn->abort_lock_lock_released == false) {
        // We only release locks when executing on active replica
        abort(*txn, cluster_worker_messages);
      }
      active_txns.erase(txn->transaction_id);
      this->n_abort_lock.fetch_add(1);
      txn->finished_commit_phase = true;
      return;
    }

    txn->execution_phase = true;
    txn->synchronous = false;

    // initiate read requests (could be remote)
    // fill in the writes
    auto result = txn->execute(this->id);
    DCHECK(txn->abort_lock == false);
    DCHECK(result == TransactionResult::READY_TO_COMMIT);
    bool commit;
    {
      ScopedTimer t([&, this](uint64_t us) {
        if (commit) {
          txn->record_commit_work_time(us);
        } else {
          auto ltc =
          std::chrono::duration_cast<std::chrono::microseconds>(
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
      commit_interval.add(std::chrono::duration_cast<std::chrono::microseconds>(
            std::chrono::steady_clock::now() - last_commit)
            .count());
      last_commit = std::chrono::steady_clock::now();
    } else {
      DCHECK(false);
      // if (is_replica_worker)
      //   LOG(INFO) << "Txn " << txn_id << " Execution result " << (int)result << " abort by lock conflict on cluster worker " << this_cluster_worker_id;
      // Txns on slave replicas won't abort due to locking failure.
      if (txn->abort_lock) {
        this->n_abort_lock.fetch_add(1);
      } else {
        DCHECK(txn->abort_read_validation);
        this->n_abort_read_validation.fetch_add(1);
      }
      if (this->context.sleep_on_retry) {
        // std::this_thread::sleep_for(std::chrono::milliseconds(
        //     this->random.uniform_dist(100, 1000)));
      }
      //retry_transaction = true;
      active_txns.erase(txn->transaction_id);
    }
    txn->finished_commit_phase = true;
  }

  void process_commit_phase(const std::vector<TransactionType*> & txns) {
    std::deque<TransactionType*> to_commit;
    while (active_txns.size()) {
      for (size_t i = 0; i < txns.size(); ++i) {
        auto txn = txns[i];
        if (txn->finished_commit_phase) {
          DCHECK(txn->pendingResponses == 0);
          continue;
        }
        if (txn->abort_lock) {
          if (txn->abort_lock_lock_released == false && is_replica_worker == false) {
            // send the release lock message earlier
            abort(*txn, cluster_worker_messages);
          }
        }
        if (txn->pendingResponses == 0) {
          process_single_txn_commit(txn);
        } else {
          to_commit.clear();
          handle_requests_and_collect_ready_to_commit_txns(to_commit);
          process_to_commit(to_commit);
        }
      }
    }
  }

  void execute_transaction_batch(const std::vector<TransactionType*> all_txns,
                                 const std::vector<TransactionType*> & sp_txns, 
                                 std::vector<TransactionType*> & mp_txns) {
    std::vector<bool> workers_need_persist_cmd_buffer(this->cluster_worker_num, true);
    int64_t txn_id = 0;
    int cnt = 0;
    {
      ScopedTimer t0([&, this](uint64_t us) {
        execution_phase_time.add(us);
      });
      int cnt = 0;
      for (size_t i = 0; i < sp_txns.size(); ++i) {
        auto txn = sp_txns[i];
        txn->reset();
        txn_id = txn->transaction_id;
        if (!process_single_transaction(txn)) {
          txn->abort_lock = true;
          this->n_abort_lock.fetch_add(1);
        } else {
            if (++cnt % 5 == 0&& this->partitioner->replica_num() > 1 && is_replica_worker == false) {
              send_commands_to_replica(true);
            }
        }
        handle_requests();
      }
      if (this->partitioner->replica_num() > 1 && is_replica_worker == false) {
        send_commands_to_replica(true);
      }

      size_t rounds = 0;
      if (true) {
        process_execution_phase(mp_txns);
        handle_requests();
        process_commit_phase(mp_txns);
        ++rounds;
      } else {
        size_t mp_txn_to_process = mp_txns.size();
        size_t current_mp_txn_to_process = mp_txns.size();
        while (current_mp_txn_to_process > mp_txn_to_process * 0.05) {
          process_execution_phase(mp_txns);
          handle_requests();
          process_commit_phase(mp_txns);
          std::vector<TransactionType*> tmp;
          for (size_t i = 0; i < mp_txns.size(); ++i) {
            if (mp_txns[i]->abort_lock && mp_txns[i]->abort_no_retry == false) {
              tmp.push_back(mp_txns[i]);
            }
          }
          std::swap(tmp, mp_txns);
          current_mp_txn_to_process = mp_txns.size();
          ++rounds;
        }
      }
      execution_phase_mp_rounds.add(rounds);
    }
    if (this->partitioner->replica_num() > 1 && is_replica_worker == false) {
      send_commands_to_replica(true);
    }

    uint64_t commit_persistence_us = 0;
    uint64_t commit_replication_us = 0;
    {
      {
        ScopedTimer t0([&, this](uint64_t us) {
          commit_persistence_us = us;
        });
        DCHECK((int)workers_need_persist_cmd_buffer.size() == this->cluster_worker_num);
        for (int i = 0; i < (int)workers_need_persist_cmd_buffer.size(); ++i) {
          if (!workers_need_persist_cmd_buffer[i] || i == this_cluster_worker_id)
            continue;
          cluster_worker_messages[i]->set_transaction_id(txn_id);
          MessageFactoryType::new_persist_cmd_buffer_message(*cluster_worker_messages[i], 0, this_cluster_worker_id);
          sent_persist_cmd_buffer_requests++;
        }
        flush_messages();
      }

      bool cmd_buffer_flushed = false;
      if (is_replica_worker == false && this->partitioner->replica_num() > 1) {
        ScopedTimer t1([&, this](uint64_t us) {
          commit_replication_us = us;
          replication_time.add(us);
        });
        int first_account = 0;
        int communication_rounds = 0;
        auto minimum_coord_txn_written_log_position_snap = minimum_coord_txn_written_log_position;
        while (minimum_replayed_log_position < minimum_coord_txn_written_log_position_snap) {
          ScopedTimer t2([&, this](uint64_t us) {
            this->replica_progress_query_latency.add(us);
          });
          // Keep querying the replica for its replayed log position until minimum_replayed_log_position >= minimum_coord_txn_written_log_position_snap
          cluster_worker_messages[replica_cluster_worker_id]->set_transaction_id(txn_id);
          MessageFactoryType::new_get_replayed_log_posistion_message(*cluster_worker_messages[replica_cluster_worker_id], 1, this_cluster_worker_id);
          flush_messages();
          get_replica_replay_log_position_requests++;
          while (get_replica_replay_log_position_responses < get_replica_replay_log_position_requests) {
            if (cmd_buffer_flushed == false) {
              ScopedTimer t0([&, this](uint64_t us) {
                commit_persistence_us = us;
              });
              persist_and_clear_command_buffer(true);
              cmd_buffer_flushed = true;
            }
            handle_requests();
          }
          DCHECK(get_replica_replay_log_position_responses == get_replica_replay_log_position_requests);
          if (first_account == 0) {
            //if (minimum_replayed_log_position < minimum_coord_txn_written_log_position_snap) {
              auto gap = std::max((int64_t)0, minimum_coord_txn_written_log_position_snap - minimum_replayed_log_position);
              this->replication_gap_after_active_replica_execution.add(gap);
            //}
            first_account = 1;
          }
          communication_rounds++;
        }
        this->replication_sync_comm_rounds.add(communication_rounds);
      }
      if (cmd_buffer_flushed == false) {
        ScopedTimer t0([&, this](uint64_t us) {
          commit_persistence_us = us;
        });
        persist_and_clear_command_buffer(true);
        cmd_buffer_flushed = true;
      }
    }

    auto & txns = all_txns;
    size_t committed = 0;
    for (size_t i = 0; i < txns.size(); ++i) {
      auto txn = txns[i];
      if (txn->abort_lock || txn->abort_no_retry)
        continue;
      committed++;
      txn->record_commit_persistence_time(commit_persistence_us);
      txn->record_commit_replication_time(commit_replication_us);
      //DCHECK(!txn->is_single_partition());
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
    this->round_concurrency.add(txns.size());
    this->effective_round_concurrency.add(committed);
  }

  void process_batch_of_transactions() {
    if (pending_txns.empty())
      return;
    // if (is_replica_worker == false) {
    //   reorder_pending_txns();
    // }
    std::size_t until_ith = pending_txns.size();
    // Execution
    
    auto txns = std::vector<TransactionType*>(pending_txns.begin(), pending_txns.begin() + until_ith);
    std::vector<TransactionType*> sp_txns;
    std::vector<TransactionType*> mp_txns;
    for (size_t i = 0; i < txns.size(); ++i) {
      if (txns[i]->is_single_partition()) {
        sp_txns.push_back(txns[i]);
      } else {
        mp_txns.push_back(txns[i]);
      }
    }
    execute_transaction_batch(txns, sp_txns, mp_txns);

    DCHECK(until_ith <= pending_txns.size());
    for (std::size_t i = 0; i < until_ith; ++i) {
      if (pending_txns.front()->abort_lock == false || pending_txns.front()->abort_no_retry) {
        std::unique_ptr<TransactionType> txn(pending_txns.front());
        pending_txns.pop_front();
      } else {
        pending_txns.push_back(pending_txns.front());
        pending_txns.pop_front();
      }
    }
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
    process_batch_of_transactions();
    return;
  }

  bool processing_mp = false;
  void replay_sp_commands(int partition) {
    int i = partition_to_cmd_queue_index[partition];
    if (partition_command_queue_processing[i])
        return;
    auto & q = partition_command_queues[i];
    if (q.empty())
      return;
    partition_command_queue_processing[i] = true;
    while (q.empty() == false) {
      auto & cmd = q.front();
      if (cmd.is_coordinator == false) {
        //DCHECK(false);
        if (owned_partition_locked_by[cmd.partition_id] == -1) {
          // The transaction owns the partition.
          // Wait for coordinator transaction finish reading/writing and unlock the partiiton.
          owned_partition_locked_by[cmd.partition_id] = cmd.tid;
          DCHECK(get_partition_last_replayed_position_in_log(cmd.partition_id) <= cmd.position_in_log);
          get_partition_last_replayed_position_in_log(cmd.partition_id) = cmd.position_in_log;
          cmd_stall_time.add(std::chrono::duration_cast<std::chrono::microseconds>(
              std::chrono::steady_clock::now() - cmd.queue_ts)
              .count());
          q.pop_front();
        } else {
          if (cmd.queue_head_processed == false) {
            cmd_queue_time.add(std::chrono::duration_cast<std::chrono::microseconds>(
              std::chrono::steady_clock::now() - cmd.queue_ts)
              .count());
            cmd.queue_ts = std::chrono::steady_clock::now();
            cmd.queue_head_processed = true;
          }
          // The partition is being locked by front transaction executing, try next time.
          break;
        }
      } else if (cmd.is_mp == false) {
        if (cmd.txn->being_replayed == false) {
          auto queue_time =
          std::chrono::duration_cast<std::chrono::microseconds>(
              std::chrono::steady_clock::now() - cmd.txn->startTime)
              .count();
          cmd.txn->being_replayed = true;
          cmd.txn->startTime = std::chrono::steady_clock::now();
          cmd.txn->record_commit_prepare_time(queue_time);
        }
        if (owned_partition_locked_by[cmd.partition_id] == -1) {
          // The transaction owns the partition.
          // Start executing the sp transaction.
          auto sp_txn = cmd.txn.get();
          DCHECK(sp_txn->transaction_id);
          DCHECK(sp_txn);
          auto res = process_single_transaction(sp_txn);
          DCHECK(res);
          if (res) {
            auto latency =
            std::chrono::duration_cast<std::chrono::microseconds>(
                std::chrono::steady_clock::now() - sp_txn->startTime)
                .count();
            this->percentile.add(latency);
            this->local_latency.add(latency);
            this->record_txn_breakdown_stats(*sp_txn);
            DCHECK(get_partition_last_replayed_position_in_log(cmd.partition_id) <= cmd.position_in_log);
            get_partition_last_replayed_position_in_log(cmd.partition_id) = cmd.position_in_log;
            // Make sure it is unlocked
            DCHECK(owned_partition_locked_by[cmd.partition_id] == -1);
            q.pop_front();
          } else {
            // Make sure it is unlocked
            DCHECK(owned_partition_locked_by[cmd.partition_id] == -1);
          }
        } else {
          // The partition is being locked by front transaction executing, try next time.
          break;
        }
      } else {
        break;
      }
    }
    partition_command_queue_processing[i] = false;
  }

  void replay_all_sp_commands() {
    for (auto p : managed_partitions) {
      replay_sp_commands(p);
      handle_requests(false);
    }
  }

  void replay_commands_with_priority() {
    DCHECK(is_replica_worker);
    replay_all_sp_commands();
    if (processing_mp)
      return;
    std::vector<int> partition_orders;
    for (size_t i = 0; i < partition_command_queues.size(); ++i) {
      auto & q =  partition_command_queues[i];
      if (!q.empty() && q.front().is_coordinator == true && q.front().is_mp == true && partition_command_queue_processing[i] == false) {
        partition_orders.push_back(i);
      }
    }
    if (partition_orders.empty())
      return;
    processing_mp = true;
    std::sort(partition_orders.begin(), partition_orders.end(), [ &, this](int x, int y) { return partition_command_queues[x].front().position_in_log < partition_command_queues[y].front().position_in_log; });
    for (auto i : partition_orders) {
      if (partition_command_queue_processing[i])
        continue;
      partition_command_queue_processing[i] = true;
      auto & q = partition_command_queues[i];
      DCHECK(q.empty() == false);
      auto & cmd = q.front();
      DCHECK(cmd.is_coordinator == true && cmd.is_mp == true);
      if (cmd.txn->being_replayed == false) {
        auto queue_time =
        std::chrono::duration_cast<std::chrono::microseconds>(
            std::chrono::steady_clock::now() - cmd.txn->startTime)
            .count();
        cmd.txn->being_replayed = true;
        cmd.txn->startTime = std::chrono::steady_clock::now();
        cmd.txn->record_commit_prepare_time(queue_time);
      }
      //DCHECK(false);
      DCHECK(cmd.txn.get());
      auto mp_txn = cmd.txn.get();
      DCHECK(mp_txn->transaction_id);
      DCHECK(mp_txn);
      auto res = process_single_transaction(mp_txn);
      if (res) {
        auto latency =
        std::chrono::duration_cast<std::chrono::microseconds>(
            std::chrono::steady_clock::now() - mp_txn->startTime)
            .count();
        this->percentile.add(latency);
        this->dist_latency.add(latency);
        this->record_txn_breakdown_stats(*mp_txn);
        q.pop_front();
      } else {
        // The txn failed to execute due to lock conflicts because other partitions have not been locked yet by their replay threads.
        // Retry next time.
      }
      partition_command_queue_processing[i] = false;
    }
    processing_mp = false;
  }

  void replay_commands() {
    DCHECK(is_replica_worker);
    for (size_t i = 0; i < partition_command_queues.size(); ++i){
      if (partition_command_queue_processing[i])
        continue;
      auto & q = partition_command_queues[i];
      if (q.empty())
        continue;
      partition_command_queue_processing[i] = true;
      while (q.empty() == false) {
        handle_requests(false);
        auto & cmd = q.front();
        if (cmd.is_coordinator == false) {
          //DCHECK(false);
          if (owned_partition_locked_by[cmd.partition_id] == -1) {
            // The transaction owns the partition.
            // Wait for coordinator transaction finish reading/writing and unlock the partiiton.
            owned_partition_locked_by[cmd.partition_id] = cmd.tid;
            DCHECK(get_partition_last_replayed_position_in_log(cmd.partition_id) <= cmd.position_in_log);
            get_partition_last_replayed_position_in_log(cmd.partition_id) = cmd.position_in_log;
            cmd_stall_time.add(std::chrono::duration_cast<std::chrono::microseconds>(
                std::chrono::steady_clock::now() - cmd.queue_ts)
                .count());
            q.pop_front();
          } else {
            if (cmd.queue_head_processed == false) {
              cmd_queue_time.add(std::chrono::duration_cast<std::chrono::microseconds>(
                std::chrono::steady_clock::now() - cmd.queue_ts)
                .count());
              cmd.queue_ts = std::chrono::steady_clock::now();
              cmd.queue_head_processed = true;
            }
            // The partition is being locked by front transaction executing, try next time.
            break;
          }
        } else if (cmd.is_mp == false) {
          if (cmd.txn->being_replayed == false) {
            auto queue_time =
            std::chrono::duration_cast<std::chrono::microseconds>(
                std::chrono::steady_clock::now() - cmd.txn->startTime)
                .count();
            cmd.txn->being_replayed = true;
            cmd.txn->startTime = std::chrono::steady_clock::now();
            cmd.txn->record_commit_prepare_time(queue_time);
          }
          if (owned_partition_locked_by[cmd.partition_id] == -1) {
            // The transaction owns the partition.
            // Start executing the sp transaction.
            auto sp_txn = cmd.txn.get();
            DCHECK(sp_txn->transaction_id);
            DCHECK(sp_txn);
            auto res = process_single_transaction(sp_txn);
            DCHECK(res);
            if (res) {
              auto latency =
              std::chrono::duration_cast<std::chrono::microseconds>(
                  std::chrono::steady_clock::now() - sp_txn->startTime)
                  .count();
              this->percentile.add(latency);
              this->local_latency.add(latency);
              this->record_txn_breakdown_stats(*sp_txn);
              DCHECK(get_partition_last_replayed_position_in_log(cmd.partition_id) <= cmd.position_in_log);
              get_partition_last_replayed_position_in_log(cmd.partition_id) = cmd.position_in_log;
              // Make sure it is unlocked
              DCHECK(owned_partition_locked_by[cmd.partition_id] == -1);
              q.pop_front();
            } else {
              // Make sure it is unlocked
              DCHECK(owned_partition_locked_by[cmd.partition_id] == -1);
            }
          } else {
            // The partition is being locked by front transaction executing, try next time.
            break;
          }
        } else { // MP transaction and this is the initiator
          if (processing_mp == true)
            break;
          if (cmd.txn->being_replayed == false) {
            auto queue_time =
            std::chrono::duration_cast<std::chrono::microseconds>(
                std::chrono::steady_clock::now() - cmd.txn->startTime)
                .count();
            cmd.txn->being_replayed = true;
            cmd.txn->startTime = std::chrono::steady_clock::now();
            cmd.txn->record_commit_prepare_time(queue_time);
          }
          processing_mp = true;
          //DCHECK(false);
          DCHECK(cmd.txn.get());
          auto mp_txn = cmd.txn.get();
          DCHECK(mp_txn->transaction_id);
          DCHECK(mp_txn);
          auto res = process_single_transaction(mp_txn);
          if (res) {
            auto latency =
            std::chrono::duration_cast<std::chrono::microseconds>(
                std::chrono::steady_clock::now() - mp_txn->startTime)
                .count();
            this->percentile.add(latency);
            this->dist_latency.add(latency);
            this->record_txn_breakdown_stats(*mp_txn);
            processing_mp = false;
            txn_retries.add(mp_txn->tries);
            q.pop_front();
          } else {
            processing_mp = false;
            // The txn failed to execute due to lock conflicts because other partitions have not been locked yet by their replay threads.
            // Retry next time.
            break;
          }
        }
      }
      partition_command_queue_processing[i] = false;
      handle_requests(false);
    }
  }
  
  void replay_commands_round_roubin() {
    DCHECK(is_replica_worker);
    for (size_t i = 0; i < partition_command_queues.size(); ++i){
      if (partition_command_queue_processing[i])
        continue;
      auto & q = partition_command_queues[i];
      if (q.empty())
        continue;
      partition_command_queue_processing[i] = true;
      auto & cmd = q.front();
      if (cmd.is_coordinator == false) {
        //DCHECK(false);
        if (owned_partition_locked_by[cmd.partition_id] == -1) {
          // The transaction owns the partition.
          // Wait for coordinator transaction finish reading/writing and unlock the partiiton.
          owned_partition_locked_by[cmd.partition_id] = cmd.tid;
          DCHECK(get_partition_last_replayed_position_in_log(cmd.partition_id) <= cmd.position_in_log);
          get_partition_last_replayed_position_in_log(cmd.partition_id) = cmd.position_in_log;
          q.pop_front();
        } else {
          // The partition is being locked by front transaction executing, try next time.
        }
      } else if (cmd.is_mp == false) {
        if (owned_partition_locked_by[cmd.partition_id] == -1) {
          // The transaction owns the partition.
          // Start executing the sp transaction.
          auto sp_txn = cmd.txn.get();
          DCHECK(sp_txn->transaction_id);
          DCHECK(sp_txn);
          auto res = process_single_transaction(sp_txn);
          DCHECK(res);
          if (res) {
            auto latency =
            std::chrono::duration_cast<std::chrono::microseconds>(
                std::chrono::steady_clock::now() - sp_txn->startTime)
                .count();
            this->percentile.add(latency);
            this->local_latency.add(latency);
            this->record_txn_breakdown_stats(*sp_txn);
            DCHECK(get_partition_last_replayed_position_in_log(cmd.partition_id) <= cmd.position_in_log);
            get_partition_last_replayed_position_in_log(cmd.partition_id) = cmd.position_in_log;
            // Make sure it is unlocked
            DCHECK(owned_partition_locked_by[cmd.partition_id] == -1);
            q.pop_front();
          } else {
            // Make sure it is unlocked
            DCHECK(owned_partition_locked_by[cmd.partition_id] == -1);
          }
        } else {
           // The partition is being locked by front transaction executing, try next time.
        }
      } else if (processing_mp == false) { // MP transaction and this is the initiator
        //DCHECK(false);
        if (cmd.txn->being_replayed == false) {
          auto queue_time =
          std::chrono::duration_cast<std::chrono::microseconds>(
              std::chrono::steady_clock::now() - cmd.txn->startTime)
              .count();
          cmd.txn->being_replayed = true;
          cmd.txn->startTime = std::chrono::steady_clock::now();
          cmd.txn->startTime = std::chrono::steady_clock::now();
          cmd.txn->record_commit_prepare_time(queue_time);
        }
        processing_mp = true;
        DCHECK(cmd.txn.get());
        auto mp_txn = cmd.txn.get();
        DCHECK(mp_txn->transaction_id);
        DCHECK(mp_txn);
        auto res = process_single_transaction(mp_txn);
        if (res) {
          auto latency =
          std::chrono::duration_cast<std::chrono::microseconds>(
              std::chrono::steady_clock::now() - mp_txn->startTime)
              .count();
          this->percentile.add(latency);
          this->dist_latency.add(latency);
          this->record_txn_breakdown_stats(*mp_txn);
          //DCHECK(get_partition_last_replayed_position_in_log(cmd.partition_id) <= cmd.position_in_log);
          //get_partition_last_replayed_position_in_log(cmd.partition_id) = cmd.position_in_log;
          q.pop_front();
        } else {
          // The txn failed to execute due to lock conflicts because other partitions have not lock the partitions yet.
          // Retry next time.
        }
        processing_mp = false;
      }
      partition_command_queue_processing[i] = false;
      handle_requests(false);
    }
  }


  void send_commands_to_replica(bool persist = false) {
    if (command_buffer_outgoing.empty())
      return; // Nothing to send
    auto data = serialize_commands(command_buffer_outgoing.begin(), command_buffer_outgoing.end());
    MessageFactoryType::new_command_replication(
            *cluster_worker_messages[replica_cluster_worker_id], 1, data, this_cluster_worker_id, persist);
    flush_messages();
    //LOG(INFO) << "This cluster worker " << this_cluster_worker_id << " sent " << command_buffer_outgoing.size() << " commands to replia worker " <<  replica_cluster_worker_id;
    command_buffer_outgoing.clear();
  }

  void push_replica_message(Message *message) override { 
    DCHECK(message->get_is_replica());
    replica_worker->push_message(message);
  }

  std::deque<TransactionType*> to_commit_dummy;
  std::size_t handle_requests(bool should_replay_commands = true) {
    auto ret = handle_requests_and_collect_ready_to_commit_txns(to_commit_dummy, should_replay_commands);
    to_commit_dummy.clear();
    return ret;
  }

  std::size_t handle_requests_and_collect_ready_to_commit_txns(std::deque<TransactionType*> & to_commit, bool should_replay_commands = true) {
    std::size_t size = 0;
    while (!this->in_queue.empty()) {
      ++size;
      std::unique_ptr<Message> message(this->in_queue.front());
      bool ok = this->in_queue.pop();
      CHECK(ok);
      DCHECK(message->get_worker_id() == this->id);
      if (message->get_is_replica())
        DCHECK(is_replica_worker);
      else
        DCHECK(!is_replica_worker);

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
        TransactionType * txn = nullptr;
        if (active_txns.count(tid) > 0) {
          txn = active_txns[tid];
        }
        if (type == (int)HStoreMessage::ACQUIRE_PARTITION_LOCK_AND_READ_REQUEST) {
          acquire_partition_lock_and_read_request_handler(*message, messagePiece,
                                                 *cluster_worker_messages[message->get_source_cluster_worker_id()], *table,
                                                 txn);
        } else if (type == (int)HStoreMessage::ACQUIRE_PARTITION_LOCK_AND_READ_RESPONSE) {
          acquire_partition_lock_and_read_response_handler(*message, messagePiece,
                                                 *cluster_worker_messages[message->get_source_cluster_worker_id()], *table,
                                                 txn);
          DCHECK(txn);
          if (txn->pendingResponses == 0) {
            to_commit.push_back(txn);
          }
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
        } else if (type == (int)HStoreMessage::GET_REPLAYED_LOG_POSITION_REQUEST) {
          get_replayed_log_position_request_handler(*message, messagePiece, *cluster_worker_messages[message->get_source_cluster_worker_id()], 
                                                *table, 
                                                txn);
        } else if (type == (int)HStoreMessage::GET_REPLAYED_LOG_POSITION_RESPONSE) {
          get_replayed_log_position_response_handler(*message, messagePiece, *cluster_worker_messages[message->get_source_cluster_worker_id()], 
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

    if (should_replay_commands && this->partitioner->replica_num() > 1 && is_replica_worker) {
      replay_commands();
    }
    // if (this->partitioner->replica_num() > 1 && is_replica_worker == false) {
    //   send_commands_to_replica();
    // }
    return size;
  }

  void drive_event_loop(bool new_transaction = true) {
    handle_requests();
    if (new_transaction && is_replica_worker == false)
      process_new_transactions();
    if (this->partitioner->replica_num() > 1 && is_replica_worker) {
      replay_commands();
    }
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
      //std::this_thread::sleep_for(std::chrono::microseconds(10));
      return nullptr;
    } else {
      auto partition_id = managed_partitions[this->random.next() % managed_partitions.size()];
      auto txn = this->workload.next_transaction(this->context, partition_id, this->id).release();
      return txn;
    }
  }

  void start() override {
    LOG(INFO) << "Executor " << (is_replica_worker ? "Replica" : "") << this->id << " starts with thread id" << gettid();

    last_commit = std::chrono::steady_clock::now();
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
      drive_event_loop();
      status = static_cast<ExecutorStatus>(this->worker_status.load());
    } while (status != ExecutorStatus::STOP);
    
    if (is_replica_worker == true) {
      onExit();
    }
    if (is_replica_worker == false)
      this->n_complete_workers.fetch_add(1);

    // once all workers are stopped, we need to process the replication
    // requests

    while (static_cast<ExecutorStatus>(this->worker_status.load()) !=
           ExecutorStatus::CLEANUP) {
      drive_event_loop(false);
    }

    drive_event_loop(false);
    if (is_replica_worker == false)
      this->n_complete_workers.fetch_add(1);
    //LOG(INFO) << "Executor " << this->id << " exits.";
  }

  void onExit() override {

    LOG(INFO) << (is_replica_worker ? "Replica" : "") << " Worker " << this->id << " commit: "<< this->worker_commit 
              << ". batch concurrency: " << this->round_concurrency.nth(50) 
              << ". effective batch concurrency: " << this->effective_round_concurrency.nth(50) 
              << ". latency: " << this->percentile.nth(50)
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
              << " (99%). " << " spread_avg_time " << spread_time.avg() << ". "
              << " replica progress query latency " << replica_progress_query_latency.nth(50) << " us(50%), " << replica_progress_query_latency.avg() << " us(avg). "
              << " replication gap " << replication_gap_after_active_replica_execution.avg() 
              << " replication sync comm rounds " << replication_sync_comm_rounds.avg() 
              << " replication time " << replication_time.avg() 
              << " txn tries " << txn_retries.avg() 
              << " commit interval " << commit_interval.nth(50) 
              << " cmd queue time " << cmd_queue_time.nth(50)
              << " cmd stall time by lock " << cmd_stall_time.nth(50) 
              << " execution phase time " << execution_phase_time.avg()
              << " execution phase mp rounds " << execution_phase_mp_rounds.avg() << ". \n"
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
