//
// Created by Yi Lu on 7/25/18.
//

#pragma once

#include "benchmark/tpcc/Context.h"
#include "benchmark/ycsb/Database.h"
#include "benchmark/ycsb/Random.h"
#include "benchmark/ycsb/Storage.h"
#include "benchmark/ycsb/Transaction.h"
#include "core/Partitioner.h"

namespace star {

namespace ycsb {

template <class Transaction> class Workload {
public:
  using TransactionType = Transaction;
  using DatabaseType = Database;
  using ContextType = Context;
  using RandomType = Random;
  using StorageType = Storage;

  Workload(std::size_t coordinator_id, DatabaseType &db, RandomType &random,
           Partitioner &partitioner)
      : coordinator_id(coordinator_id), db(db), random(random),
        partitioner(partitioner) {}

  std::unique_ptr<TransactionType> next_transaction(ContextType &context,
                                                    std::size_t partition_id,
                                                    StorageType &storage, 
                                                    std::size_t worker_id) {
    if (context.cross_txn_workers > 0) {
      const static uint32_t num_workers_per_node = context.partition_num / context.coordinator_num;
      int cluster_worker_id = coordinator_id * num_workers_per_node + worker_id;
      if (cluster_worker_id < (int)context.cross_txn_workers) {
        context.crossPartitionProbability = 100;
      } else {
        context.crossPartitionProbability = 0;
      }
    }

    std::unique_ptr<TransactionType> p =
        std::make_unique<ReadModifyWrite<Transaction>>(
            coordinator_id, partition_id, db, context, random, partitioner,
            storage);

    return p;
  }

private:
  std::size_t coordinator_id;
  DatabaseType &db;
  RandomType &random;
  Partitioner &partitioner;
};

} // namespace ycsb
} // namespace star
