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

    
  static int64_t next_transaction_id(uint64_t coordinator_id) {
    constexpr int coordinator_id_offset = 56;
    static std::atomic<int64_t> tid_static{1};
    auto tid = tid_static.fetch_add(1);
    return ((int64_t)coordinator_id << coordinator_id_offset) | tid;
  }

  std::unique_ptr<TransactionType> next_transaction(ContextType &context,
                                                    std::size_t partition_id,
                                                    std::size_t worker_id) {
    // const static uint32_t num_workers_per_node = context.partition_num / context.coordinator_num;
    // int cluster_worker_id = coordinator_id * num_workers_per_node + worker_id;
    // if (cluster_worker_id == 1) {
    //   context.crossPartitionProbability = 100;
    // }

    static std::atomic<uint64_t> tid_cnt(0);
    long long transactionId = tid_cnt.fetch_add(1);
    auto random_seed = Time::now();
    random.set_seed(random_seed);
    std::unique_ptr<TransactionType> p =
        std::make_unique<ReadModifyWrite<Transaction>>(
            coordinator_id, partition_id, db, context, random, partitioner);
    p->txn_random_seed_start = random_seed;
    p->transaction_id = next_transaction_id(coordinator_id);
    return p;
  }

  std::unique_ptr<TransactionType> deserialize_from_raw(ContextType &context, const std::string & data) {
    Decoder decoder(data);
    uint64_t seed;
    std::size_t ith_replica;
    std::size_t partition_id;
    int32_t partition_count;
    int64_t transaction_id;
    uint64_t straggler_wait_time;

    //std::vector<int32_t> partitions;
    decoder >> transaction_id >> straggler_wait_time >> ith_replica >> seed >> partition_id >> partition_count;
    // for (int32_t i = 0; i < partition_count; ++i){
    //   int32_t p;
    //   decoder >> p;
    //   partitions.push_back(p);
    // }
    RandomType random;
    random.set_seed(seed);
 
    std::unique_ptr<TransactionType> p =
        std::make_unique<ReadModifyWrite<Transaction>>(
            coordinator_id, partition_id, db, context, random, partitioner, ith_replica);
    p->txn_random_seed_start = seed;
    DCHECK(p->get_partition_count() == partition_count);
    // for (int32_t i = 0; i < partition_count; ++i){
    //   DCHECK(partitions[i] == p->get_partition(i));
    // }
    p->transaction_id = transaction_id;
    p->straggler_wait_time = straggler_wait_time;
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
