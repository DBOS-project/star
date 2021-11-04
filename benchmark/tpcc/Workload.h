//
// Created by Yi Lu on 7/24/18.
//

#pragma once

#include <string>
#include "benchmark/tpcc/Context.h"
#include "benchmark/tpcc/Database.h"
#include "benchmark/tpcc/Random.h"
#include "benchmark/tpcc/Storage.h"
#include "benchmark/tpcc/Transaction.h"
#include "core/Partitioner.h"

namespace star {

namespace tpcc {

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
                                                    std::size_t worker_id) {

    int x = random.uniform_dist(1, 100);
    std::unique_ptr<TransactionType> p;

    static std::atomic<uint64_t> tid_cnt(0);
    long long transactionId = tid_cnt.fetch_add(1);
    auto random_seed = Time::now();


    std::string transactionType;
    random.init_seed(random_seed);
    if (context.workloadType == TPCCWorkloadType::MIXED) {
      if (x <= 50) {
        p = std::make_unique<NewOrder<Transaction>>(
            coordinator_id, partition_id, db, context, random, partitioner);
        transactionType = "TPCC NewOrder";
      } else {
        p = std::make_unique<Payment<Transaction>>(coordinator_id, partition_id,
                                                   db, context, random,
                                                   partitioner);
        transactionType = "TPCC Payment";
      }
    } else if (context.workloadType == TPCCWorkloadType::NEW_ORDER_ONLY) {
      p = std::make_unique<NewOrder<Transaction>>(coordinator_id, partition_id,
                                                  db, context, random,
                                                  partitioner);
      transactionType = "TPCC NewOrder";
    } else {
      p = std::make_unique<Payment<Transaction>>(coordinator_id, partition_id,
                                                 db, context, random,
                                                 partitioner);
      transactionType = "TPCC NewOrder";
    }

    return p;
  }

  std::unique_ptr<TransactionType> deserialize_from_raw(ContextType &context, const std::string & data) {
    Decoder decoder(data);
    uint64_t seed;
    uint32_t txn_type;
    std::size_t ith_replica;
    std::size_t partition_id;
    decoder >> txn_type >> ith_replica >> seed >> partition_id;
    RandomType random;
    random.init_seed(seed);

    if (txn_type == 0) {
      return std::make_unique<NewOrder<Transaction>>(
            coordinator_id, partition_id, db, context, random, partitioner,
             ith_replica);
    } else {
      return std::make_unique<Payment<Transaction>>(coordinator_id, partition_id,
                                                   db, context, random,
                                                   partitioner, ith_replica);
    }
  }

private:
  std::size_t coordinator_id;
  DatabaseType &db;
  RandomType &random;
  Partitioner &partitioner;
};

} // namespace tpcc
} // namespace star
