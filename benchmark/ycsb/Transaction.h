//
// Created by Yi Lu on 7/22/18.
//

#pragma once

#include "glog/logging.h"

#include "benchmark/ycsb/Database.h"
#include "benchmark/ycsb/Query.h"
#include "benchmark/ycsb/Schema.h"
#include "benchmark/ycsb/Storage.h"
#include "common/Operation.h"
#include "core/Defs.h"
#include "core/Partitioner.h"
#include "core/Table.h"

namespace star {
namespace ycsb {

template <class Transaction> class ReadModifyWrite : public Transaction {

public:
  using DatabaseType = Database;
  using ContextType = typename DatabaseType::ContextType;
  using RandomType = typename DatabaseType::RandomType;
  using StorageType = Storage;

  static constexpr std::size_t keys_num = 10;

  ReadModifyWrite(std::size_t coordinator_id, std::size_t partition_id,
                  DatabaseType &db, const ContextType &context,
                  RandomType &random, Partitioner &partitioner,
                  std::size_t ith_replica = 0)
      : Transaction(coordinator_id, partition_id, partitioner, ith_replica), db(db),
        context(context), random(random),
        partition_id(partition_id),
        query(makeYCSBQuery<keys_num>()(context, partition_id, random, partitioner)) {}

  virtual int32_t get_partition_count() override { return query.number_of_parts(); }

  virtual int32_t get_partition(int i) override { return query.get_part(i); }
  
  virtual bool is_single_partition() override { return query.number_of_parts() == 1; }

  virtual ~ReadModifyWrite() override = default;

  virtual const std::string serialize(std::size_t ith_replica = 0) override {
    std::string res;
    Encoder encoder(res);
    encoder << ith_replica << this->txn_random_seed_start << partition_id;
    encoder << get_partition_count();
    for (int32_t i = 0; i < get_partition_count(); ++i)
      encoder << get_partition(i);
    return res;
  }

  TransactionResult execute(std::size_t worker_id) override {
    ScopedTimer t_local_work([&, this](uint64_t us) {
      this->record_local_work_time(us);
    });
    DCHECK(context.keysPerTransaction == keys_num);

    int ycsbTableID = ycsb::tableID;

    for (auto i = 0u; i < keys_num; i++) {
      auto key = query.Y_KEY[i];
      storage.ycsb_keys[i].Y_KEY = key;
      if (query.UPDATE[i]) {
        this->search_for_update(ycsbTableID, context.getPartitionID(key),
                                storage.ycsb_keys[i], storage.ycsb_values[i]);
      } else {
        this->search_for_read(ycsbTableID, context.getPartitionID(key),
                              storage.ycsb_keys[i], storage.ycsb_values[i]);
      }
    }

    t_local_work.end();
    if (this->process_requests(worker_id)) {
      return TransactionResult::ABORT;
    }
    t_local_work.reset();
    for (auto i = 0u; i < keys_num; i++) {
      auto key = query.Y_KEY[i];
      if (query.UPDATE[i]) {

        if (this->execution_phase) {
          storage.ycsb_values[i].Y_F01.assign(
              random.a_string(YCSB_FIELD_SIZE, YCSB_FIELD_SIZE));
          storage.ycsb_values[i].Y_F02.assign(
              random.a_string(YCSB_FIELD_SIZE, YCSB_FIELD_SIZE));
          storage.ycsb_values[i].Y_F03.assign(
              random.a_string(YCSB_FIELD_SIZE, YCSB_FIELD_SIZE));
          storage.ycsb_values[i].Y_F04.assign(
              random.a_string(YCSB_FIELD_SIZE, YCSB_FIELD_SIZE));
          storage.ycsb_values[i].Y_F05.assign(
              random.a_string(YCSB_FIELD_SIZE, YCSB_FIELD_SIZE));
          storage.ycsb_values[i].Y_F06.assign(
              random.a_string(YCSB_FIELD_SIZE, YCSB_FIELD_SIZE));
          storage.ycsb_values[i].Y_F07.assign(
              random.a_string(YCSB_FIELD_SIZE, YCSB_FIELD_SIZE));
          storage.ycsb_values[i].Y_F08.assign(
              random.a_string(YCSB_FIELD_SIZE, YCSB_FIELD_SIZE));
          storage.ycsb_values[i].Y_F09.assign(
              random.a_string(YCSB_FIELD_SIZE, YCSB_FIELD_SIZE));
          storage.ycsb_values[i].Y_F10.assign(
              random.a_string(YCSB_FIELD_SIZE, YCSB_FIELD_SIZE));
        }

        this->update(ycsbTableID, context.getPartitionID(key),
                     storage.ycsb_keys[i], storage.ycsb_values[i]);
      }
    }

    if (this->execution_phase && context.nop_prob > 0) {
      auto x = random.uniform_dist(1, 10000);
      if (x <= context.nop_prob) {
        for (auto i = 0u; i < context.n_nop; i++) {
          asm("nop");
        }
      }
    }

    return TransactionResult::READY_TO_COMMIT;
  }

  void reset_query() override {
    query = makeYCSBQuery<keys_num>()(context, partition_id, random, this->partitioner);
  }

private:
  DatabaseType &db;
  const ContextType &context;
  RandomType random;
  Storage storage;
  std::size_t partition_id;
  YCSBQuery<keys_num> query;
};
} // namespace ycsb

} // namespace star
