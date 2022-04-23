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
static thread_local std::vector<Storage*> storage_cache;

Storage* get_storage() {
  if (storage_cache.empty()) {
    for (size_t i = 0; i < 10; ++i) {
      storage_cache.push_back(new Storage());
    }
  }
  Storage * last = storage_cache.back();
  storage_cache.pop_back();
  return last;
}

void put_storage(Storage * s) {
  storage_cache.push_back(s);
}

template <class Transaction> class ReadModifyWrite : public Transaction {

public:
  using DatabaseType = Database;
  using ContextType = typename DatabaseType::ContextType;
  using RandomType = typename DatabaseType::RandomType;
  using StorageType = Storage;

  static constexpr std::size_t keys_num = 10;

  ReadModifyWrite(std::size_t coordinator_id, std::size_t partition_id, std::size_t granule_id,
                  DatabaseType &db, const ContextType &context,
                  RandomType &random, Partitioner &partitioner,
                  std::size_t ith_replica = 0)
      : Transaction(coordinator_id, partition_id, partitioner, ith_replica), db(db),
        context(context), random(random),
        partition_id(partition_id), granule_id(granule_id),
        query(makeYCSBQuery<keys_num>()(context, partition_id, granule_id, random, partitioner)) {
          storage = get_storage();
  }

  virtual int32_t get_partition_count() override { return query.number_of_parts(); }

  virtual int32_t get_partition(int ith_partition) override { return query.get_part(ith_partition); }

  virtual int32_t get_partition_granule_count(int ith_partition) override { return query.get_part_granule_count(ith_partition); }
  // A ycsb transaction accesses only one granule per partition.
  virtual int32_t get_granule(int ith_partition, int j) override { return query.get_granule(ith_partition, j); }

  virtual bool is_single_partition() override { return query.number_of_parts() == 1; }

  virtual ~ReadModifyWrite() {
    put_storage(storage);
    storage = nullptr;
  }

  virtual const std::string serialize(std::size_t ith_replica = 0) override {
    std::string res;
    Encoder encoder(res);
    encoder << this->transaction_id << this->straggler_wait_time << ith_replica << this->txn_random_seed_start << partition_id << granule_id;
    encoder << get_partition_count();
    // int granules_count = 0;
    // for (int32_t i = 0; i < get_partition_count(); ++i)
    //   granules_count += get_partition_granule_count(i);
    // for (int32_t i = 0; i < get_partition_count(); ++i)
    //   encoder << get_partition(i);
    // encoder << granules_count;
    // for (int32_t i = 0; i < get_partition_count(); ++i)
    //   for (int32_t j = 0; j < get_partition_granule_count(i); ++j)
    //     encoder << get_granule(i, j);
    Transaction::serialize_lock_status(encoder);
    return res;
  }

  TransactionResult execute(std::size_t worker_id) override {
    ScopedTimer t_local_work([&, this](uint64_t us) {
      this->record_local_work_time(us);
    });
    DCHECK(context.keysPerTransaction == keys_num);

    int ycsbTableID = ycsb::tableID;
    DCHECK(ycsbTableID < 1);
    for (auto i = 0u; i < keys_num; i++) {
      auto key = query.Y_KEY[i];
      storage->ycsb_keys[i].Y_KEY = key;
      if (query.UPDATE[i]) {
        this->search_for_update(ycsbTableID, context.getPartitionID(key),
                                storage->ycsb_keys[i], storage->ycsb_values[i],
                                context.getGranule(key));
      } else {
        this->search_for_read(ycsbTableID, context.getPartitionID(key),
                              storage->ycsb_keys[i], storage->ycsb_values[i],
                              context.getGranule(key));
      }
    }
    for (auto i = 0u; i < keys_num; i++) {
      auto key = query.Y_KEY[i];
      if (query.UPDATE[i]) {
        this->update(ycsbTableID, context.getPartitionID(key),
                     storage->ycsb_keys[i], storage->ycsb_values[i],
                                context.getGranule(key));
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
          storage->ycsb_values[i].Y_F01.assign(
              random.a_string(YCSB_FIELD_SIZE, YCSB_FIELD_SIZE));
          storage->ycsb_values[i].Y_F02.assign(
              random.a_string(YCSB_FIELD_SIZE, YCSB_FIELD_SIZE));
          storage->ycsb_values[i].Y_F03.assign(
              random.a_string(YCSB_FIELD_SIZE, YCSB_FIELD_SIZE));
          storage->ycsb_values[i].Y_F04.assign(
              random.a_string(YCSB_FIELD_SIZE, YCSB_FIELD_SIZE));
          storage->ycsb_values[i].Y_F05.assign(
              random.a_string(YCSB_FIELD_SIZE, YCSB_FIELD_SIZE));
          storage->ycsb_values[i].Y_F06.assign(
              random.a_string(YCSB_FIELD_SIZE, YCSB_FIELD_SIZE));
          storage->ycsb_values[i].Y_F07.assign(
              random.a_string(YCSB_FIELD_SIZE, YCSB_FIELD_SIZE));
          storage->ycsb_values[i].Y_F08.assign(
              random.a_string(YCSB_FIELD_SIZE, YCSB_FIELD_SIZE));
          storage->ycsb_values[i].Y_F09.assign(
              random.a_string(YCSB_FIELD_SIZE, YCSB_FIELD_SIZE));
          storage->ycsb_values[i].Y_F10.assign(
              random.a_string(YCSB_FIELD_SIZE, YCSB_FIELD_SIZE));
        }

        // this->update(ycsbTableID, context.getPartitionID(key),
        //              storage->ycsb_keys[i], storage->ycsb_values[i],
        //                         context.getGranule(key));
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

    if (this->execution_phase) {
      uint64_t wait_time = 0;
      if (this->context.stragglers_partition == -1 && this->straggler_wait_time) {
        wait_time = this->straggler_wait_time;
      } else if (this->context.stragglers_partition != -1) {
        int partition_count = get_partition_count();
        for(int i = 0; i < partition_count; ++i) {
          if (get_partition(i) == this->context.stragglers_partition) {
            wait_time = this->context.stragglers_total_wait_time;
          }
        }
      }
      if (wait_time) {
        auto start_time = std::chrono::steady_clock::now();
        while ((uint64_t)std::chrono::duration_cast<std::chrono::microseconds>(
                      std::chrono::steady_clock::now() - start_time)
                      .count() < wait_time) {
          this->remote_request_handler(worker_id);
        }
      }
    }

    return TransactionResult::READY_TO_COMMIT;
  }

  void reset_query() override {
    query = makeYCSBQuery<keys_num>()(context, partition_id, granule_id, random, this->partitioner);
  }

private:
  DatabaseType &db;
  const ContextType &context;
  RandomType random;
  Storage * storage = nullptr;
  std::size_t partition_id, granule_id;
  YCSBQuery<keys_num> query;
};
} // namespace ycsb

} // namespace star
