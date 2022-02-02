//
// Created by Yi Lu on 1/7/19.
//

#pragma once

#include "core/Manager.h"
#include "core/Partitioner.h"
#include "protocol/Aria/Aria.h"
#include "protocol/Aria/AriaExecutor.h"
#include "protocol/Aria/AriaHelper.h"
#include "protocol/Aria/AriaTransaction.h"

#include <atomic>
#include <thread>
#include <vector>

namespace star {

template <class Workload> class AriaManager : public star::Manager {
public:
  using base_type = star::Manager;

  using WorkloadType = Workload;
  using DatabaseType = typename WorkloadType::DatabaseType;
  using StorageType = typename WorkloadType::StorageType;

  std::unique_ptr<Partitioner> partitioner;
  WorkloadType workload;
  WALLogger * logger = nullptr;
  using TransactionType = AriaTransaction;
  static_assert(std::is_same<typename WorkloadType::TransactionType,
                             TransactionType>::value,
                "Transaction types do not match.");
  using ContextType = typename DatabaseType::ContextType;
  using RandomType = typename DatabaseType::RandomType;
  ContextType ctx;
  AriaManager(std::size_t coordinator_id, std::size_t id, DatabaseType &db,
              const ContextType &context, std::atomic<bool> &stopFlag)
      : base_type(coordinator_id, id, context, stopFlag), partitioner(PartitionerFactory::create_partitioner(
            context.partitioner, coordinator_id, context.coordinator_num)),
            workload(coordinator_id, db, random, *this->partitioner), ctx(context), db(db), epoch(0) {

    storages.resize(context.batch_size);
    transactions.resize(context.batch_size);
    if (context.logger) {
      DCHECK(context.log_path != "");
      logger = context.logger;
    } else {
      if (context.log_path != "") {
        std::string redo_filename =
            context.log_path + "_" + std::to_string(id) + ".txt";
        logger = new SimpleWALLogger(redo_filename.c_str(), context.emulated_persist_latency);
      }
    }
  }


  std::size_t get_partition_id() {

    std::size_t partition_id;

    CHECK(context.partition_num % context.coordinator_num == 0);

    auto partition_num_per_node =
        context.partition_num / context.coordinator_num;
    partition_id = random.uniform_dist(0, partition_num_per_node - 1) *
                       context.coordinator_num +
                   coordinator_id;
    CHECK(partitioner->has_master_partition(partition_id));
    return partition_id;
  }


  void generate_and_log_transactions() {
    uint64_t commit_persistence_time = 0;
    {
      ScopedTimer t2([&, this](uint64_t us) {
        commit_persistence_time = us;
      });
      auto n_abort = total_abort.load();
      std::string txn_command_data = "";
      for (std::size_t i = 0; i < transactions.size(); i++) {

        // if null, generate a new transaction, on this node.
        // else only reset the query

        if (transactions[i] == nullptr || i >= n_abort) {
          auto partition_id = get_partition_id();
          transactions[i] =
              workload.next_transaction(ctx, partition_id, this->id);
          // auto total_batch_size = context.coordinator_num * context.batch_size;
          // if (context.stragglers_per_batch) {
          //   auto v = random.uniform_dist(1, total_batch_size);
          //   if (v <= (uint64_t)context.stragglers_per_batch) {
          //     transactions[i]->straggler_wait_time = context.stragglers_total_wait_time / context.stragglers_per_batch;
          //   }
          // }
          // if (context.straggler_zipf_factor > 0) {
          //   int length_type = star::Zipf::globalZipfForStraggler().value(random.next_double());
          //   transactions[i]->straggler_wait_time = transaction_lengths[length_type];
          //   transaction_lengths_count[length_type]++;
          // }
          txn_command_data += transactions[i]->serialize(0);
        } else {
          transactions[i]->reset();
        }
      }

      this->logger->write(txn_command_data.data(), txn_command_data.size(), true);
    }
    for (auto i = id; i < transactions.size(); i += context.worker_num) {
      transactions[i]->record_commit_persistence_time(commit_persistence_time);
    }
  }
  void coordinator_start() override {

    std::size_t n_workers = context.worker_num;
    std::size_t n_coordinators = context.coordinator_num;

    while (!stopFlag.load()) {

      // the coordinator on each machine first moves the aborted transactions
      // from the last batch earlier to the next batch and set remaining
      // transaction slots to null.

      // then, each worker threads generates a transaction using the same seed.
      epoch.fetch_add(1);
      cleanup_batch();

      generate_and_log_transactions();

      // LOG(INFO) << "Seed: " << random.get_seed();
      n_started_workers.store(0);
      n_completed_workers.store(0);
      signal_worker(ExecutorStatus::Aria_READ);
      wait_all_workers_start();
      wait_all_workers_finish();
      broadcast_stop();
      wait4_stop(n_coordinators - 1);
      n_completed_workers.store(0);
      set_worker_status(ExecutorStatus::STOP);
      wait_all_workers_finish();
      // wait for all machines until they finish the Aria_READ phase.
      wait4_ack();

      // Allow each worker to commit transactions
      n_started_workers.store(0);
      n_completed_workers.store(0);
      signal_worker(ExecutorStatus::Aria_COMMIT);
      wait_all_workers_start();
      wait_all_workers_finish();
      broadcast_stop();
      wait4_stop(n_coordinators - 1);
      n_completed_workers.store(0);
      set_worker_status(ExecutorStatus::STOP);
      wait_all_workers_finish();
      // wait for all machines until they finish the Aria_COMMIT phase.
      wait4_ack();
    }

    signal_worker(ExecutorStatus::EXIT);
  }

  void non_coordinator_start() override {

    std::size_t n_workers = context.worker_num;
    std::size_t n_coordinators = context.coordinator_num;

    for (;;) {
      // LOG(INFO) << "Seed: " << random.get_seed();
      ExecutorStatus status = wait4_signal();
      if (status == ExecutorStatus::EXIT) {
        set_worker_status(ExecutorStatus::EXIT);
        break;
      }

      DCHECK(status == ExecutorStatus::Aria_READ);
      // the coordinator on each machine first moves the aborted transactions
      // from the last batch earlier to the next batch and set remaining
      // transaction slots to null.

      epoch.fetch_add(1);
      cleanup_batch();

      generate_and_log_transactions();

      n_started_workers.store(0);
      n_completed_workers.store(0);
      set_worker_status(ExecutorStatus::Aria_READ);
      wait_all_workers_start();
      wait_all_workers_finish();
      broadcast_stop();
      wait4_stop(n_coordinators - 1);
      n_completed_workers.store(0);
      set_worker_status(ExecutorStatus::STOP);
      wait_all_workers_finish();
      send_ack();

      status = wait4_signal();
      DCHECK(status == ExecutorStatus::Aria_COMMIT);
      n_started_workers.store(0);
      n_completed_workers.store(0);
      set_worker_status(ExecutorStatus::Aria_COMMIT);
      wait_all_workers_start();
      wait_all_workers_finish();
      broadcast_stop();
      wait4_stop(n_coordinators - 1);
      n_completed_workers.store(0);
      set_worker_status(ExecutorStatus::STOP);
      wait_all_workers_finish();
      send_ack();
    }
  }

  void cleanup_batch() {
    std::size_t it = 0;
    for (auto i = 0u; i < transactions.size(); i++) {
      if (transactions[i] == nullptr) {
        break;
      }
      if (transactions[i]->abort_lock) {
        transactions[it++].swap(transactions[i]);
      }
    }
    total_abort.store(it);
  }

public:
  RandomType random;
  DatabaseType &db;
  std::atomic<uint32_t> epoch;
  std::vector<StorageType> storages;
  std::vector<std::unique_ptr<TransactionType>> transactions;
  std::atomic<uint32_t> total_abort;
};
} // namespace aria