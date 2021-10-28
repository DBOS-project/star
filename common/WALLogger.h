//
// Created by Yi Lu on 3/21/19.
//

#pragma once

#include <glog/logging.h>
#include <chrono>
#include <thread>
#include <cstring>
#include <string>
#include <fcntl.h>
#include <stdio.h>
#include <mutex>
#include <atomic>

#include "common/Percentile.h"

#include "BufferedFileWriter.h"
#include "Time.h"
namespace star {
class WALLogger {
public:
  WALLogger(const std::string & filename, std::size_t emulated_persist_latency) : filename(filename), emulated_persist_latency(emulated_persist_latency) {}

  virtual ~WALLogger() {}

  virtual size_t write(const char *str, long size) = 0;
  virtual void sync(size_t lsn, std::function<void()> on_blocking = [](){}) = 0;
  virtual void close() = 0;

  virtual void print_sync_time()  {};

  const std::string filename;
  std::size_t emulated_persist_latency;
};


class GroupCommitLogger : public WALLogger {
public:

  GroupCommitLogger(const std::string & filename, std::size_t group_commit_txn_cnt, std::size_t group_commit_latency = 10, std::size_t emulated_persist_latency = 0) 
    : WALLogger(filename, emulated_persist_latency), writer(filename.c_str(), 
    emulated_persist_latency), write_lsn(0), sync_lsn(0), 
    group_commit_latency_us(group_commit_latency), 
    group_commit_txn_cnt(group_commit_txn_cnt), last_sync_time(Time::now()), waiting_syncs(0) {
  }

  ~GroupCommitLogger() override {}

  std::size_t write(const char *str, long size) override{
    std::lock_guard<std::mutex> g(mtx);
    auto start_lsn = write_lsn.load();
    auto end_lsn = start_lsn + size;
    writer.write(str, size);
    write_lsn += size;
    return end_lsn;
  }

  void do_sync() {
    std::lock_guard<std::mutex> g(mtx);
    auto waiting_sync_cnt = waiting_syncs.load();
    if (waiting_sync_cnt < group_commit_txn_cnt / 2 && (Time::now() - last_sync_time) / 1000 < group_commit_latency_us) {
        return;
    }
    
    auto flush_lsn = write_lsn.load();
    waiting_sync_cnt = waiting_syncs.load();

    if (sync_lsn < write_lsn) {
      auto t = Time::now();
      writer.flush();
      writer.sync();
      sync_time.add(Time::now() - t);
      //LOG(INFO) << "sync " << waiting_sync_cnt << " writes"; 
    }
    last_sync_time = Time::now();
    waiting_syncs -= waiting_sync_cnt;
    sync_lsn.store(flush_lsn);
  }

  void sync(std::size_t lsn, std::function<void()> on_blocking = [](){}) override {
    waiting_syncs.fetch_add(1);
    while (sync_lsn.load() < lsn) {
      on_blocking();
      std::this_thread::sleep_for(std::chrono::microseconds(10));
      if (waiting_syncs.load() >= group_commit_txn_cnt / 2 || (Time::now() - last_sync_time) / 1000 >= group_commit_latency_us) {
        do_sync();
      }
    }
  }

  void close() override {
    writer.close();
  }

  void print_sync_time() override {
    LOG(INFO) << "Disk Sync time "
              << this->sync_time.nth(50) / 1000 << " us (50th), "
              << this->sync_time.nth(75) / 1000 << " us (75th), "
              << this->sync_time.nth(90) / 1000 << " us (90th), "
              << this->sync_time.nth(95) / 1000 << " us (95th). ";
  }
private:
  std::mutex mtx;
  BufferedFileWriter writer;
  std::atomic<uint64_t> write_lsn;
  std::atomic<uint64_t> sync_lsn;
  std::size_t group_commit_latency_us;
  std::size_t group_commit_txn_cnt;
  std::atomic<std::size_t> last_sync_time;
  std::atomic<uint64_t> waiting_syncs;
  Percentile<uint64_t> sync_time;
  
};


class SimpleWALLogger : public WALLogger {
public:

  SimpleWALLogger(const std::string & filename, std::size_t emulated_persist_latency = 0) 
    : WALLogger(filename, emulated_persist_latency), writer(filename.c_str(), emulated_persist_latency) {
  }

  ~SimpleWALLogger() override {}
  std::size_t write(const char *str, long size) override{
    std::lock_guard<std::mutex> g(mtx);
    writer.write(str, size);
    return 0;
  }

  void sync(std::size_t lsn, std::function<void()> on_blocking = [](){}) override {
    std::lock_guard<std::mutex> g(mtx);
    writer.sync();
  }

  void close() override {
    std::lock_guard<std::mutex> g(mtx);
    writer.close();
  }

private:
  BufferedFileWriter writer;
  std::mutex mtx;
};


// class ScalableWALLogger : public WALLogger {
// public:

//   ScalableWALLogger(const std::string & filename, std::size_t emulated_persist_latency = 0) 
//     : WALLogger(filename, emulated_persist_latency), write_lsn(0), alloc_lsn(0), emulated_persist_latency(emulated_persist_latency){
//     fd = open(filename.c_str(), O_WRONLY | O_CREAT | O_TRUNC,
//               S_IRUSR | S_IWUSR | S_IRGRP | S_IWGRP | S_IROTH | S_IWOTH);
//   }

//   ~ScalableWALLogger() override {}

//   std::size_t roundUp(std::size_t numToRound, std::size_t multiple) 
//   {
//       DCHECK(multiple);
//       int isPositive = 1;
//       return ((numToRound + isPositive * (multiple - 1)) / multiple) * multiple;
//   }

//   std::size_t write(const char *str, long size) override{
//     auto write_seq = alloc_lsn.load();
//     auto start_lsn = alloc_lsn.fetch_add(size);
  
//     while (start_lsn - flush_lsn > BUFFER_SIZE) {
//       // Wait for previous buffer to be flush down to file system (page cache).
//       std::this_thread::sleep_for(std::chrono::microseconds(1));
//     }
  
//     auto end_lsn = start_lsn + size;
//     auto start_buffer_offset = write_seq % BUFFER_SIZE;
//     auto buffer_left_size = BUFFER_SIZE - start_buffer_offset;

//     if (buffer_left_size >= size) {
//       memcpy(buffer + start_buffer_offset, str, size);
//       write_lsn += size;
//       buffer_left_size -= size;
//       size = 0;
//     } else {
//       memcpy(buffer + start_buffer_offset, str, buffer_left_size);
//       write_lsn += buffer_left_size;
//       size -= buffer_left_size;
//       buffer_left_size = 0;
//       str += buffer_left_size;
//     }
  
//     if (size || buffer_left_size == 0) { // torn write, flush data
//       auto block_up_seq = roundUp(write_seq, BUFFER_SIZE);
//       if (write_seq % BUFFER_SIZE == 0) {
//         block_up_seq = write_seq + BUFFER_SIZE;
//       }

//       while (write_lsn.load() == block_up_seq) { // Wait for the holes in the current block to be filled
//         std::this_thread::sleep_for(std::chrono::microseconds(1));
//       }
//       auto flush_lsn_save = flush_lsn.load();
//       {
//         std::lock_guard<std::mutex> g(mtx);
//         if (flush_lsn_save == flush_lsn) {// Only allow one thread to acquire the right to flush down the buffer
//           int err = ::write(fd, buffer, BUFFER_SIZE);
//           CHECK(err >= 0);
//           flush_lsn.fetch_add(BUFFER_SIZE);
//           DCHECK(block_up_seq == flush_lsn);
//         }
//       }
      
//       if (size) { // Write the left part to the new buffer
//         memcpy(buffer + 0, str, size);
//         write_lsn += size;
//       }
//     }
  
//     return end_lsn;
//   }

//   void sync_file() {
//     DCHECK(fd >= 0);
//     int err = 0;
//     if (emulated_persist_latency)
//       std::this_thread::sleep_for(std::chrono::microseconds(emulated_persist_latency));
//     //err = fdatasync(fd);
//     CHECK(err == 0);
//   }

//   void sync(std::size_t lsn) override {
//     DCHECK(fd >= 0);
//     int err = 0;
//     while (sync_lsn < lsn) {

//     }
//     sync_lsn = flush_lsn.load();
//     if (emulated_persist_latency)
//       std::this_thread::sleep_for(std::chrono::microseconds(emulated_persist_latency));
//     //err = fdatasync(fd);
//     CHECK(err == 0);
//   }

//   void close() override {
//     ::close(fd);
//   }

// private:
//   int fd;
//   std::mutex mtx;

// public:
//   static constexpr uint32_t BUFFER_SIZE = 1024 * 1024 * 4; // 4MB

// private:
//   std::atomic<uint64_t> alloc_lsn;
//   std::atomic<uint64_t> write_lsn;
//   std::atomic<uint64_t> flush_lsn;
//   std::atomic<uint64_t> sync_lsn;
//   char buffer[BUFFER_SIZE];
//   std::size_t emulated_persist_latency;
// };

}