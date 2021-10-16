//
// Created by Yi Lu on 8/29/18.
//

#pragma once

#include "common/Percentile.h"
#include "common/BufferedReader.h"
#include "common/LockfreeQueue.h"
#include "common/Message.h"
#include "common/Socket.h"
#include "core/ControlMessage.h"
#include "core/Worker.h"
#include <atomic>
#include <glog/logging.h>
#include <thread>
#include <vector>

namespace star {
class IncomingDispatcher {
public:
  IncomingDispatcher(std::size_t cid, std::size_t group_id,
                     std::size_t io_thread_num, std::vector<Socket> &sockets,
                     const std::vector<std::shared_ptr<Worker>> &workers,
                     LockfreeQueue<Message *> &coordinator_queue,
                     LockfreeQueue<Message *> &out_to_in_queue,
                     std::atomic<bool> &stopFlag, Context context)
      : coord_id(cid), group_id(group_id), io_thread_num(io_thread_num),
        network_size(0), workers(workers), coordinator_queue(coordinator_queue), out_to_in_queue(out_to_in_queue),
        stopFlag(stopFlag), context(context) {
    LOG(INFO) << "IncomingDispatcher " << group_id << " coord_id " << coord_id;
    for (auto i = 0u; i < sockets.size(); i++) {
      buffered_readers.emplace_back(sockets[i]);
    }
  }

  void start() {
    auto numCoordinators = buffered_readers.size();
    auto numWorkers = context.worker_num;

    // single node test mode
    // if (numCoordinators == 1) {
    //   return;
    // }

    LOG(INFO) << "Incoming Dispatcher started, numCoordinators = "
              << numCoordinators << ", numWorkers = " << numWorkers
              << ", group id = " << group_id << ", coordinator = " << coord_id;

    auto process_internal_message_tranfer = [&, this]() {
      while (out_to_in_queue.empty() == false) {
        auto message_get_start = std::chrono::steady_clock::now();
        std::unique_ptr<Message> message(out_to_in_queue.front());
        bool ok = out_to_in_queue.pop();
        CHECK(ok);
        auto workerId = message->get_worker_id();
        auto dest_node_id = message->get_dest_node_id();
        DCHECK(dest_node_id == coord_id);
        if (context.enable_hstore_master && workerId > context.worker_num) {
          //LOG(INFO) << "message coming at worker id " << workerId;
          DCHECK(coord_id == 0);
          DCHECK(workerId == context.worker_num + 1 || workerId == context.worker_num + 2);
          // release the unique ptr
          if (workerId == context.worker_num + 1) {
            workers[context.worker_num + 1]->push_master_message(message.release());
          } else {
            workers[context.worker_num + 1]->push_master_special_message(message.release());
          }
        } else {
          DCHECK(workerId % io_thread_num == group_id);
          workers[workerId]->push_message(message.release());
        }
        auto ltc = std::chrono::duration_cast<std::chrono::nanoseconds>(
                    std::chrono::steady_clock::now() - message_get_start)
                    .count();
        internal_message_recv_latency.add(ltc);
      };
    };
    while (!stopFlag.load()) {
      //LOG(INFO) << "Dispatcher coordinator = " << coord_id;

      process_internal_message_tranfer();
      for (auto i = 0u; i < numCoordinators; i++) {
        if (i == coord_id) {
          continue;
        }
        auto message_get_start = std::chrono::steady_clock::now();
        auto message = buffered_readers[i].next_message();

        if (message == nullptr) {
          //process_internal_message_tranfer();
          std::this_thread::yield();
          continue;
        }
        //LOG(INFO) << " message";
        network_size += message->get_message_length();

        // check coordinator message
        if (is_coordinator_message(message.get())) {

          //LOG(INFO) << "coord " << coord_id << " message";
          coordinator_queue.push(message.release());
          CHECK(group_id == 0);
          continue;
        }

        auto workerId = message->get_worker_id();
        if (context.enable_hstore_master && workerId > context.worker_num) {
          //LOG(INFO) << "message coming at worker id " << workerId;
          DCHECK(coord_id == 0);
          DCHECK(workerId == context.worker_num + 1 || workerId == context.worker_num + 2);
          // release the unique ptr
          if (context.enable_hstore_master && workerId == context.worker_num + 1) {
            workers[context.worker_num + 1]->push_master_message(message.release());
          } else {
            workers[context.worker_num + 1]->push_master_special_message(message.release());
          }
        } else {
          //LOG(INFO) << " message for workerId " << workerId;
          CHECK(workerId % io_thread_num == group_id);
          // release the unique ptr
          workers[workerId]->push_message(message.release());
        }
        
        auto ltc = std::chrono::duration_cast<std::chrono::microseconds>(
                    std::chrono::steady_clock::now() - message_get_start)
                    .count();
        socket_message_recv_latency.add(ltc);
        DCHECK(message == nullptr);
      }
    }

    LOG(INFO) << "Incoming Dispatcher exits, network size: " << network_size << ". socket_message_recv_latency(50th) " 
              << socket_message_recv_latency.nth(50) << " socket_message_recv_latency(75th) " << socket_message_recv_latency.nth(75)
              << " socket_message_recv_latency(95th) " << socket_message_recv_latency.nth(95)
              << " socket_message_recv_latency(99th) " << socket_message_recv_latency.nth(99)
              << " internal_message_recv_latency(50th) " << internal_message_recv_latency.nth(50) / 1000.0;
  }

  bool is_coordinator_message(Message *message) {
    return (*(message->begin())).get_message_type() ==
           static_cast<uint32_t>(ControlMessage::STATISTICS);
  }

  std::unique_ptr<Message> fetchMessage(Socket &socket) { return nullptr; }

private:
  const std::size_t coord_id;
  std::size_t group_id;
  std::size_t io_thread_num;
  std::size_t network_size;
  std::vector<BufferedReader> buffered_readers;
  std::vector<std::shared_ptr<Worker>> workers;
  LockfreeQueue<Message *> &coordinator_queue;
  LockfreeQueue<Message *> &out_to_in_queue;
  Percentile<uint64_t> socket_message_recv_latency;
  Percentile<uint64_t> internal_message_recv_latency;
  std::atomic<bool> &stopFlag;
  Context context;
};

class OutgoingDispatcher {
public:
  OutgoingDispatcher(std::size_t coord_id, std::size_t group_id,
                     std::size_t io_thread_num, std::vector<Socket> &sockets,
                     const std::vector<std::shared_ptr<Worker>> &workers,
                     LockfreeQueue<Message *> &coordinator_queue,
                     LockfreeQueue<Message *> &out_to_in_queue,
                     std::atomic<bool> &stopFlag, Context context)
      : coordinator_id(coord_id), group_id(group_id), io_thread_num(io_thread_num),
        network_size(0), sockets(sockets), workers(workers),
        coordinator_queue(coordinator_queue), out_to_in_queue(out_to_in_queue),
        stopFlag(stopFlag), context(context) {
          LOG(INFO) << "OutgoingDispatcher " << group_id << " coord_id " << coordinator_id;
  }

  void start() {

    auto numCoordinators = sockets.size();
    auto numWorkers = workers.size();
    // single node test mode
    // if (numCoordinators == 1) {
    //   return;
    // }

    LOG(INFO) << "Outgoing Dispatcher started, numCoordinators = "
              << numCoordinators << ", numWorkers = " << numWorkers
              << ", group id = " << group_id;
    Percentile<uint64_t> msg_disp_ltc;
    bool is_hstore = context.protocol == "HStore";
    while (!stopFlag.load()) {

      // check coordinator
      if (group_id == 0 && !coordinator_queue.empty()) {
        auto start = Time::now();
        std::unique_ptr<Message> message(coordinator_queue.front());
        bool ok = coordinator_queue.pop();
        CHECK(ok);
        sendMessage(message.get());
        auto spent = Time::now() - start;
        LOG(INFO) << "Handling coordinator message took " << spent / 1000;
      }

      //auto start = Time::now();
      for (auto i = group_id; i < numWorkers; i += io_thread_num) {
        dispatchMessage(workers[i]);
      }
      //auto spent = (Time::now() - start) / 1000;
      // if (spent > 100) {
      //   LOG(INFO) << "Dispatching messsaegs took " << spent;
      // }
      //msg_disp_ltc.add(spent);
      //std::this_thread::yield();
    }

    LOG(INFO) << "Outgoing Dispatcher exits, network size: " << network_size
              // << ". msg_send_latency(50th) " << message_send_latency.nth(50) 
              // << " msg_send_latency(75th) " << message_send_latency.nth(75)
              // << " msg_send_latency(95th) " << message_send_latency.nth(95)
              // << " msg_send_latency(99th) " << message_send_latency.nth(99)
              << " msg_gen_to_sent_latency(50th) " << gen_to_sent_latency.nth(50) 
              << " msg_gen_to_sent_latency(75th) " << gen_to_sent_latency.nth(75)
              << " msg_gen_to_sent_latency(95th) " << gen_to_sent_latency.nth(95)
              << " msg_gen_to_sent_latency(99th) " << gen_to_sent_latency.nth(99)
              << " msg_gen_to_queue_latency(50th) " << gen_to_queue_latency.nth(50) 
              << " msg_gen_to_queue_latency(75th) " << gen_to_queue_latency.nth(75)
              << " msg_gen_to_queue_latency(95th) " << gen_to_queue_latency.nth(95)
              << " msg_gen_to_queue_latency(99th) " << gen_to_queue_latency.nth(99)
              << " msg_disp_ltc(50th) " << msg_disp_ltc.nth(50) 
              << " msg_disp_ltc(75th) " << msg_disp_ltc.nth(75)
              << " msg_disp_ltc(95th) " << msg_disp_ltc.nth(95)
              << " msg_disp_ltc(99th) " << msg_disp_ltc.nth(99)
              << " msg_disp_ltc(100th) " << msg_disp_ltc.nth(100)
              << " network_size " << network_size
              << " network_msg_cnt " << network_msg_cnt
              << " internal_network_size " << internal_network_size
              << " internal_network_msg_cnt " << internal_network_msg_cnt;
  }

  void sendMessage(Message *message) {
    auto dest_node_id = message->get_dest_node_id();
    DCHECK(dest_node_id >= 0 && dest_node_id < sockets.size() &&
           dest_node_id != coordinator_id);
    DCHECK(message->get_message_length() == message->data.length());

    sockets[dest_node_id].write_n_bytes(message->get_raw_ptr(),
                                        message->get_message_length());

    network_size += message->get_message_length();
  }

  void dispatchMessage(const std::shared_ptr<Worker> &worker) {
    auto message_get_start = Time::now();
    uint64_t ltc;
    Message *raw_message = worker->pop_message();
    if (raw_message == nullptr) {
      return;
    }
    auto gen_time = raw_message->get_gen_time();
    auto put_to_out_queue_time = raw_message->get_put_to_out_queue_time();
    //ltc = (Time::now() - message_get_start) / 1000;
    //message_send_latency.add(ltc);
    // wrap the message with a unique pointer.
    std::unique_ptr<Message> message(raw_message);
    // send the message
    ltc = (Time::now() - gen_time) / 1000;
    gen_to_sent_latency.add(ltc);
    if (message->get_dest_node_id() != this->coordinator_id) {
      sendMessage(message.get());
      network_msg_cnt++;
    } else {
      // if (message->get_worker_id() >= context.worker_num) {
      //   LOG(INFO) << "message with worker id " << message->get_worker_id() 
      //             << " put to out_to_in_queue";
      // }
      DCHECK(message->get_message_length() == message->data.length());
      DCHECK(message->get_dest_node_id() == this->coordinator_id);
      out_to_in_queue.push(message.release());
      internal_network_size += message->get_message_length();
      internal_network_msg_cnt++;
    }
    
    
    ltc = (put_to_out_queue_time - gen_time) / 1000;
    gen_to_queue_latency.add(ltc);
  }

private:
  std::size_t coordinator_id;
  std::size_t group_id;
  std::size_t io_thread_num;
  std::size_t network_size;
  std::size_t internal_network_size = 0;
  std::size_t network_msg_cnt = 0;
  std::size_t internal_network_msg_cnt = 0;
  std::vector<Socket> &sockets;
  std::vector<std::shared_ptr<Worker>> workers;
  LockfreeQueue<Message *> &coordinator_queue;
  LockfreeQueue<Message *> &out_to_in_queue;
  Percentile<uint64_t> message_send_latency;
  Percentile<uint64_t> gen_to_sent_latency;
  Percentile<uint64_t> gen_to_queue_latency;
  std::atomic<bool> &stopFlag;
  Context context;
};

} // namespace star
