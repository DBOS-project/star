//
// Created by Xinjing on 9/12/21.
//

#pragma once

#include "common/Encoder.h"
#include "common/Message.h"
#include "common/MessagePiece.h"
#include "core/ControlMessage.h"
#include "core/Table.h"

#include "protocol/H-Store/HStoreHelper.h"
#include "protocol/TwoPL/TwoPLRWKey.h"
#include "protocol/H-Store/HStoreTransaction.h"

namespace star {

enum class HStoreMessage {
  READ_LOCK_REQUEST = static_cast<int>(ControlMessage::NFIELDS),
  READ_LOCK_RESPONSE,
  WRITE_LOCK_REQUEST,
  WRITE_LOCK_RESPONSE,
  ABORT_REQUEST,
  WRITE_REQUEST,
  WRITE_RESPONSE,
  REPLICATION_REQUEST,
  REPLICATION_RESPONSE,
  RELEASE_READ_LOCK_REQUEST,
  RELEASE_READ_LOCK_RESPONSE,
  RELEASE_WRITE_LOCK_REQUEST,
  RELEASE_WRITE_LOCK_RESPONSE,
  MASTER_LOCK_PARTITION_REQUEST,
  MASTER_LOCK_PARTITION_RESPONSE,
  MASTER_UNLOCK_PARTITION_REQUEST,
  MASTER_UNLOCK_PARTITION_RESPONSE,
  COMMAND_REPLICATION_REQUEST,
  COMMAND_REPLICATION_RESPONSE,
  COMMAND_REPLICATION_SP_REQUEST,
  COMMAND_REPLICATION_SP_RESPONSE,
  ACQUIRE_PARTITION_LOCK_AND_READ_REQUEST,
  ACQUIRE_PARTITION_LOCK_AND_READ_RESPONSE,
  ACQUIRE_PARTITION_LOCK_REQUEST,
  ACQUIRE_PARTITION_LOCK_RESPONSE,
  WRITE_BACK_REQUEST,
  WRITE_BACK_RESPONSE,
  RELEASE_PARTITION_LOCK_REQUEST,
  RELEASE_PARTITION_LOCK_RESPONSE,
  PREPARE_REQUEST,
  PREPARE_RESPONSE,
  PREPARE_REDO_REQUEST,
  PREPARE_REDO_RESPONSE,
  NFIELDS
};

class HStoreMessageFactory {

public:

  static std::size_t new_master_lock_partition_message(Message &message, ITable & table, uint32_t this_worker_id, int num_parts, std::function<int32_t(int)> get_part_func) {

    /*
     * The structure of a partition lock request: (remote_worker_id, # parts, part1, part2...)
     */

    auto message_size =
        MessagePiece::get_header_size() + sizeof(uint32_t) + + sizeof(uint32_t) + sizeof(int32_t) * num_parts;
    auto message_piece_header = MessagePiece::construct_message_piece_header(
        static_cast<uint32_t>(HStoreMessage::MASTER_LOCK_PARTITION_REQUEST), message_size,
        table.tableID(), table.partitionID());

    Encoder encoder(message.data);
    encoder << message_piece_header;
    encoder << this_worker_id;
    encoder << (uint32_t)num_parts;
    // std::string dbg_str;
    // for (size_t i = 0; i < parts.size(); ++i) {
    //   dbg_str += std::to_string(parts[i]) + ",";
    // }
    // LOG(INFO) << "new_master_lock_partition_message cluster_worker " << this_worker_id
    //           << " need locks on partitions: " << dbg_str;
    for (int i = 0; i < num_parts; ++i) {
      encoder << get_part_func(i);
    }
    message.flush();
    message.set_gen_time(Time::now());
    return message_size;
  }

  static std::size_t new_master_unlock_partition_message(Message &message, ITable & table, uint32_t this_worker_id) {

    /*
     * The structure of a partition lock request: (remote_worker_id)
     */

    auto message_size =
        MessagePiece::get_header_size() + sizeof(uint32_t);
    auto message_piece_header = MessagePiece::construct_message_piece_header(
        static_cast<uint32_t>(HStoreMessage::MASTER_UNLOCK_PARTITION_REQUEST), message_size,
        table.tableID(), table.partitionID());

    Encoder encoder(message.data);
    encoder << message_piece_header;
    encoder << this_worker_id;
    message.flush();
    message.set_gen_time(Time::now());
    return message_size;
  }

  static std::size_t new_release_partition_lock_message(Message &message, ITable & table, uint32_t this_worker_id, bool sync, std::size_t ith_replica) {

    /*
     * The structure of a partition lock request: (remote_worker_id)
     */

    auto message_size =
        MessagePiece::get_header_size() + sizeof(uint32_t) + sizeof(bool) + sizeof(std::size_t);
    auto message_piece_header = MessagePiece::construct_message_piece_header(
        static_cast<uint32_t>(HStoreMessage::RELEASE_PARTITION_LOCK_REQUEST), message_size,
        table.tableID(), table.partitionID());

    Encoder encoder(message.data);
    encoder << message_piece_header;
    encoder << this_worker_id;
    encoder << sync;
    encoder << ith_replica;
    message.set_is_replica(ith_replica > 0);
    message.flush();
    message.set_gen_time(Time::now());
    return message_size;
  }

  static std::size_t new_command_replication_response_message(Message & message) {
    auto message_size = MessagePiece::get_header_size();

    auto message_piece_header = MessagePiece::construct_message_piece_header(
        static_cast<uint32_t>(HStoreMessage::COMMAND_REPLICATION_RESPONSE), message_size,
        0, 0);

    star::Encoder encoder(message.data);
    encoder << message_piece_header;
  
    message.flush();
    message.set_gen_time(Time::now());
    return message_size;
  }

  static std::size_t new_command_replication_sp_response_message(Message & message) {
    auto message_size = MessagePiece::get_header_size();

    auto message_piece_header = MessagePiece::construct_message_piece_header(
        static_cast<uint32_t>(HStoreMessage::COMMAND_REPLICATION_SP_RESPONSE), message_size,
        0, 0);

    star::Encoder encoder(message.data);
    encoder << message_piece_header;
  
    message.flush();
    message.set_gen_time(Time::now());
    return message_size;
  }


  static std::size_t new_command_replication(Message & message, std::size_t ith_replica, const std::string & data, int this_cluster_worker_id) {
    auto message_size =
      MessagePiece::get_header_size() + sizeof(std::size_t) + data.size() + sizeof(this_cluster_worker_id);
    auto message_piece_header = MessagePiece::construct_message_piece_header(
      static_cast<uint32_t>(HStoreMessage::COMMAND_REPLICATION_REQUEST), message_size,
      0, 0);
    Encoder encoder(message.data);
    encoder << message_piece_header << ith_replica << this_cluster_worker_id;
    encoder.write_n_bytes(data.c_str(), data.size());
    message.set_is_replica(ith_replica > 0);
    message.flush();
    message.set_gen_time(Time::now());
    return message_size;
  }

  static std::size_t new_command_replication_sp(Message & message, std::size_t ith_replica, const std::vector< std::string> & commands_data, int this_cluster_worker_id) {
    auto message_size =
      MessagePiece::get_header_size() + sizeof(std::size_t) + sizeof(commands_data.size()) + sizeof(this_cluster_worker_id);
    for (size_t i = 0; i < commands_data.size(); ++i) {
      message_size += sizeof(commands_data[i].size());
      message_size += commands_data[i].size();
    }
    auto message_piece_header = MessagePiece::construct_message_piece_header(
      static_cast<uint32_t>(HStoreMessage::COMMAND_REPLICATION_SP_REQUEST), message_size,
      0, 0);
    Encoder encoder(message.data);
    encoder << message_piece_header << ith_replica << this_cluster_worker_id;
    encoder << commands_data.size();
    for (size_t i = 0; i < commands_data.size(); ++i) {
      encoder << commands_data[i].size();
      encoder.write_n_bytes(commands_data[i].c_str(), commands_data[i].size());
    }
    message.set_is_replica(ith_replica > 0);
    message.flush();
    message.set_gen_time(Time::now());
    return message_size;
  }

  template<class DatabaseType>
  static std::size_t new_prepare_and_redo_message(Message &message, const std::vector<TwoPLRWKey> & redoWriteSet, 
                                                  DatabaseType & db, bool persist_log, std::size_t ith_replica) {
    auto message_size = MessagePiece::get_header_size();
    auto message_piece_header = MessagePiece::construct_message_piece_header(
        static_cast<uint32_t>(HStoreMessage::PREPARE_REDO_REQUEST), message_size,
        0, 0);
    
    Encoder encoder(message.data);
    size_t start_off = encoder.size();
    encoder << message_piece_header << persist_log << ith_replica;
    encoder << redoWriteSet.size();
    for (size_t i = 0; i < redoWriteSet.size(); ++i) {
      auto writeKey = redoWriteSet[i];
      auto tableId = writeKey.get_table_id();
      auto partitionId = writeKey.get_partition_id();
      auto table = db.find_table(tableId, partitionId);
      auto key_size = table->key_size();
      auto value_size = table->value_size();
      auto key = writeKey.get_key();
      auto value = writeKey.get_value();
      encoder << tableId << partitionId << key_size;
      encoder.write_n_bytes(key, key_size);
      encoder << value_size;
      encoder.write_n_bytes(value, value_size);
    }

    message_size = encoder.size() - start_off;
    message_piece_header = MessagePiece::construct_message_piece_header(
        static_cast<uint32_t>(HStoreMessage::PREPARE_REDO_REQUEST),
        message_size, 0, 0);
    encoder.replace_bytes_range(start_off, (void *)&message_piece_header, sizeof(message_piece_header));
    message.set_is_replica(ith_replica > 0);
    message.flush();
    return message_size;
  }

  static std::size_t new_prepare_message(Message &message, ITable & table, uint32_t this_worker_id) {

    /*
     * The structure of a partition lock request: (remote_worker_id)
     */

    auto message_size =
        MessagePiece::get_header_size() + sizeof(uint32_t);
    auto message_piece_header = MessagePiece::construct_message_piece_header(
        static_cast<uint32_t>(HStoreMessage::PREPARE_REQUEST), message_size,
        table.tableID(), table.partitionID());

    Encoder encoder(message.data);
    encoder << message_piece_header;
    encoder << this_worker_id;
    message.flush();
    message.set_gen_time(Time::now());
    return message_size;
  }

  static std::size_t new_acquire_partition_lock_and_read_message(Message &message, ITable &table,
                                           const void *key,
                                           uint32_t key_offset,
                                           uint32_t this_worker_id,
                                           std::size_t ith_replica) {

    /*
     * The structure of a partition lock request: (primary key, key offset, remote_worker_id)
     */

    auto key_size = table.key_size();

    auto message_size =
        MessagePiece::get_header_size() + key_size + sizeof(key_offset) + sizeof(uint32_t) + sizeof(std::size_t);
    auto message_piece_header = MessagePiece::construct_message_piece_header(
        static_cast<uint32_t>(HStoreMessage::ACQUIRE_PARTITION_LOCK_AND_READ_REQUEST), message_size,
        table.tableID(), table.partitionID());

    // LOG(INFO) << "this_cluster_worker_id "<< this_worker_id << " new_acquire_partition_lock_and_read_message message on partition " 
    //           << table.partitionID() << " of " << ith_replica << " replica";

    Encoder encoder(message.data);
    encoder << message_piece_header;
    encoder.write_n_bytes(key, key_size);
    encoder << key_offset;
    encoder << this_worker_id;
    encoder << ith_replica;
    message.set_is_replica(ith_replica > 0);
    message.flush();
    message.set_gen_time(Time::now());
    return message_size;
  }

  static std::size_t new_acquire_partition_lock_message(Message &message, std::size_t partition_id,
                                           uint32_t source_cluster_worker_id,
                                           uint32_t cluster_worker_id,
                                           uint32_t owner_cluster_worker,
                                           std::size_t ith_replica) {

    /*
     * The structure of a partition lock request: (remote_worker_id, ith_replica)
     */

    auto message_size =
        MessagePiece::get_header_size() + sizeof(uint32_t) * 2 + sizeof(std::size_t);
    auto message_piece_header = MessagePiece::construct_message_piece_header(
        static_cast<uint32_t>(HStoreMessage::ACQUIRE_PARTITION_LOCK_REQUEST), message_size,
        0, partition_id);

    // LOG(INFO) << "source_cluster_worker_id " << source_cluster_worker_id  << " cluster_worker_id "<< cluster_worker_id << " new_acquire_partition_lock message on partition " 
    //           << partition_id << " of " << ith_replica << " replica" << " managed by cluster worker " << owner_cluster_worker;
    Encoder encoder(message.data);
    encoder << message_piece_header;
    encoder << source_cluster_worker_id << cluster_worker_id;
    encoder << ith_replica;
    message.set_is_replica(ith_replica > 0);
    message.flush();
    message.set_gen_time(Time::now());
    return message_size;
  }

  static std::size_t new_write_back_message(Message &message, ITable &table,
                                       const void *key, const void *value, uint32_t this_worker_id, 
                                       uint64_t commit_tid, std::size_t ith_replica,
                                       bool persist_commit_record = false) {

    /*
     * The structure of a write request: (request_remote_worker, nowrite, primary key, field value)
     */

    auto key_size = table.key_size();
    auto field_size = table.field_size();

    auto message_size = MessagePiece::get_header_size() + sizeof(uint32_t) + sizeof(std::size_t) + key_size + field_size  + sizeof(commit_tid) + sizeof(persist_commit_record);
    auto message_piece_header = MessagePiece::construct_message_piece_header(
        static_cast<uint32_t>(HStoreMessage::WRITE_BACK_REQUEST), message_size,
        table.tableID(), table.partitionID());

    Encoder encoder(message.data);
    encoder << message_piece_header << commit_tid << persist_commit_record;
    encoder << this_worker_id;
    encoder << ith_replica;
    encoder.write_n_bytes(key, key_size);
    table.serialize_value(encoder, value);
    message.set_is_replica(ith_replica > 0);
    message.flush();
    message.set_gen_time(Time::now());
    return message_size;
  }

  static std::size_t new_read_lock_message(Message &message, ITable &table,
                                           const void *key,
                                           uint32_t key_offset) {

    /*
     * The structure of a read lock request: (primary key, key offset)
     */

    auto key_size = table.key_size();

    auto message_size =
        MessagePiece::get_header_size() + key_size + sizeof(key_offset);
    auto message_piece_header = MessagePiece::construct_message_piece_header(
        static_cast<uint32_t>(HStoreMessage::READ_LOCK_REQUEST), message_size,
        table.tableID(), table.partitionID());

    Encoder encoder(message.data);
    encoder << message_piece_header;
    encoder.write_n_bytes(key, key_size);
    encoder << key_offset;
    message.flush();
    message.set_gen_time(Time::now());
    return message_size;
  }

  static std::size_t new_write_lock_message(Message &message, ITable &table,
                                            const void *key,
                                            uint32_t key_offset) {

    /*
     * The structure of a write lock request: (primary key, key offset)
     */

    auto key_size = table.key_size();

    auto message_size =
        MessagePiece::get_header_size() + key_size + sizeof(key_offset);
    auto message_piece_header = MessagePiece::construct_message_piece_header(
        static_cast<uint32_t>(HStoreMessage::WRITE_LOCK_REQUEST), message_size,
        table.tableID(), table.partitionID());

    Encoder encoder(message.data);
    encoder << message_piece_header;
    encoder.write_n_bytes(key, key_size);
    encoder << key_offset;
    message.flush();
    message.set_gen_time(Time::now());
    return message_size;
  }

  static std::size_t new_abort_message(Message &message, ITable &table,
                                       const void *key, bool write_lock) {
    /*
     * The structure of an abort request: (primary key, wrtie lock)
     */

    auto key_size = table.key_size();

    auto message_size =
        MessagePiece::get_header_size() + key_size + sizeof(bool);
    auto message_piece_header = MessagePiece::construct_message_piece_header(
        static_cast<uint32_t>(HStoreMessage::ABORT_REQUEST), message_size,
        table.tableID(), table.partitionID());

    Encoder encoder(message.data);
    encoder << message_piece_header;
    encoder.write_n_bytes(key, key_size);
    encoder << write_lock;
    message.flush();
    message.set_gen_time(Time::now());
    return message_size;
  }

  static std::size_t new_write_message(Message &message, ITable &table,
                                       const void *key, const void *value) {

    /*
     * The structure of a write request: (primary key, field value)
     */

    auto key_size = table.key_size();
    auto field_size = table.field_size();

    auto message_size = MessagePiece::get_header_size() + key_size + field_size;
    auto message_piece_header = MessagePiece::construct_message_piece_header(
        static_cast<uint32_t>(HStoreMessage::WRITE_REQUEST), message_size,
        table.tableID(), table.partitionID());

    Encoder encoder(message.data);
    encoder << message_piece_header;
    encoder.write_n_bytes(key, key_size);
    table.serialize_value(encoder, value);
    message.flush();
    message.set_gen_time(Time::now());
    return message_size;
  }

  static std::size_t new_replication_message(Message &message, ITable &table,
                                             const void *key, const void *value,
                                             uint64_t commit_tid) {

    /*
     * The structure of a replication request: (primary key, field value,
     * commit_tid)
     */

    auto key_size = table.key_size();
    auto field_size = table.field_size();

    auto message_size = MessagePiece::get_header_size() + key_size +
                        field_size + sizeof(commit_tid);
    auto message_piece_header = MessagePiece::construct_message_piece_header(
        static_cast<uint32_t>(HStoreMessage::REPLICATION_REQUEST), message_size,
        table.tableID(), table.partitionID());

    Encoder encoder(message.data);
    encoder << message_piece_header;
    encoder.write_n_bytes(key, key_size);
    table.serialize_value(encoder, value);
    encoder << commit_tid;
    message.flush();
    message.set_gen_time(Time::now());
    return message_size;
  }

  static std::size_t new_release_read_lock_message(Message &message,
                                                   ITable &table,
                                                   const void *key) {
    /*
     * The structure of a release read lock request: (primary key)
     */

    auto key_size = table.key_size();

    auto message_size = MessagePiece::get_header_size() + key_size;
    auto message_piece_header = MessagePiece::construct_message_piece_header(
        static_cast<uint32_t>(HStoreMessage::RELEASE_READ_LOCK_REQUEST),
        message_size, table.tableID(), table.partitionID());

    Encoder encoder(message.data);
    encoder << message_piece_header;
    encoder.write_n_bytes(key, key_size);
    message.flush();
    message.set_gen_time(Time::now());
    return message_size;
  }

  static std::size_t new_release_write_lock_message(Message &message,
                                                    ITable &table,
                                                    const void *key,
                                                    uint64_t commit_tid) {

    /*
     * The structure of a release write lock request: (primary key, commit tid)
     */

    auto key_size = table.key_size();

    auto message_size =
        MessagePiece::get_header_size() + key_size + sizeof(commit_tid);
    auto message_piece_header = MessagePiece::construct_message_piece_header(
        static_cast<uint32_t>(HStoreMessage::RELEASE_WRITE_LOCK_REQUEST),
        message_size, table.tableID(), table.partitionID());

    Encoder encoder(message.data);
    encoder << message_piece_header;
    encoder.write_n_bytes(key, key_size);
    encoder << commit_tid;
    message.flush();
    message.set_gen_time(Time::now());
    return message_size;
  }
};

class HStoreMessageHandler {
  using Transaction = HStoreTransaction;

public:
  static void read_lock_request_handler(MessagePiece inputPiece,
                                        Message &responseMessage, ITable &table,
                                        Transaction *txn) {
    DCHECK(inputPiece.get_message_type() ==
           static_cast<uint32_t>(HStoreMessage::READ_LOCK_REQUEST));
    auto table_id = inputPiece.get_table_id();
    auto partition_id = inputPiece.get_partition_id();
    DCHECK(table_id == table.tableID());
    DCHECK(partition_id == table.partitionID());
    auto key_size = table.key_size();
    auto value_size = table.value_size();

    /*
     * The structure of a read lock request: (primary key, key offset)
     * The structure of a read lock response: (success?, key offset, value?,
     * tid?)
     */

    auto stringPiece = inputPiece.toStringPiece();
    uint32_t key_offset;

    DCHECK(inputPiece.get_message_length() ==
           MessagePiece::get_header_size() + key_size + sizeof(key_offset));

    const void *key = stringPiece.data();
    auto row = table.search(key);
    std::atomic<uint64_t> &tid = *std::get<0>(row);

    stringPiece.remove_prefix(key_size);
    star::Decoder dec(stringPiece);
    dec >> key_offset;

    DCHECK(dec.size() == 0);

    bool success;
    uint64_t latest_tid = TwoPLHelper::read_lock(tid, success);

    // prepare response message header
    auto message_size =
        MessagePiece::get_header_size() + sizeof(bool) + sizeof(key_offset);

    if (success) {
      message_size += value_size + sizeof(uint64_t);
    }

    auto message_piece_header = MessagePiece::construct_message_piece_header(
        static_cast<uint32_t>(HStoreMessage::READ_LOCK_RESPONSE), message_size,
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
      TwoPLHelper::read(row, dest, value_size);
      encoder << latest_tid;
    }

    responseMessage.flush();
  }

  static void read_lock_response_handler(MessagePiece inputPiece,
                                         Message &responseMessage,
                                         ITable &table, Transaction *txn) {
    DCHECK(inputPiece.get_message_type() ==
           static_cast<uint32_t>(HStoreMessage::READ_LOCK_RESPONSE));
    auto table_id = inputPiece.get_table_id();
    auto partition_id = inputPiece.get_partition_id();
    DCHECK(table_id == table.tableID());
    DCHECK(partition_id == table.partitionID());
    auto key_size = table.key_size();
    auto value_size = table.value_size();

    /*
     * The structure of a read lock response: (success?, key offset, value?,
     * tid?)
     */

    bool success;
    uint32_t key_offset;

    StringPiece stringPiece = inputPiece.toStringPiece();
    Decoder dec(stringPiece);
    dec >> success >> key_offset;

    if (success) {
      DCHECK(inputPiece.get_message_length() ==
             MessagePiece::get_header_size() + sizeof(success) +
                 sizeof(key_offset) + value_size + sizeof(uint64_t));

      TwoPLRWKey &readKey = txn->readSet[key_offset];
      dec.read_n_bytes(readKey.get_value(), value_size);
      uint64_t tid;
      dec >> tid;
      readKey.set_read_lock_bit();
      readKey.set_tid(tid);
    } else {
      DCHECK(inputPiece.get_message_length() ==
             MessagePiece::get_header_size() + sizeof(success) +
                 sizeof(key_offset));

      txn->abort_lock = true;
    }

    txn->pendingResponses--;
    txn->network_size += inputPiece.get_message_length();
  }

  static void write_lock_request_handler(MessagePiece inputPiece,
                                         Message &responseMessage,
                                         ITable &table, Transaction *txn) {

    DCHECK(inputPiece.get_message_type() ==
           static_cast<uint32_t>(HStoreMessage::WRITE_LOCK_REQUEST));
    auto table_id = inputPiece.get_table_id();
    auto partition_id = inputPiece.get_partition_id();
    DCHECK(table_id == table.tableID());
    DCHECK(partition_id == table.partitionID());
    auto key_size = table.key_size();
    auto value_size = table.value_size();

    /*
     * The structure of a write lock request: (primary key, key offset)
     * The structure of a write lock response: (success?, key offset, value?,
     * tid?)
     */

    auto stringPiece = inputPiece.toStringPiece();
    uint32_t key_offset;

    DCHECK(inputPiece.get_message_length() ==
           MessagePiece::get_header_size() + key_size + sizeof(key_offset));

    const void *key = stringPiece.data();
    auto row = table.search(key);
    std::atomic<uint64_t> &tid = *std::get<0>(row);

    stringPiece.remove_prefix(key_size);
    star::Decoder dec(stringPiece);
    dec >> key_offset;

    DCHECK(dec.size() == 0);

    bool success;
    uint64_t latest_tid = TwoPLHelper::write_lock(tid, success);

    // prepare response message header
    auto message_size =
        MessagePiece::get_header_size() + sizeof(bool) + sizeof(key_offset);

    if (success) {
      message_size += value_size + sizeof(uint64_t);
    }

    auto message_piece_header = MessagePiece::construct_message_piece_header(
        static_cast<uint32_t>(HStoreMessage::WRITE_LOCK_RESPONSE), message_size,
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
      TwoPLHelper::read(row, dest, value_size);
      encoder << latest_tid;
    }

    responseMessage.flush();
  }

  static void write_lock_response_handler(MessagePiece inputPiece,
                                          Message &responseMessage,
                                          ITable &table, Transaction *txn) {
    DCHECK(inputPiece.get_message_type() ==
           static_cast<uint32_t>(HStoreMessage::WRITE_LOCK_RESPONSE));
    auto table_id = inputPiece.get_table_id();
    auto partition_id = inputPiece.get_partition_id();
    DCHECK(table_id == table.tableID());
    DCHECK(partition_id == table.partitionID());
    auto key_size = table.key_size();
    auto value_size = table.value_size();

    /*
     * The structure of a read lock response: (success?, key offset, value?,
     * tid?)
     */

    bool success;
    uint32_t key_offset;

    StringPiece stringPiece = inputPiece.toStringPiece();
    Decoder dec(stringPiece);
    dec >> success >> key_offset;

    if (success) {
      DCHECK(inputPiece.get_message_length() ==
             MessagePiece::get_header_size() + sizeof(success) +
                 sizeof(key_offset) + value_size + sizeof(uint64_t));

      TwoPLRWKey &readKey = txn->readSet[key_offset];
      dec.read_n_bytes(readKey.get_value(), value_size);
      uint64_t tid;
      dec >> tid;
      readKey.set_write_lock_bit();
      readKey.set_tid(tid);
    } else {
      DCHECK(inputPiece.get_message_length() ==
             MessagePiece::get_header_size() + sizeof(success) +
                 sizeof(key_offset));

      txn->abort_lock = true;
    }

    txn->pendingResponses--;
    txn->network_size += inputPiece.get_message_length();
  }

  static void abort_request_handler(MessagePiece inputPiece,
                                    Message &responseMessage, ITable &table,
                                    Transaction *txn) {

    DCHECK(inputPiece.get_message_type() ==
           static_cast<uint32_t>(HStoreMessage::ABORT_REQUEST));
    auto table_id = inputPiece.get_table_id();
    auto partition_id = inputPiece.get_partition_id();
    DCHECK(table_id == table.tableID());
    DCHECK(partition_id == table.partitionID());
    auto key_size = table.key_size();

    /*
     * The structure of an abort request: (primary key, write_lock)
     * The structure of an abort response: null
     */

    DCHECK(inputPiece.get_message_length() ==
           MessagePiece::get_header_size() + key_size + sizeof(bool));

    auto stringPiece = inputPiece.toStringPiece();

    const void *key = stringPiece.data();
    stringPiece.remove_prefix(key_size);

    bool write_lock;
    Decoder dec(stringPiece);
    dec >> write_lock;

    std::atomic<uint64_t> &tid = table.search_metadata(key);

    if (write_lock) {
      TwoPLHelper::write_lock_release(tid);
    } else {
      TwoPLHelper::read_lock_release(tid);
    }
  }

  static void write_request_handler(MessagePiece inputPiece,
                                    Message &responseMessage, ITable &table,
                                    Transaction *txn) {

    DCHECK(inputPiece.get_message_type() ==
           static_cast<uint32_t>(HStoreMessage::WRITE_REQUEST));
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
           MessagePiece::get_header_size() + key_size + field_size);

    auto stringPiece = inputPiece.toStringPiece();

    const void *key = stringPiece.data();
    stringPiece.remove_prefix(key_size);

    table.deserialize_value(key, stringPiece);

    // prepare response message header
    auto message_size = MessagePiece::get_header_size();
    auto message_piece_header = MessagePiece::construct_message_piece_header(
        static_cast<uint32_t>(HStoreMessage::WRITE_RESPONSE), message_size,
        table_id, partition_id);

    star::Encoder encoder(responseMessage.data);
    encoder << message_piece_header;
    responseMessage.flush();
  }

  static void write_response_handler(MessagePiece inputPiece,
                                     Message &responseMessage, ITable &table,
                                     Transaction *txn) {

    DCHECK(inputPiece.get_message_type() ==
           static_cast<uint32_t>(HStoreMessage::WRITE_RESPONSE));
    auto table_id = inputPiece.get_table_id();
    auto partition_id = inputPiece.get_partition_id();
    DCHECK(table_id == table.tableID());
    DCHECK(partition_id == table.partitionID());
    auto key_size = table.key_size();

    /*
     * The structure of a write response: ()
     */

    txn->pendingResponses--;
    txn->network_size += inputPiece.get_message_length();
  }

  static void replication_request_handler(MessagePiece inputPiece,
                                          Message &responseMessage,
                                          ITable &table, Transaction *txn) {
    DCHECK(inputPiece.get_message_type() ==
           static_cast<uint32_t>(HStoreMessage::REPLICATION_REQUEST));
    auto table_id = inputPiece.get_table_id();
    auto partition_id = inputPiece.get_partition_id();
    DCHECK(table_id == table.tableID());
    DCHECK(partition_id == table.partitionID());
    auto key_size = table.key_size();
    auto field_size = table.field_size();

    /*
     * The structure of a replication request: (primary key, field value, commit
     * tid) The structure of a replication response: ()
     */

    auto stringPiece = inputPiece.toStringPiece();

    const void *key = stringPiece.data();
    stringPiece.remove_prefix(key_size);
    auto valueStringPiece = stringPiece;
    stringPiece.remove_prefix(field_size);

    uint64_t commit_tid;
    Decoder dec(stringPiece);
    dec >> commit_tid;

    DCHECK(dec.size() == 0);

    std::atomic<uint64_t> &tid = table.search_metadata(key);

    uint64_t last_tid = TwoPLHelper::write_lock(tid);
    DCHECK(last_tid < commit_tid);
    table.deserialize_value(key, valueStringPiece);
    TwoPLHelper::write_lock_release(tid, commit_tid);

    // prepare response message header
    auto message_size = MessagePiece::get_header_size();
    auto message_piece_header = MessagePiece::construct_message_piece_header(
        static_cast<uint32_t>(HStoreMessage::REPLICATION_RESPONSE), message_size,
        table_id, partition_id);

    star::Encoder encoder(responseMessage.data);
    encoder << message_piece_header;
    responseMessage.flush();
  }

  static void replication_response_handler(MessagePiece inputPiece,
                                           Message &responseMessage,
                                           ITable &table, Transaction *txn) {

    DCHECK(inputPiece.get_message_type() ==
           static_cast<uint32_t>(HStoreMessage::REPLICATION_RESPONSE));
    auto table_id = inputPiece.get_table_id();
    auto partition_id = inputPiece.get_partition_id();
    DCHECK(table_id == table.tableID());
    DCHECK(partition_id == table.partitionID());
    auto key_size = table.key_size();

    /*
     * The structure of a replication response: ()
     */

    txn->pendingResponses--;
    txn->network_size += inputPiece.get_message_length();
  }

  static void release_read_lock_request_handler(MessagePiece inputPiece,
                                                Message &responseMessage,
                                                ITable &table,
                                                Transaction *txn) {

    DCHECK(inputPiece.get_message_type() ==
           static_cast<uint32_t>(HStoreMessage::RELEASE_READ_LOCK_REQUEST));
    auto table_id = inputPiece.get_table_id();
    auto partition_id = inputPiece.get_partition_id();
    DCHECK(table_id == table.tableID());
    DCHECK(partition_id == table.partitionID());
    auto key_size = table.key_size();

    /*
     * The structure of a release read lock request: (primary key)
     * The structure of a release read lock response: ()
     */

    DCHECK(inputPiece.get_message_length() ==
           MessagePiece::get_header_size() + key_size);

    auto stringPiece = inputPiece.toStringPiece();

    const void *key = stringPiece.data();
    stringPiece.remove_prefix(key_size);

    std::atomic<uint64_t> &tid = table.search_metadata(key);

    TwoPLHelper::read_lock_release(tid);

    // prepare response message header
    auto message_size = MessagePiece::get_header_size();
    auto message_piece_header = MessagePiece::construct_message_piece_header(
        static_cast<uint32_t>(HStoreMessage::RELEASE_READ_LOCK_RESPONSE),
        message_size, table_id, partition_id);

    star::Encoder encoder(responseMessage.data);
    encoder << message_piece_header;
    responseMessage.flush();
  }

  static void release_read_lock_response_handler(MessagePiece inputPiece,
                                                 Message &responseMessage,
                                                 ITable &table,
                                                 Transaction *txn) {

    DCHECK(inputPiece.get_message_type() ==
           static_cast<uint32_t>(HStoreMessage::RELEASE_READ_LOCK_RESPONSE));
    auto table_id = inputPiece.get_table_id();
    auto partition_id = inputPiece.get_partition_id();
    DCHECK(table_id == table.tableID());
    DCHECK(partition_id == table.partitionID());
    auto key_size = table.key_size();

    /*
     * The structure of a release read lock response: ()
     */

    txn->pendingResponses--;
    txn->network_size += inputPiece.get_message_length();
  }

  static void release_write_lock_request_handler(MessagePiece inputPiece,
                                                 Message &responseMessage,
                                                 ITable &table,
                                                 Transaction *txn) {

    DCHECK(inputPiece.get_message_type() ==
           static_cast<uint32_t>(HStoreMessage::RELEASE_WRITE_LOCK_REQUEST));
    auto table_id = inputPiece.get_table_id();
    auto partition_id = inputPiece.get_partition_id();
    DCHECK(table_id == table.tableID());
    DCHECK(partition_id == table.partitionID());
    auto key_size = table.key_size();
    /*
     * The structure of a release write lock request: (primary key, commit tid)
     * The structure of a release write lock response: ()
     */

    DCHECK(inputPiece.get_message_length() ==
           MessagePiece::get_header_size() + key_size + sizeof(uint64_t));

    auto stringPiece = inputPiece.toStringPiece();

    const void *key = stringPiece.data();
    stringPiece.remove_prefix(key_size);

    uint64_t commit_tid;
    Decoder dec(stringPiece);
    dec >> commit_tid;

    std::atomic<uint64_t> &tid = table.search_metadata(key);
    TwoPLHelper::write_lock_release(tid, commit_tid);

    // prepare response message header
    auto message_size = MessagePiece::get_header_size();
    auto message_piece_header = MessagePiece::construct_message_piece_header(
        static_cast<uint32_t>(HStoreMessage::RELEASE_WRITE_LOCK_RESPONSE),
        message_size, table_id, partition_id);

    star::Encoder encoder(responseMessage.data);
    encoder << message_piece_header;
    responseMessage.flush();
  }

  static void release_write_lock_response_handler(MessagePiece inputPiece,
                                                  Message &responseMessage,
                                                  ITable &table,
                                                  Transaction *txn) {
    DCHECK(inputPiece.get_message_type() ==
           static_cast<uint32_t>(HStoreMessage::RELEASE_WRITE_LOCK_RESPONSE));
    auto table_id = inputPiece.get_table_id();
    auto partition_id = inputPiece.get_partition_id();
    DCHECK(table_id == table.tableID());
    DCHECK(partition_id == table.partitionID());
    auto key_size = table.key_size();

    /*
     * The structure of a release write lock response: ()
     */

    txn->pendingResponses--;
    txn->network_size += inputPiece.get_message_length();
  }

public:
  static std::vector<
      std::function<void(MessagePiece, Message &, ITable &, Transaction *)>>
  get_message_handlers() {
    std::vector<
        std::function<void(MessagePiece, Message &, ITable &, Transaction *)>>
        v;
    v.resize(static_cast<int>(ControlMessage::NFIELDS));
    v.push_back(read_lock_request_handler);
    v.push_back(read_lock_response_handler);
    v.push_back(write_lock_request_handler);
    v.push_back(write_lock_response_handler);
    v.push_back(abort_request_handler);
    v.push_back(write_request_handler);
    v.push_back(write_response_handler);
    v.push_back(replication_request_handler);
    v.push_back(replication_response_handler);
    v.push_back(release_read_lock_request_handler);
    v.push_back(release_read_lock_response_handler);
    v.push_back(release_write_lock_request_handler);
    v.push_back(release_write_lock_response_handler);
    return v;
  }
};

} // namespace star
