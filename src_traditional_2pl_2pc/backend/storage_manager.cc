// Author: Alexander Thomson (thomson@cs.yale.edu)
// Author: Kun Ren (kun@cs.yale.edu)

#include "backend/storage_manager.h"

#include <ucontext.h>
#include <cstdlib>
#include <iostream>

#include "backend/storage.h"
#include "common/configuration.h"
#include "common/connection.h"
#include "common/utils.h"
#include "proto/txn.pb.h"
#include "proto/message.pb.h"

StorageManager::StorageManager(Configuration* config, Connection* connection,
                               Storage* actual_storage, TxnProto* txn, int partition_id)
    : configuration_(config), connection_(connection),
      actual_storage_(actual_storage), txn_(txn), partition_id_(partition_id) {
  MessageProto message;

  // If reads are performed at this node, execute local reads and broadcast
  // results to all (other) writers.
received_prepared_reply_number = 0;
received_prepared_message = false;

received_commit_reply_number = 0;
received_commit_message = false;

  bool reader = false;
  for (int i = 0; i < txn->readers_size(); i++) {
    if (txn->readers(i) == partition_id_)
      reader = true;
  }

  if (reader) {
   // message.set_destination_channel(IntToString(txn->txn_id()));
    message.set_type(MessageProto::READ_RESULT);

    // Execute local reads.
    for (int i = 0; i < txn->read_set_size(); i++) {
      const Key& key = txn->read_set(i);
      if (configuration_->LookupPartition(key) ==
          partition_id_) {
        Value* val = actual_storage_->ReadObject(key);
        objects_[key] = val;
        message.add_keys(key);
        message.add_values(val == NULL ? "" : *val);
      }
    }
    for (int i = 0; i < txn->read_write_set_size(); i++) {
      const Key& key = txn->read_write_set(i);
      if (configuration_->LookupPartition(key) == partition_id_) {
        Value* val = actual_storage_->ReadObject(key);
        objects_[key] = val;
        message.add_keys(key);
        message.add_values(val == NULL ? "" : *val);
      }
    }

    // Broadcast local reads to (other) writers.
    for (int i = 0; i < txn->writers_size(); i++) {
      if (txn->writers(i) != partition_id_) {
        message.set_destination_node(txn->writers(i));
        string channel = IntToString(txn->txn_id());
        message.set_destination_channel(channel);        
        connection_->Send1(message);
      }
    }
  }

  // Note whether this node is a writer. If not, no need to do anything further.
  writer = false;
  for (int i = 0; i < txn->writers_size(); i++) {
    if (txn->writers(i) == partition_id_)
      writer = true;
  }

  // Scheduler is responsible for calling HandleReadResponse. We're done here.
}

void StorageManager::HandleReadResult(const MessageProto& message) {
  assert(message.type() == MessageProto::READ_RESULT);
  for (int i = 0; i < message.keys_size(); i++) {
    Value* val = new Value(message.values(i));
    objects_[message.keys(i)] = val;
    remote_reads_.push_back(val);
  }
}

bool StorageManager::ReadyToExecute() {
  return static_cast<int>(objects_.size()) ==
         txn_->read_set_size() + txn_->read_write_set_size();
}
// -----------FOR 2PC prepard phase------------------
bool StorageManager::ReceivedAllPreparedReplyMessages() {
  return received_prepared_reply_number == txn_->writers_size() - 1;
}

void StorageManager::BroadcastPreparedMessages() {
  // Broadcast local reads to (other) writers.
  for (int i = 0; i < txn_->writers_size(); i++) {
    if (txn_->writers(i) != partition_id_) {
      MessageProto message;
      message.set_type(MessageProto::PREPARED);
      message.set_destination_node(txn_->writers(i));
      string channel = IntToString(txn_->txn_id());
      message.set_destination_channel(channel);        
      connection_->Send1(message);
    }
  }
}

void StorageManager::HandlePreparedReplyMessages(const MessageProto& message) {
  assert(message.type() == MessageProto::PREPARED_REPLY);
  received_prepared_reply_number ++;
}

bool StorageManager::ReceivedPreparedMessages() {
  return received_prepared_message == true;
}

void StorageManager::HandlePreparedMessages(const MessageProto& message) {
  assert(message.type() == MessageProto::PREPARED);
  received_prepared_message = true;
}

void StorageManager::SendPreparedReplyMessages() {
  MessageProto message;
  message.set_type(MessageProto::PREPARED_REPLY);
  // Broadcast local reads to (other) writers.

  message.set_destination_node(txn_->txn_node());
  string channel = IntToString(txn_->txn_id());
  message.set_destination_channel(channel);        
  connection_->Send1(message);
}
// -----------FOR 2PC prepard phase------------------


// -----------FOR 2PC commit phase------------------
bool StorageManager::ReceivedAllCommitReplyMessages() {
  return received_commit_reply_number == txn_->writers_size() - 1;
}

void StorageManager::BroadcastCommitMessages() {

  // Broadcast local reads to (other) writers.
  for (int i = 0; i < txn_->writers_size(); i++) {
    if (txn_->writers(i) != partition_id_) {
      MessageProto message;
      message.set_type(MessageProto::COMMIT);
      message.set_destination_node(txn_->writers(i));
      string channel = IntToString(txn_->txn_id());
      message.set_destination_channel(channel);        
      connection_->Send1(message);
    }
  }
}

void StorageManager::HandleCommitReplyMessages(const MessageProto& message) {
  assert(message.type() == MessageProto::COMMIT_REPLY);
  received_commit_reply_number ++;
}

bool StorageManager::ReceivedCommitMessages() {
  return received_commit_message == true;
}

void StorageManager::HandleCommitMessages(const MessageProto& message) {
  assert(message.type() == MessageProto::COMMIT);
  received_commit_message = true;
}

void StorageManager::SendCommitReplyMessages() {
  MessageProto message;
  message.set_type(MessageProto::COMMIT_REPLY);
  // Broadcast local reads to (other) writers.

  message.set_destination_node(txn_->txn_node());
  string channel = IntToString(txn_->txn_id());
  message.set_destination_channel(channel);        
  connection_->Send1(message);
}
// -----------FOR 2PC commit phase------------------




StorageManager::~StorageManager() {
  for (vector<Value*>::iterator it = remote_reads_.begin();
       it != remote_reads_.end(); ++it) {
    delete *it;
  }
}

Value* StorageManager::ReadObject(const Key& key) {
  return objects_[key];
}

bool StorageManager::PutObject(const Key& key, Value* value) {
  // Write object to storage if applicable.
  if (configuration_->LookupPartition(key) == partition_id_)
    return actual_storage_->PutObject(key, value, txn_->txn_id());
  else
    return true;  // Not this node's problem.
}

bool StorageManager::DeleteObject(const Key& key) {
  // Delete object from storage if applicable.
  if (configuration_->LookupPartition(key) == partition_id_)
    return actual_storage_->DeleteObject(key, txn_->txn_id());
  else
    return true;  // Not this node's problem.
}

