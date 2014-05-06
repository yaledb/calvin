// Author: Alexander Thomson (thomson@cs.yale.edu)
// Author: Kun Ren (kun.ren@yale.edu)

#include "backend/storage_manager.h"

#include <ucontext.h>

#include "backend/storage.h"
#include "common/configuration.h"
#include "common/connection.h"
#include "common/utils.h"
#include "proto/txn.pb.h"
#include "proto/message.pb.h"

StorageManager::StorageManager(Configuration* config, Connection* connection,
                               Storage* actual_storage, TxnProto* txn)
    : configuration_(config), connection_(connection),
      actual_storage_(actual_storage), txn_(txn) {
  MessageProto message;

  // If reads are performed at this node, execute local reads and broadcast
  // results to all (other) writers.
  bool reader = false;
  for (int i = 0; i < txn->readers_size(); i++) {
    if (txn->readers(i) == configuration_->this_node_id)
      reader = true;
  }

  if (reader) {
    message.set_destination_channel(IntToString(txn->txn_id()));
    message.set_type(MessageProto::READ_RESULT);

    // Execute local reads.
    for (int i = 0; i < txn->read_set_size(); i++) {
      const Key& key = txn->read_set(i);
      if (configuration_->LookupPartition(key) ==
          configuration_->this_node_id) {
        Value* val = actual_storage_->ReadObject(key);
        objects_[key] = val;
        message.add_keys(key);
        message.add_values(val == NULL ? "" : *val);
      }
    }
    for (int i = 0; i < txn->read_write_set_size(); i++) {
      const Key& key = txn->read_write_set(i);
      if (configuration_->LookupPartition(key) ==
          configuration_->this_node_id) {
        Value* val = actual_storage_->ReadObject(key);
        objects_[key] = val;
        message.add_keys(key);
        message.add_values(val == NULL ? "" : *val);
      }
    }

    // Broadcast local reads to (other) writers.
    for (int i = 0; i < txn->writers_size(); i++) {
      if (txn->writers(i) != configuration_->this_node_id) {
        message.set_destination_node(txn->writers(i));
        connection_->Send1(message);
      }
    }
  }

  // Note whether this node is a writer. If not, no need to do anything further.
  writer = false;
  for (int i = 0; i < txn->writers_size(); i++) {
    if (txn->writers(i) == configuration_->this_node_id)
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
  if (configuration_->LookupPartition(key) == configuration_->this_node_id)
    return actual_storage_->PutObject(key, value, txn_->txn_id());
  else
    return true;  // Not this node's problem.
}

bool StorageManager::DeleteObject(const Key& key) {
  // Delete object from storage if applicable.
  if (configuration_->LookupPartition(key) == configuration_->this_node_id)
    return actual_storage_->DeleteObject(key, txn_->txn_id());
  else
    return true;  // Not this node's problem.
}

