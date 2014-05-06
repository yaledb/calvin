// Author: Alexander Thomson (thomson@cs.yale.edu)
// Author: Kun Ren (kun@cs.yale.edu)

#include "backend/storage_manager.h"

#include <ucontext.h>

#include "backend/storage.h"
#include "common/configuration.h"
#include "common/connection.h"
#include "common/utils.h"
#include "proto/txn.pb.h"
#include "proto/message.pb.h"

StorageManager::StorageManager(Configuration* config, Connection* connection,Storage* actual_storage,
                               TxnProto* txn, int partition_id, ValueStore* cache_storage[])
    : configuration_(config), connection_(connection),actual_storage_(actual_storage),
      txn_(txn), partition_id_(partition_id){
  MessageProto message;

  // If reads are performed at this node, execute local reads and broadcast
  // results to all (other) writers.
  bool reader = false;
  for (int i = 0; i < txn->readers_size(); i++) {
    if (txn->readers(i) == partition_id_)
      reader = true;
  }

  if (reader) {
    message.set_type(MessageProto::READ_RESULT);
int j = 0;
for(int i = 0; i < txn->read_write_set_size(); i++) {
  if (configuration_->LookupPartition(txn->read_write_set(i)) ==
          partition_id_) {
    objects_[txn->read_write_set(i)] = cache_storage[j];
    message.add_keys(txn->read_write_set(i));
    message.add_values(cache_storage[j]->value);
    j++;
  }
}

for(int i = 0; i < txn->read_set_size(); i++) {
  if (configuration_->LookupPartition(txn->read_set(i)) ==
          partition_id_) {
    objects_[txn->read_set(i)] = cache_storage[j];
    message.add_keys(txn->read_set(i));
    message.add_values(cache_storage[j]->value);
    j++;
  } 
}

    // Broadcast local reads to (other) writers.
    for (int i = 0; i < txn->writers_size(); i++) {
      if (txn->writers(i) != partition_id_) {
        message.set_destination_node(txn->writers(i) / WorkersNumber);
        string channel = IntToString(txn->writers(i));
        channel.append(IntToString(txn->txn_id()));
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
    ValueStore* val = new ValueStore(message.values(i), 1 , 1);
    objects_[message.keys(i)] = val;
    remote_reads_.push_back(val);
  }
}

bool StorageManager::ReadyToExecute() {
  return static_cast<int>(objects_.size()) ==
         txn_->read_set_size() + txn_->read_write_set_size();
}

StorageManager::~StorageManager() {
  for (vector<ValueStore*>::iterator it = remote_reads_.begin();
       it != remote_reads_.end(); ++it) {
    delete *it;
  }
}

ValueStore* StorageManager::ReadObject(const Key& key) {
  return objects_[key];
}

bool StorageManager::PutObject(const Key& key, ValueStore* value) {
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

