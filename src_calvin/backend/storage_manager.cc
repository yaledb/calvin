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

#include "applications/tpcc.h"
#include "proto/tpcc.pb.h"

#include <iostream>
#include <vector>

using std::vector;
using std::cout;

enum class KeyType{
  None,
  Warehouse,          //"w123"
  WarehouseYTD,       //"w123y"
  District,           //"w123d3"
  DistrictYTD,        //"w123d3y"
  Customer,           //"w123d2c312"
  Stock,              //"w123si999"
  Item,               //"i999"
};

KeyType testKeyType(const Key &key){
  KeyType ret;
  if (key.find('c') != static_cast<size_t>(-1)){
    ret = KeyType::Customer;
  }
  else if (key.find('s') != static_cast<size_t>(-1)){
    ret = KeyType::Stock;
  }
  else if (key.find('i') != static_cast<size_t>(-1)){
    ret = KeyType::Item;
  }
  else if (key.find('d') != static_cast<size_t>(-1)){
    if (key.find('y') != static_cast<size_t>(-1)){
      ret = KeyType::DistrictYTD;
    }
    else{
      ret = KeyType::District;
    }
  }
  else{
    if (key.find('y') != static_cast<size_t>(-1)){
      ret = KeyType::WarehouseYTD;
    }
    else{
      ret = KeyType::Warehouse;
    }
  }
  return ret;
}

// Aux functionto resolve district id
vector<string> resolveDistrictId(const Key &districtId){
  auto pos = districtId.find('d');
  return {districtId.substr(0, pos), districtId.substr(pos)};
}

// Aux function to resolve stock id
vector<string> resolveStockId(const Key &stockId){
  auto pos = stockId.find('s');
  return {stockId.substr(0, pos), stockId.substr(pos+1)};
}

// Aux function to resolve cumstomer id
vector<string> resolveCustomerId(const Key &customerId){
  // string format is like: w127d5c932
  auto dpos = customerId.find('d');
  auto cpos = customerId.find('c');
  return {customerId.substr(0, dpos), 
          customerId.substr(dpos, cpos - dpos),
          customerId.substr(cpos)};
}

Value* genVal(const Key &key){
  auto type = testKeyType(key); // may merge the function into this function.
  Value* val = nullptr;
  if (type == KeyType::Warehouse){
    auto warehouse = TPCC::CreateWarehouse(key);
    val = new Value();
    assert(warehouse->SerializeToString(val));
    delete warehouse;
  }
  else if (type == KeyType::WarehouseYTD){
    val = new Value(key);
  }
  else if (type == KeyType::District){
    auto seg = resolveDistrictId(key);
    auto district = TPCC::CreateDistrict(seg[1], seg[0]);
    val = new Value();
    assert(district->SerializeToString(val));
    delete district;
  }
  else if (type == KeyType::DistrictYTD){
    val = new Value(key);
  }
  else if (type == KeyType::Customer){
    auto seg = resolveCustomerId(key);
    auto customer = TPCC::CreateCustomer(seg[2], seg[1], seg[0]);
    val = new Value();
    assert(customer->SerializeToString(val));
    delete customer;
  }
  else if (type == KeyType::Stock){
    auto seg = resolveStockId(key);
    auto stock = TPCC::CreateStock(seg[1], seg[0]);
    val = new Value();
    assert(stock->SerializeToString(val));
    delete stock;
  }
  else{ // item
    cout << "key is "<<key<<"\n";
    assert(false);
  }
  return val;
}

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
        if (val == NULL){
          // continue;
          val = genVal(key);
        }

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
        if (val == NULL){
          // continue;
          val = genVal(key);
        }
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
  //assert(objects_.count(key) == 1);
  //if (objects_.count(key) != 0){
  //  return objects_[key];
  //}
  //return nullptr;
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

