// Author: Alexander Thomson (thomson@cs.yale.edu)
// Author: Kun Ren (kun@cs.yale.edu)
//
// A simple implementation of the storage interface using an stl map.

#include "backend/simple_storage.h"

ValueStore* SimpleStorage::ReadObject(const Key& key, int64 txn_id) { 
  if (objects_.count(key) != 0) {
    return objects_[key];
  } else {
    return NULL;
  }
}

bool SimpleStorage::PutObject(const Key& key, ValueStore* value, int64 txn_id) {
  objects_[key] = value;
  return true;
}

bool SimpleStorage::DeleteObject(const Key& key, int64 txn_id) {
  objects_.erase(key);
  return true;
}
