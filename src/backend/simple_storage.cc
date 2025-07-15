// Author: Alexander Thomson (thomson@cs.yale.edu)
// Author: Kun Ren (kun.ren@yale.edu)
//
// A simple implementation of the storage interface using an stl map.

#include "backend/simple_storage.h"

Value* SimpleStorage::ReadObject(const Key& key, int64 txn_id) {
  if (objects_.count(key) != 0) {
    return objects_[key];
  } else {
    return NULL;
  }
}

bool SimpleStorage::PutObject(const Key& key, Value* value, int64 txn_id) {
pthread_mutex_lock(&mutex_);
  objects_[key] = value;
pthread_mutex_unlock(&mutex_);
  return true;
}

bool SimpleStorage::DeleteObject(const Key& key, int64 txn_id) {
  objects_.erase(key);
  return true;
}

void SimpleStorage::Initmutex() {
  pthread_mutex_init(&mutex_, NULL);
}
