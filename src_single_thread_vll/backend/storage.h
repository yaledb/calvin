// Author: Alexander Thomson (thomson@cs.yale.edu)
//
// The Storage class provides an interface for writing and accessing data
// objects stored by the system.

#ifndef _DB_BACKEND_STORAGE_H_
#define _DB_BACKEND_STORAGE_H_

#include <vector>

#include "common/types.h"

using std::vector;

struct ValueStore {
  ValueStore(Value v, int e, int s) : value(v), exclusive_lock_number(e), share_lock_number(s) {}
  // The actual value
  Value value;
  int exclusive_lock_number;
  int share_lock_number;
};


class Storage {
 public:
  virtual ~Storage() {}

  // Loads object specified by 'key' into memory if currently stored
  // on disk, asynchronously or otherwise.
  virtual bool Prefetch(const Key &key, double* wait_time) = 0;

  // Unfetch object on memory, writing it off to disk, asynchronously or
  // otherwise.
  virtual bool Unfetch(const Key &key) = 0;

  // If the object specified by 'key' exists, copies the object into '*result'
  // and returns true. If the object does not exist, false is returned.
  virtual ValueStore* ReadObject(const Key& key, int64 txn_id = 0) = 0;

  // Sets the object specified by 'key' equal to 'value'. Any previous version
  // of the object is replaced. Returns true if the write succeeds, or false if
  // it fails for any reason.
  virtual bool PutObject(const Key& key, ValueStore* value, int64 txn_id = 0) = 0;

  // Removes the object specified by 'key' if there is one. Returns true if the
  // deletion succeeds (or if no object is found with the specified key), or
  // false if it fails for any reason.
  virtual bool DeleteObject(const Key& key, int64 txn_id = 0) = 0;

  // TODO(Thad): Something here
  virtual void PrepareForCheckpoint(int64 stable) {}
  virtual int Checkpoint() { return 0; }
};

#endif  // _DB_BACKEND_STORAGE_H_

