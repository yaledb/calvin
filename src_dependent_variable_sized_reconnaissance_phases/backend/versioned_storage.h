// Author: Thaddeus Diamond (diamond@cs.yale.edu)
//
// This is an abstract interface to a versioned storage system
// (credit: comments are exactly the same as they are for storage.h)

#ifndef _DB_BACKEND_VERSIONED_STORAGE_H_
#define _DB_BACKEND_VERSIONED_STORAGE_H_

#include <vector>

#include "backend/storage.h"

using std::vector;

class VersionedStorage : public Storage {
 public:
  virtual ~VersionedStorage() {}

  // Read object takes a transaction id and places the value at time t <= txn_id
  // into the address pointed to by result
  virtual Value* ReadObject(const Key& key, int64 txn_id) = 0;

  // Put object takes a value and adds a version to the linked list storage
  // with the specified txn_id as a timestamp
  virtual bool PutObject(const Key& key, Value* value, int64 txn_id) = 0;

  // The delete method actually merely places an empty string at the version
  // specified by txn_id.  This is in contrast to actually deleting a value
  // (which versioned storage never does.
  virtual bool DeleteObject(const Key& key, int64 txn_id) = 0;

  // TODO(Thad): We should really make this a virtually required interface for
  // all storage classes but to avoid conflicts I'm just gonna leave it in the
  // versioned storage class.
  // This method is a requirement for versioned storages to implement.  It
  // causes a snapshot to be performed at the virtually consistent state
  // specified (i.e. txn_id).
  virtual int Checkpoint() = 0;


  // TODO(Thad): Implement something real here
  virtual bool Prefetch(const Key &key, double* wait_time)  { return false; }
  virtual bool Unfetch(const Key &key)                      { return false; }
};

#endif  // _DB_BACKEND_VERSIONED_STORAGE_H_
