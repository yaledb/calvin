// Author: Thaddeus Diamond (diamond@cs.yale.edu)
//
// This implements a simple collapsed storage that can be used in a versioned
// deterministic database system.

#ifndef _DB_BACKEND_COLLAPSED_VERSIONED_STORAGE_H_
#define _DB_BACKEND_COLLAPSED_VERSIONED_STORAGE_H_

#include <climits>
#include <cstring>
#include <tr1/unordered_map>

#include "backend/versioned_storage.h"

#define CHKPNTDIR "../db/checkpoints"

using std::tr1::unordered_map;

struct DataNode {
  int64 txn_id;
  Value* value;
  DataNode* next;
};

class CollapsedVersionedStorage : public VersionedStorage {
 public:
  CollapsedVersionedStorage() {
    stable_ = 0;

#ifdef TPCCHACK
    memset(&NewOrderStore, 0, sizeof(NewOrderStore));
    memset(&OrderStore, 0, sizeof(OrderStore));
    memset(&OrderLineStore, 0, sizeof(OrderLineStore));
    memset(&HistoryStore, 0, sizeof(HistoryStore));
#endif
  }
  virtual ~CollapsedVersionedStorage() {}

  // TODO(Thad and Philip): How can we incorporate this type of versioned
  // storage into the work that you've been doing with prefetching?  It seems
  // like we could do something optimistic with writing to disk and avoiding
  // having to checkpoint, but we should see.

  // Standard operators in the DB
  virtual Value* ReadObject(const Key& key, int64 txn_id = LLONG_MAX);
  virtual bool PutObject(const Key& key, Value* value, int64 txn_id);
  virtual bool DeleteObject(const Key& key, int64 txn_id);

  // Specify the overloaded parent functions we are using here
  using VersionedStorage::Prefetch;
  using VersionedStorage::Unfetch;

  // At a new versioned state, the version system is notified that the
  // previously stable values are no longer necessary.  At this point in time,
  // the database can switch the labels as to what is stable (the previously
  // frozen values) to a new txn_id occurring in the future.
  virtual void PrepareForCheckpoint(int64 stable) { stable_ = stable; }
  virtual int Checkpoint();

  // The capture checkpoint method is an internal method that allows us to
  // write out the stable checkpoint to disk.
  virtual void CaptureCheckpoint();

 private:
  // We make a simple mapping of keys to a map of "versions" of our value.
  // The int64 represents a simple transaction id and the Value associated with
  // it is whatever value was written out at that time.
  unordered_map<Key, DataNode*> objects_;

  // The stable and frozen int64 represent which transaction ID's are stable
  // to write out to storage, and which should be the latest to be overwritten
  // in the current database execution cycle, respectively.
  int64 stable_;
};

static inline void* RunCheckpointer(void* storage) {
  (reinterpret_cast<CollapsedVersionedStorage*>(storage))->CaptureCheckpoint();
  return NULL;
}

#endif  // _DB_BACKEND_COLLAPSED_VERSIONED_STORAGE_H_
