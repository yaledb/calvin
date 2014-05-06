// Author: Alexander Thomson (thomson@cs.yale.edu)
// Author: Kun Ren (kun@cs.yale.edu)
//
// Lock manager implementing deterministic two-phase locking as described in
// 'The Case for Determinism in Database Systems'.

#ifndef _DB_SCHEDULER_DETERMINISTIC_LOCK_MANAGER_H_
#define _DB_SCHEDULER_DETERMINISTIC_LOCK_MANAGER_H_

#include <deque>
#include <tr1/unordered_map>

#include "common/configuration.h"
#include "scheduler/lock_manager.h"
#include "common/utils.h"

using std::tr1::unordered_map;
using std::deque;

#define TABLE_SIZE 1000000

class TxnProto;

class DeterministicLockManager {
 public:
  DeterministicLockManager(deque<TxnProto*>* ready_txns,
                           Configuration* config);
  virtual ~DeterministicLockManager() {}
  virtual int Lock(TxnProto* txn);
  virtual void Release(const Key& key, TxnProto* txn);
  virtual void Release(TxnProto* txn);

 private:
  int Hash(const Key& key) {
    uint64 hash = 2166136261;
    for (size_t i = 0; i < key.size(); i++) {
      hash = hash ^ (key[i]);
      hash = hash * 16777619;
    }
    return hash % TABLE_SIZE;
  }

  bool IsLocal(const Key& key) {
    return configuration_->LookupPartition(key) == configuration_->this_node_id;
  }

  // Configuration object (needed to avoid locking non-local keys).
  Configuration* configuration_;

  // The DeterministicLockManager's lock table tracks all lock requests. For a
  // given key, if 'lock_table_' contains a nonempty queue, then the item with
  // that key is locked and either:
  //  (a) first element in the queue specifies the owner if that item is a
  //      request for a write lock, or
  //  (b) a read lock is held by all elements of the longest prefix of the queue
  //      containing only read lock requests.
  // Note: using STL deque rather than queue for erase(iterator position).
  struct LockRequest {
    LockRequest(LockMode m, TxnProto* t) : txn(t), mode(m) {}
    TxnProto* txn;  // Pointer to txn requesting the lock.
    LockMode mode;  // Specifies whether this is a read or write lock request.
  };
  
  struct KeysList {
    KeysList(Key m, deque<LockRequest>* t) : key(m), locksrequest(t) {}
    Key key;
    deque<LockRequest>* locksrequest;
  };


  deque<KeysList>* lock_table_[TABLE_SIZE];

  // Queue of pointers to transactions that have acquired all locks that
  // they have requested. 'ready_txns_[key].front()' is the owner of the lock
  // for a specified key.
  //
  // Owned by the DeterministicScheduler.
  deque<TxnProto*>* ready_txns_;

  // Tracks all txns still waiting on acquiring at least one lock. Entries in
  // 'txn_waits_' are invalided by any call to Release() with the entry's
  // txn.
  unordered_map<TxnProto*, int> txn_waits_;
};
#endif  // _DB_SCHEDULER_DETERMINISTIC_LOCK_MANAGER_H_
