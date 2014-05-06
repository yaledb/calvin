// Author: Kun Ren (kun@cs.yale.edu)
//
// Lock manager implementing deterministic two-phase locking as described in
// 'The Case for Determinism in Database Systems'.

#ifndef _DB_SCHEDULER_DETERMINISTIC_LOCK_MANAGER_H_
#define _DB_SCHEDULER_DETERMINISTIC_LOCK_MANAGER_H_

#include <deque>
#include <tr1/unordered_map>
#include <vector>

#include "common/configuration.h"
#include "scheduler/lock_manager.h"
#include "common/utils.h"
#include "common/connection.h"

using std::tr1::unordered_map;
using std::deque;
using std::pair;
using std::vector;

#define TABLE_SIZE 1000000

class TxnProto;

class DeterministicLockManager {
 public:
  DeterministicLockManager(AtomicQueue<TxnProto*>* ready_txns, Configuration* config);
  virtual ~DeterministicLockManager() {}
  virtual int Lock(TxnProto* txn, Connection* connection);
  virtual int ContinueLock(TxnProto* txn, Connection* connection);
  virtual void Release(TxnProto* txn, Connection* connection);
  virtual void Release(const Key& key, TxnProto* txn, Connection* connection);
  virtual bool CheckDeadlock(TxnProto* txn);
  virtual bool CheckDistributedDeadlock(int64 txn1_id, int64 txn2_id);
  virtual void HandleDistributedDeadlock(MessageProto message, Connection* connection);
  virtual int try_deadlock_mutex();
  virtual void release_deadlock_mutex();
  virtual TxnProto* GetBlockedTxn(string txn_id);

  class Latch {
   public:
     pthread_mutex_t lock_;
     
     Latch() {
       pthread_mutex_init(&lock_, NULL);
     }  
  };
  Latch* LatchFor(const int& index);

pthread_cond_t*  cond;
pthread_mutex_t*  mutex;
unordered_map<TxnProto*, Key> txn_waits_;
  
 private:
  int Hash(const Key& key) {
    uint64 hash = 2166136261;
    for (size_t i = 0; i < key.size(); i++) {
      hash = hash ^ (key[i]);
      hash = hash * 16777619;
    }
    return hash % TABLE_SIZE;
    //return atoi(key.c_str());
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

  Latch* latches_;
  
  AtomicQueue<TxnProto*>* ready_txns_;

  // Tracks all txns still waiting on acquiring at least one lock. Entries in
  // 'txn_waits_' are invalided by any call to Release() with the entry's
  // txn.
  
  pthread_mutex_t new_mutex_;
  
  pthread_mutex_t distributed_deadlock_mutex_;
  
  unordered_map<int64, int64> wait_for_graph;
  
  unordered_map<int64, vector<int64>> distributed_wait_for_graph;
};
#endif  // _DB_SCHEDULER_DETERMINISTIC_LOCK_MANAGER_H_
