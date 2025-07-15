// Author: Alexander Thomson (thomson@cs.yale.edu)
//
// Interface for lock managers in the system.

#ifndef _DB_SCHEDULER_LOCK_MANAGER_H_
#define _DB_SCHEDULER_LOCK_MANAGER_H_

#include <vector>

#include "common/types.h"

using std::vector;

class TxnProto;

// This interface supports locks being held in both read/shared and
// write/exclusive modes.
enum LockMode {
  UNLOCKED = 0,
  READ = 1,
  WRITE = 2,
};

class LockManager {
 public:
  virtual ~LockManager() {}
  // Attempts to assign the lock for each key in keys to the specified
  // transaction. Returns the number of requested locks NOT assigned to the
  // transaction (therefore Lock() returns 0 if the transaction successfully
  // acquires all locks).
  //
  // Requires: 'read_keys' and 'write_keys' do not overlap, and neither contains
  //           duplicate keys.
  // Requires: Lock has not previously been called with this txn_id. Note that
  //           this means Lock can only ever be called once per txn.
  virtual int Lock(TxnProto* txn) = 0;

  // For each key in 'keys':
  //   - If the specified transaction owns the lock on the item, the lock is
  //     released.
  //   - If the transaction is in the queue to acquire a lock on the item, the
  //     request is cancelled and the transaction is removed from the item's
  //     queue.
  virtual void Release(const Key& key, TxnProto* txn) = 0;
  virtual void Release(TxnProto* txn) = 0;

  // Locked sets '*owner' to contain the txn IDs of all txns holding the lock,
  // and returns the current state of the lock: UNLOCKED if it is not currently
  // held, READ or WRITE if it is, depending on the current state.
  virtual LockMode Status(const Key& key, vector<TxnProto*>* owners) = 0;
};
#endif  // _DB_SCHEDULER_LOCK_MANAGER_H_
