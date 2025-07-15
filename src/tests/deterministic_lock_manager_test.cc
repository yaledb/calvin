// Author: Alexander Thomson (thomson@cs.yale.edu)

#include "scheduler/deterministic_lock_manager.h"

#include <set>
#include <string>

#include "applications/microbenchmark.h"
#include "applications/tpcc.h"
#include "common/utils.h"
#include "common/testing.h"

using std::set;
/*
TEST(SimpleLockingTest) {
  deque<TxnProto*> ready_txns;
  DeterministicLockManager lm(&ready_txns);
  vector<Key> key1, none;
  vector<TxnProto*> owners;

  key1.push_back(Key("key1"));

  TxnProto* t1 = (TxnProto*) 1;
  TxnProto* t2 = (TxnProto*) 2;
  TxnProto* t3 = (TxnProto*) 3;

  // Txn 1 acquires read lock.
  lm.Lock(key1, none, t1);
  EXPECT_EQ(READ, lm.Status(Key("key1"), &owners));
  EXPECT_EQ(1, owners.size());
  EXPECT_EQ(t1, owners[0]);
  EXPECT_EQ(1, ready_txns.size());
  EXPECT_EQ(t1, ready_txns.at(0));

  // Txn 2 requests write lock. Not granted.
  lm.Lock(none, key1, t2);
  EXPECT_EQ(READ, lm.Status(Key("key1"), &owners));
  EXPECT_EQ(1, owners.size());
  EXPECT_EQ(t1, owners[0]);
  EXPECT_EQ(1, ready_txns.size());

  // Txn 3 requests read lock. Not granted.
  lm.Lock(key1, none, t3);
  EXPECT_EQ(READ, lm.Status(Key("key1"), &owners));
  EXPECT_EQ(1, owners.size());
  EXPECT_EQ(t1, owners[0]);
  EXPECT_EQ(1, ready_txns.size());

  // Txn 1 releases lock.  Txn 2 is granted write lock.
  lm.Release(key1, t1);
  EXPECT_EQ(WRITE, lm.Status(Key("key1"), &owners));
  EXPECT_EQ(1, owners.size());
  EXPECT_EQ(t2, owners[0]);
  EXPECT_EQ(2, ready_txns.size());
  EXPECT_EQ(t2, ready_txns.at(1));

  // Txn 2 releases lock.  Txn 3 is granted read lock.
  lm.Release(key1, t2);
  EXPECT_EQ(READ, lm.Status(Key("key1"), &owners));
  EXPECT_EQ(1, owners.size());
  EXPECT_EQ(t3, owners[0]);
  EXPECT_EQ(3, ready_txns.size());
  EXPECT_EQ(t3, ready_txns.at(2));

  END;
}

TEST(LocksReleasedOutOfOrder) {
  deque<TxnProto*> ready_txns;
  DeterministicLockManager lm(&ready_txns);
  vector<Key> key1, none;
  vector<TxnProto*> owners;

  key1.push_back(Key("key1"));

  TxnProto* t1 = (TxnProto*) 1;
  TxnProto* t2 = (TxnProto*) 2;
  TxnProto* t3 = (TxnProto*) 3;
  TxnProto* t4 = (TxnProto*) 4;

  lm.Lock(key1, none, t1);  // Txn 1 acquires read lock.
  lm.Lock(none, key1, t2);  // Txn 2 requests write lock. Not granted.
  lm.Lock(key1, none, t3);  // Txn 3 requests read lock. Not granted.
  lm.Lock(key1, none, t4);  // Txn 4 requests read lock. Not granted.

  lm.Release(key1, t2);  // Txn 2 cancels write lock request.

  // Txns 1, 3 and 4 should now have a shared lock.
  EXPECT_EQ(READ, lm.Status(Key("key1"), &owners));
  EXPECT_EQ(3, owners.size());
  EXPECT_EQ(t1, owners[0]);
  EXPECT_EQ(t3, owners[1]);
  EXPECT_EQ(t4, owners[2]);
  EXPECT_EQ(3, ready_txns.size());
  EXPECT_EQ(t1, ready_txns.at(0));
  EXPECT_EQ(t3, ready_txns.at(1));
  EXPECT_EQ(t4, ready_txns.at(2));

  END;
}
*/

TEST(ThroughputTest) {
  deque<TxnProto*> ready_txns;
  Configuration config(0, "common/configuration_test_one_node.conf");
  DeterministicLockManager lm(&ready_txns, &config);
  vector<TxnProto*> txns;

  TPCC tpcc;
  TPCCArgs args;
  args.set_system_time(GetTime());
  args.set_multipartition(false);
  string args_string;
  args.SerializeToString(&args_string);

  for (int i = 0; i < 100000; i++) {
//    txns.push_back(new TxnProto());
//    for (int j = 0; j < 10; j++)
//      txns[i]->add_read_write_set(IntToString(j * 1000 + rand() % 1000));
    txns.push_back(tpcc.NewTxn(i, TPCC::NEW_ORDER, args_string, NULL));
  }

  double start = GetTime();

  int next = 0;
  for (int i = 0; i < 1000; i++) {
    for (int j = 0; j < 100; j++)
      lm.Lock(txns[next++]);

    while (ready_txns.size() > 0) {
      TxnProto* txn = ready_txns.front();
      ready_txns.pop_front();
      lm.Release(txn);
    }
  }

  cout << 100000.0 / (GetTime() - start) << " txns/sec\n";

  END;
}

int main(int argc, char** argv) {
//  SimpleLockingTest();
//  LocksReleasedOutOfOrder();
  ThroughputTest();
}

