// Author: Alexander Thomson (thomson@cs.yale.edu)
// Author: Kun Ren (kun@cs.yale.edu)
//
// TODO(scw): remove iostream, use cstdio instead

#include "applications/microbenchmark.h"

#include <iostream>

#include "backend/storage.h"
#include "backend/storage_manager.h"
#include "common/utils.h"
#include "common/configuration.h"
#include "proto/txn.pb.h"

// #define PREFETCHING
#define COLD_CUTOFF 990000

// Fills '*keys' with num_keys unique ints k where
// 'key_start' <= k < 'key_limit', and k == part (mod nparts).
// Requires: key_start % nparts == 0
void Microbenchmark::GetRandomKeys(set<int>* keys, int num_keys, int key_start,
                                   int key_limit, int part) {
  assert(key_start % nparts == 0);
  keys->clear();
  for (int i = 0; i < num_keys; i++) {
    // Find a key not already in '*keys'.
    int key;
    do {
      key = key_start + part +
            nparts * (rand() % ((key_limit - key_start)/nparts));
    } while (keys->count(key));
    keys->insert(key);
  }
}

TxnProto* Microbenchmark::InitializeTxn() {
  // Create the new transaction object
  TxnProto* txn = new TxnProto();

  // Set the transaction's standard attributes
  txn->set_txn_id(0);
  txn->set_txn_type(INITIALIZE);

  // Nothing read, everything written.
  for (int i = 0; i < kDBSize; i++)
    txn->add_write_set(IntToString(i));

  return txn;
}

TxnProto* Microbenchmark::MicroTxnSP(int64 txn_id, int part) {
  // Create the new transaction object
  TxnProto* txn = new TxnProto();

  // Set the transaction's standard attributes
  txn->set_txn_id(txn_id);
  txn->set_txn_type(MICROTXN_SP);
  txn->set_status(TxnProto::NEW);


  // Add one hot key to read/write set.
  int hotkey = part + nparts * (rand() % hot_records);
  txn->add_read_write_set(IntToString(hotkey));

  // Insert set of kRWSetSize - 1 random cold keys from specified partition into
  // read/write set.
  set<int> keys;
  GetRandomKeys(&keys,
                kRWSetSize - 1,
                nparts * hot_records,
                nparts * kDBSize,
                part);
  for (set<int>::iterator it = keys.begin(); it != keys.end(); ++it)
    txn->add_read_write_set(IntToString(*it));

  return txn;
}

TxnProto* Microbenchmark::MicroTxnMP(int64 txn_id, int part1, int part2) {
  assert(part1 != part2 || nparts == 1);
  // Create the new transaction object
  TxnProto* txn = new TxnProto();

  // Set the transaction's standard attributes
  txn->set_txn_id(txn_id);
  txn->set_txn_type(MICROTXN_MP);
  txn->set_multipartition(true);
  txn->set_status(TxnProto::NEW);

  // Add two hot keys to read/write set---one in each partition.
  int hotkey1 = part1 + nparts * (rand() % hot_records);
  int hotkey2 = part2 + nparts * (rand() % hot_records);
  txn->add_read_write_set(IntToString(hotkey1));
  txn->add_read_write_set(IntToString(hotkey2));

  // Insert set of kRWSetSize/2 - 1 random cold keys from each partition into
  // read/write set.
  set<int> keys;
  GetRandomKeys(&keys,
                kRWSetSize/2 - 1,
                nparts * hot_records,
                nparts * kDBSize,
                part1);
  for (set<int>::iterator it = keys.begin(); it != keys.end(); ++it)
    txn->add_read_write_set(IntToString(*it));
  GetRandomKeys(&keys,
                kRWSetSize/2 - 1,
                nparts * hot_records,
                nparts * kDBSize,
                part2);
  for (set<int>::iterator it = keys.begin(); it != keys.end(); ++it)
    txn->add_read_write_set(IntToString(*it));

  return txn;
}

// The load generator can be called externally to return a transaction proto
// containing a new type of transaction.
TxnProto* Microbenchmark::NewTxn(int64 txn_id, int txn_type,
                                 string args, Configuration* config, int partition_id) const {
  return NULL;
}

int Microbenchmark::Execute(TxnProto* txn, StorageManager* storage) const {
  // Read all elements of 'txn->read_set()', add one to each, write them all
  // back out.
  for (int i = 0; i < kRWSetSize; i++) {
    ValueStore* val = storage->ReadObject(txn->read_write_set(i));
    
    // The following is for "long" transactions.
    int x = 1;
    for(int i = 0; i < 1100; i++) {
      x = x*x+1;
      x = x+10;
      x = x-2;
    }

    val->value = IntToString(StringToInt(val->value) + 1);
    val->exclusive_lock_number --;
  }

  return 0;
}

void Microbenchmark::InitializeStorage(Storage* storage,
                                       Configuration* conf,
                                       int partition_id) const {
  for (int i = 0; i < nparts*kDBSize; i++) {
    if (conf->LookupPartition(IntToString(i)) == partition_id) {
      storage->PutObject(IntToString(i), new ValueStore(IntToString(i), 0, 0));
    }
  }
}

