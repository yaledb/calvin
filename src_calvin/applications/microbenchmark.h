// Author: Alexander Thomson (thomson@cs.yale.edu)
// Author: Kun Ren (kun@cs.yale.edu)
//
// A microbenchmark application that reads all elements of the read_set, does
// some trivial computation, and writes to all elements of the write_set.

#ifndef _DB_APPLICATIONS_MICROBENCHMARK_H_
#define _DB_APPLICATIONS_MICROBENCHMARK_H_

#include <set>
#include <string>

#include "applications/application.h"
#include "common/definitions.hh"
#include "common/random.hh"
#include "common/zipf.hh"

using std::set;
using std::string;

class Microbenchmark : public Application {
 public:
  Xoroshiro128Plus rnd_;
  FastZipf zipf_;

  enum TxnType {
    INITIALIZE = 0,
    MICROTXN_SP = 1,
    MICROTXN_MP = 2,
  };

  Microbenchmark(int nodecount, int hotcount)
      : rnd_(), zipf_(&rnd_, SKEW, DB_SIZE) {
    nparts = nodecount;
    hot_records = hotcount;
  }

  virtual ~Microbenchmark() {}

  virtual TxnProto* NewTxn(int64 txn_id,
                           int txn_type,
                           string args,
                           Configuration* config = NULL) const;
  virtual int Execute(TxnProto* txn, StorageManager* storage) const;

  TxnProto* InitializeTxn();
  TxnProto* MicroTxnSP(int64 txn_id, int part);
  TxnProto* MicroTxnMP(int64 txn_id, int part1, int part2);

  int nparts;
  int hot_records;

  virtual void InitializeStorage(Storage* storage, Configuration* conf) const;

 private:
  void GetRandomKeys(set<int>* keys,
                     int num_keys,
                     int key_start,
                     int key_limit,
                     int part,
                     bool is_uniform);
  Microbenchmark() : rnd_(), zipf_(&rnd_, SKEW, DB_SIZE) {}
};

#endif  // _DB_APPLICATIONS_MICROBENCHMARK_H_
