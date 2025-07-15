// Author: Alexander Thomson (thomson@cs.yale.edu)

#include "applications/microbenchmark.h"

#include <set>

#include "backend/simple_storage.h"
#include "backend/storage_manager.h"
#include "common/configuration.h"
#include "common/connection.h"
#include "common/testing.h"
#include "common/utils.h"
#include "proto/txn.pb.h"

using std::set;

SimpleStorage* actual_storage;
Configuration* config;

#define CHECK_OBJECT(KEY, EXPECTED_VALUE) do {     \
  Value* actual_value;                             \
  actual_value = actual_storage->ReadObject(KEY); \
  EXPECT_EQ(EXPECTED_VALUE, *actual_value);        \
} while (0)

TEST(MicrobenchmarkTest) {
  config = new Configuration(0, "common/configuration_test_one_node.conf");
  ConnectionMultiplexer* multiplexer = new ConnectionMultiplexer(config);
  Connection* connection = multiplexer->NewConnection("asdf");
  actual_storage = new SimpleStorage();
  Microbenchmark microbenchmark(1, 100);

  // Initialize storage.
  microbenchmark.InitializeStorage(actual_storage, config);

  // Execute a 'MICROTXN_SP' txn.
  TxnProto* txn = microbenchmark.MicroTxnSP(1, 0);
  txn->add_readers(0);
  txn->add_writers(0);

  StorageManager* storage = new StorageManager(config, connection,
                                               actual_storage, txn);
  microbenchmark.Execute(txn, storage);

  // Check post-execution storage state.
  set<int> write_set;
  for (int i = 0; i < Microbenchmark::kRWSetSize; i++)
    write_set.insert(StringToInt(txn->write_set(i)));
  for (int i = 0; i < microbenchmark.kDBSize; i++) {
    if (write_set.count(i))
      CHECK_OBJECT(IntToString(i), IntToString(i+1));
    else
      CHECK_OBJECT(IntToString(i), IntToString(i));
  }



  delete storage;
  delete txn;

  delete actual_storage;
  delete connection;
  delete multiplexer;
  delete config;

  END;
}

int main(int argc, char** argv) {
  MicrobenchmarkTest();
}

