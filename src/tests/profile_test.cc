#include "applications/tpcc.h"
#include "backend/collapsed_versioned_storage.h"
#include "backend/storage_manager.h"
#include "common/configuration.h"
#include "common/connection.h"
#include "common/testing.h"
#include "common/utils.h"

int main(int argc, char** argv) {
  Configuration* config =
    new Configuration(0, "common/configuration_test_one_node.conf");
  CollapsedVersionedStorage* storage = new CollapsedVersionedStorage();
  TPCC* tpcc = new TPCC();

  TPCC().InitializeStorage(storage, config);

  for (int i = 0; i < 100000; i++) {
    TPCCArgs args;
    args.set_system_time(GetTime());
    args.set_multipartition(false);
    string args_string;
    args.SerializeToString(&args_string);
    TxnProto* txn = tpcc->NewTxn(0, TPCC::NEW_ORDER, args_string, config);
    txn->add_readers(0);
    txn->add_writers(0);

    StorageManager* manager = new StorageManager(config, NULL, storage, txn);

    tpcc->Execute(txn, manager);

    delete manager;
    delete txn;
  }

  delete tpcc;
  delete storage;
  delete config;
}
