// Author: Alexander Thomson (thomson@cs.yale.edu)
// Author: Kun Ren (kun@cs.yale.edu)
//
// SerialScheduler is a trivial scheduler that executes transactions serially
// as they come in, without locking.
//
// TODO(scw): replace iostream with cstdio

#include "scheduler/serial_scheduler.h"

#include <iostream>

#include "applications/application.h"
#include "common/configuration.h"
#include "common/connection.h"
#include "common/utils.h"
#include "backend/storage_manager.h"
#include "proto/message.pb.h"
#include "proto/txn.pb.h"

SerialScheduler::SerialScheduler(Configuration* conf, Connection* connection,
                                 Storage* storage, bool checkpointing)
    : configuration_(conf), connection_(connection), storage_(storage),
      checkpointing_(checkpointing) {
}

SerialScheduler::~SerialScheduler() {}

void SerialScheduler::Run(const Application& application) {
  MessageProto message;
  TxnProto txn;
  StorageManager* manager;
  Connection* manager_connection =
  connection_->multiplexer()->NewConnection("manager_connection");

  int txns = 0;
  double time = GetTime();
  double start_time = time;
  while (true) {
    if (connection_->GetMessage(&message)) {
      // Execute all txns in batch.
      for (int i = 0; i < message.data_size(); i++) {
        txn.ParseFromString(message.data(i));

        // Link txn-specific channel ot manager_connection.
        manager_connection->LinkChannel(IntToString(txn.txn_id()));

        // Create manager.
        manager = new StorageManager(configuration_, manager_connection,
                                     storage_, &txn);

        // Execute txn if any writes occur at this node.
        if (manager->writer) {
          while (!manager->ReadyToExecute()) {
            if (connection_->GetMessage(&message))
              manager->HandleReadResult(message);
          }
          application.Execute(&txn, manager);
        }
        // Clean up the mess.
        delete manager;
        manager_connection->UnlinkChannel(IntToString(txn.txn_id()));

        // Report throughput (once per second). TODO(alex): Fix reporting.
        if (txn.writers(txn.txn_id() % txn.writers_size()) ==
            configuration_->this_node_id)
          txns++;
        if (GetTime() > time + 1) {
          std::cout << "Executed " << txns << " txns\n" << std::flush;
          // Reset txn count.
          time = GetTime();
          txns = 0;
        }
      }
    }

    // Report throughput (once per second).
    if (GetTime() > time + 1) {
      std::cout << "Executed " << txns << " txns\n" << std::flush;
      // Reset txn count.
      time = GetTime();
      txns = 0;
    }

    // Run for at most one minute.
    if (GetTime() > start_time + 60)
      exit(0);
  }

  delete manager_connection;
}
