// Author: Alexander Thomson (thomson@cs.yale.edu)
// Author: Kun Ren (kun@cs.yale.edu)
//
// Main invokation of a single node in the system.

#include <csignal>
#include <cstdio>
#include <cstdlib>

#include "applications/microbenchmark.h"
#include "applications/tpcc.h"
#include "common/configuration.h"
#include "common/connection.h"
#include "common/utils.h"
#include "backend/simple_storage.h"
#include "scheduler/deterministic_scheduler.h"
#include "sequencer/sequencer.h"
#include "proto/tpcc_args.pb.h"

#define HOT 100

map<Key, Key> latest_order_id_for_customer;
map<Key, int> latest_order_id_for_district;
map<Key, int> smallest_order_id_for_district;
map<Key, Key> customer_for_order;
unordered_map<Key, int> next_order_id_for_district;
map<Key, int> item_for_order_line;
map<Key, int> order_line_number;

vector<Key>* involed_customers;

pthread_mutex_t mutex_;
pthread_mutex_t mutex_for_item;

unordered_map<string, int> doing_deadlocks;

// Microbenchmark load generation client.
class MClient : public Client {
 public:
  MClient(Configuration* config, int mp)
      : microbenchmark(config->all_nodes.size(), HOT), config_(config),
        percent_mp_(mp) {
  }
  virtual ~MClient() {}
  virtual void GetTxn(TxnProto** txn, int txn_id) {
    if (config_->all_nodes.size() > 1 && rand() % 100 < percent_mp_) {
      // Multipartition txn.
      int other1;
      do {
        other1 = rand() % config_->all_nodes.size();
      } while (other1 == config_->this_node_id);

      *txn = microbenchmark.MicroTxnMP(txn_id, config_->this_node_id, other1);
    } else {
      *txn = microbenchmark.MicroTxnSP(txn_id, config_->this_node_id);
    }
  }

 private:
  Microbenchmark microbenchmark;
  Configuration* config_;
  int percent_mp_;
};

// TPCC load generation client.
class TClient : public Client {
 public:
  TClient(Configuration* config, int mp) : config_(config), percent_mp_(mp) {}
  virtual ~TClient() {}
  virtual void GetTxn(TxnProto** txn, int txn_id) {
    TPCC tpcc;
    TPCCArgs args;

    args.set_system_time(GetTime());
    if (rand() % 100 < percent_mp_)
      args.set_multipartition(true);
    else
      args.set_multipartition(false);

    string args_string;
    args.SerializeToString(&args_string);

        // New order txn
   int random_txn_type = rand() % 100;
    // New order txn
    if (random_txn_type < 45)  {
      *txn = tpcc.NewTxn(txn_id, TPCC::NEW_ORDER, args_string, config_);
    } else if(random_txn_type < 88) {
      *txn = tpcc.NewTxn(txn_id, TPCC::PAYMENT, args_string, config_);
    } else if(random_txn_type < 92) {
      *txn = tpcc.NewTxn(txn_id, TPCC::ORDER_STATUS, args_string, config_);
      args.set_multipartition(false);
    } else if(random_txn_type < 96){
      *txn = tpcc.NewTxn(txn_id, TPCC::DELIVERY, args_string, config_);
      args.set_multipartition(false);
    } else {
      *txn = tpcc.NewTxn(txn_id, TPCC::STOCK_LEVEL, args_string, config_);
      args.set_multipartition(false);
    }
  }

 private:
  Configuration* config_;
  int percent_mp_;
};

void stop(int sig) {
// #ifdef PAXOS
//  StopZookeeper(ZOOKEEPER_CONF);
// #endif
  exit(sig);
}

int main(int argc, char** argv) {
  // TODO(alex): Better arg checking.
  if (argc < 4) {
    fprintf(stderr, "Usage: %s <node-id> <m[icro]|t[pcc]> <percent_mp>\n",
            argv[0]);
    exit(1);
  }

  // Catch ^C and kill signals and exit gracefully (for profiling).
  signal(SIGINT, &stop);
  signal(SIGTERM, &stop);

  // Build this node's configuration object.
  Configuration config(StringToInt(argv[1]), "deploy-run.conf");

  // Build connection context and start multiplexer thread running.
  ConnectionMultiplexer multiplexer(&config);

  // Artificial loadgen clients.
  Client* client = (argv[2][0] == 'm') ?
      reinterpret_cast<Client*>(new MClient(&config, atoi(argv[3]))) :
      reinterpret_cast<Client*>(new TClient(&config, atoi(argv[3])));

  pthread_mutex_init(&mutex_, NULL);
  pthread_mutex_init(&mutex_for_item, NULL);
  involed_customers = new vector<Key>;
// #ifdef PAXOS
//  StartZookeeper(ZOOKEEPER_CONF);
// #endif
  // Create the storage of the partition
  Storage* storage = new SimpleStorage();
    if (argv[2][0] == 'm') {
      Microbenchmark(config.all_nodes.size(), HOT).InitializeStorage(storage, &config);
    } else {
      TPCC().InitializeStorage(storage, &config);
    }
   
  // Initialize sequencer component and start sequencer thread running.
  Sequencer sequencer(&config, multiplexer.NewConnection("sequencer"), client);
storage->Initmutex();
  // Initialize scheduler component and start sequencer thread running.
    if (argv[2][0] == 'm') {
      // Run scheduler in main thread.
      DeterministicScheduler scheduler(&config,
                                       &multiplexer,
                                       storage,
				       new Microbenchmark(config.all_nodes.size(), HOT));
    } else {
	  // Run scheduler in main thread.
     DeterministicScheduler scheduler(&config,
                                       &multiplexer,
                                       storage,
     			       new TPCC());
    }

  Spin(120);
  return 0;
}

