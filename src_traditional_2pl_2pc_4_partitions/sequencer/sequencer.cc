// Author: Alexander Thomson (thomson@cs.yale.edu)
// Author: Kun Ren (kun@cs.yale.edu)
//
// The sequencer component of the system is responsible for choosing a global
// serial order of transactions to which execution must maintain equivalence.
//
// TODO(scw): replace iostream with cstdio

#include "sequencer/sequencer.h"

#include <iostream>
#include <map>
#include <queue>
#include <set>
#include <utility>

#include "backend/storage.h"
#include "common/configuration.h"
#include "common/connection.h"
#include "common/utils.h"
#include "proto/message.pb.h"
#include "proto/txn.pb.h"
#ifdef PAXOS
# include "paxos/paxos.h"
#endif

using std::map;
using std::multimap;
using std::set;
using std::queue;

#ifdef LATENCY_TEST1
double execute_begin[SAMPLES];
double execute_end[SAMPLES];
#endif

#ifdef LATENCY_TEST
double sequencer_recv[SAMPLES];
// double paxos_begin[SAMPLES];
// double paxos_end[SAMPLES];
double sequencer_send[SAMPLES];
double prefetch_cold[SAMPLES];
double scheduler_lock[SAMPLES];
double worker_begin[SAMPLES];
double worker_end[SAMPLES];
double scheduler_unlock[SAMPLES];
#endif

void* Sequencer::RunSequencerWriter(void *arg) {
  reinterpret_cast<Sequencer*>(arg)->RunWriter();
  return NULL;
}

void* Sequencer::RunSequencerReader(void *arg) {
  reinterpret_cast<Sequencer*>(arg)->RunReader();
  return NULL;
}

Sequencer::Sequencer(Configuration* conf, Connection* connection,
                     Client* client)
    : epoch_duration_(0.01), configuration_(conf), connection_(connection),
      client_(client), deconstructor_invoked_(false) {
  pthread_mutex_init(&mutex_, NULL);

cpu_set_t cpuset;
pthread_attr_t attr_writer;
pthread_attr_init(&attr_writer);

CPU_ZERO(&cpuset);
//CPU_SET(4, &cpuset);
//CPU_SET(5, &cpuset);
CPU_SET(2, &cpuset);
//CPU_SET(7, &cpuset);
pthread_attr_setaffinity_np(&attr_writer, sizeof(cpu_set_t), &cpuset);


  // Start Sequencer main loops running in background thread.
  pthread_create(&writer_thread_, &attr_writer, RunSequencerWriter,
      reinterpret_cast<void*>(this));

CPU_ZERO(&cpuset);
//CPU_SET(4, &cpuset);
//CPU_SET(5, &cpuset);
//CPU_SET(6, &cpuset);
CPU_SET(6, &cpuset);
pthread_attr_t attr_reader;
pthread_attr_init(&attr_reader);
pthread_attr_setaffinity_np(&attr_reader, sizeof(cpu_set_t), &cpuset);

  pthread_create(&reader_thread_, &attr_reader, RunSequencerReader,
      reinterpret_cast<void*>(this));
}

Sequencer::~Sequencer() {
std::cout << "~Sequencer() execute.\n" << std::flush;
  deconstructor_invoked_ = true;
  pthread_join(writer_thread_, NULL);
  pthread_join(reader_thread_, NULL);
}

void Sequencer::FindParticipatingNodes(const TxnProto& txn, set<int>* nodes) {
  nodes->clear();
  for (int i = 0; i < txn.read_set_size(); i++)
    nodes->insert(configuration_->LookupPartition(txn.read_set(i)));
  for (int i = 0; i < txn.write_set_size(); i++)
    nodes->insert(configuration_->LookupPartition(txn.write_set(i)));
  for (int i = 0; i < txn.read_write_set_size(); i++)
    nodes->insert(configuration_->LookupPartition(txn.read_write_set(i)));
}

void Sequencer::RunWriter() {
  Spin(1);

#ifdef PAXOS
  Paxos paxos(ZOOKEEPER_CONF, false);
#endif

  // Synchronization loadgen start with other sequencers.
  MessageProto synchronization_message;
  synchronization_message.set_type(MessageProto::EMPTY);
  synchronization_message.set_destination_channel("sequencer");
  for (uint32 i = 0; i < configuration_->all_nodes.size(); i++) {
    synchronization_message.set_destination_node(i);
    if (i != static_cast<uint32>(configuration_->this_node_id))
      connection_->Send(synchronization_message);
  }
  uint32 synchronization_counter = 1;
  while (synchronization_counter < configuration_->all_nodes.size()) {
    synchronization_message.Clear();
    if (connection_->GetMessage(&synchronization_message)) {
      assert(synchronization_message.type() == MessageProto::EMPTY);
      synchronization_counter++;
    }
  }
//std::cout << "Starting sequencer, enter RunWriter.\n" << std::flush;

  // Set up batch messages for each system node.
  MessageProto batch;
  batch.set_destination_channel("sequencer");
  batch.set_destination_node(-1);
  string batch_string;
  batch.set_type(MessageProto::TXN_BATCH);
if(deconstructor_invoked_ == true)
std::cout << "deconstructor_invoked_ is true.\n" << std::flush;
  for (int batch_number = configuration_->this_node_id;
       !deconstructor_invoked_;
       batch_number += configuration_->all_nodes.size()) {
    // Begin epoch.
    double epoch_start = GetTime();
    batch.set_batch_number(batch_number);
    batch.clear_data();

    // Collect txn requests for this epoch.
    int txn_id_offset = 0;
    while (!deconstructor_invoked_ &&
           GetTime() < epoch_start + epoch_duration_) {
      // Add next txn request to batch.
      if (batch.data_size() < MAX_BATCH_SIZE) {
        TxnProto* txn;
        string txn_string;
        MessageProto message;
        if (connection_->GetMessage(&message)) {
          txn = new TxnProto();
          assert(message.type() == MessageProto::TXN_RESTART);
          txn->ParseFromString(message.data(0));
          txn->set_txn_id(batch_number * MAX_BATCH_SIZE + txn_id_offset);
          txn_id_offset++;
std::cout<<"~~~~ Sequencer received a MessageProto::TXN_RESTART message, txn id is "<<txn->txn_id()<<"\n"<<std::flush;
        } else {
          client_->GetTxn(&txn, batch_number * MAX_BATCH_SIZE + txn_id_offset);
          txn_id_offset++;
        }
        
        txn->SerializeToString(&txn_string);
        batch.add_data(txn_string);
        delete txn;
      }
    }
    // Send this epoch's requests to Paxos service.
    batch.SerializeToString(&batch_string);
#ifdef PAXOS
    paxos.SubmitBatch(batch_string);
#else
    pthread_mutex_lock(&mutex_);
    batch_queue_.push(batch_string);
    pthread_mutex_unlock(&mutex_);
#endif
  }

  Spin(1);
}

void Sequencer::RunReader() {
  Spin(1);
#ifdef PAXOS
  Paxos paxos(ZOOKEEPER_CONF, true);
#endif
  // Set up batch messages for each system node.
  map<int, MessageProto> batches;
  for (int i = 0; i < (int)configuration_->all_nodes.size(); i++) {
    string channel("scheduler_");
    batches[i].set_destination_channel(channel);
    batches[i].set_destination_node(i);
    batches[i].set_type(MessageProto::TXN_BATCH);
  }

  double time = GetTime();
  int txn_count = 0;
  int batch_count = 0;
  int batch_number = configuration_->this_node_id;

#ifdef LATENCY_TEST
  int watched_txn = -1;
#endif
if(deconstructor_invoked_ == true)
std::cout << "deconstructor_invoked_ is true.\n" << std::flush;
  while (!deconstructor_invoked_) {
    // Get batch from Paxos service.
    string batch_string;
    MessageProto batch_message;
#ifdef PAXOS
    paxos.GetNextBatchBlocking(&batch_string);
#else
    bool got_batch = false;
    do {
      pthread_mutex_lock(&mutex_);
      if (batch_queue_.size()) {
        batch_string = batch_queue_.front();
        batch_queue_.pop();
        got_batch = true;
      }
      pthread_mutex_unlock(&mutex_);
      if (!got_batch)
        Spin(0.001);

    } while (!got_batch);
#endif
    batch_message.ParseFromString(batch_string);
    for (int i = 0; i < batch_message.data_size(); i++) {
      TxnProto txn;
      txn.ParseFromString(batch_message.data(i));


      // Compute readers & writers; store in txn proto.
      set<int> readers;
      set<int> writers;
      for (int i = 0; i < txn.read_set_size(); i++)
        readers.insert(configuration_->LookupPartition(txn.read_set(i)));
      for (int i = 0; i < txn.write_set_size(); i++)
        writers.insert(configuration_->LookupPartition(txn.write_set(i)));
      for (int i = 0; i < txn.read_write_set_size(); i++) {
        writers.insert(configuration_->LookupPartition(txn.read_write_set(i)));
        readers.insert(configuration_->LookupPartition(txn.read_write_set(i)));
      }

      for (set<int>::iterator it = readers.begin(); it != readers.end(); ++it)
        txn.add_readers(*it);
      for (set<int>::iterator it = writers.begin(); it != writers.end(); ++it)
        txn.add_writers(*it);
      bytes txn_data;
      txn.SerializeToString(&txn_data);


      for (set<int>::iterator it = writers.begin(); it != writers.end(); ++it)
        readers.insert(*it);


      for (set<int>::iterator it = readers.begin(); it != readers.end(); ++it) {
        batches[*it].add_data(txn_data);
      }

      txn_count++;
    }
    // Send this epoch's requests to all schedulers.
    for (map<int, MessageProto>::iterator it = batches.begin();
         it != batches.end(); ++it) {
      it->second.set_batch_number(batch_number);
      connection_->Send(it->second);
      it->second.clear_data();
    }
    batch_number += configuration_->all_nodes.size();
    batch_count++;

    // Report output.
    if (GetTime() > time + 1) {

      // Reset txn count.
      time = GetTime();
      txn_count = 0;
      batch_count = 0;
    }
  }
  Spin(1);
}
