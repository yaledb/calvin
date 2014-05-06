// Author: Kun Ren (kun@cs.yale.edu)
// Author: Alexander Thomson (thomson@cs.yale.edu)
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

Sequencer::Sequencer(Configuration* conf, ConnectionMultiplexer* multiplexer,
                     Client* client, Storage* storage)
    : epoch_duration_(0.01), configuration_(conf), multiplexer_(multiplexer),
      client_(client), storage_(storage), deconstructor_invoked_(false) {
  pthread_mutex_init(&mutex_, NULL);
  // Start Sequencer main loops running in background thread.

message_queues = new AtomicQueue<MessageProto>();
restart_queues = new AtomicQueue<MessageProto>();
connection_ = multiplexer_->NewConnection("sequencer", &message_queues, &restart_queues);
cpu_set_t cpuset;
pthread_attr_t attr_writer;
pthread_attr_init(&attr_writer);
//pthread_attr_setdetachstate(&attr, PTHREAD_CREATE_DETACHED);

CPU_ZERO(&cpuset);
//CPU_SET(4, &cpuset);
//CPU_SET(5, &cpuset);
CPU_SET(6, &cpuset);
//CPU_SET(7, &cpuset);
pthread_attr_setaffinity_np(&attr_writer, sizeof(cpu_set_t), &cpuset);



  pthread_create(&writer_thread_, &attr_writer, RunSequencerWriter,
      reinterpret_cast<void*>(this));

CPU_ZERO(&cpuset);
//CPU_SET(4, &cpuset);
//CPU_SET(5, &cpuset);
//CPU_SET(6, &cpuset);
CPU_SET(2, &cpuset);
pthread_attr_t attr_reader;
pthread_attr_init(&attr_reader);
pthread_attr_setaffinity_np(&attr_reader, sizeof(cpu_set_t), &cpuset);

  pthread_create(&reader_thread_, &attr_reader, RunSequencerReader,
      reinterpret_cast<void*>(this));
}

Sequencer::~Sequencer() {
  deconstructor_invoked_ = true;
  pthread_join(writer_thread_, NULL);
  pthread_join(reader_thread_, NULL);
  delete message_queues;
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

#ifdef PREFETCHING
double PrefetchAll(Storage* storage, TxnProto* txn) {
  double max_wait_time = 0;
  double wait_time = 0;
  for (int i = 0; i < txn->read_set_size(); i++) {
    storage->Prefetch(txn->read_set(i), &wait_time);
    max_wait_time = MAX(max_wait_time, wait_time);
  }
  for (int i = 0; i < txn->read_write_set_size(); i++) {
    storage->Prefetch(txn->read_write_set(i), &wait_time);
    max_wait_time = MAX(max_wait_time, wait_time);
  }
  for (int i = 0; i < txn->write_set_size(); i++) {
    storage->Prefetch(txn->write_set(i), &wait_time);
    max_wait_time = MAX(max_wait_time, wait_time);
  }
#ifdef LATENCY_TEST
  if (txn->txn_id() % SAMPLE_RATE == 0)
    prefetch_cold[txn->txn_id() / SAMPLE_RATE] = max_wait_time;
#endif
  return max_wait_time;
}
#endif

void Sequencer::RunWriter() {
  Spin(1);

#ifdef PAXOS
  Paxos paxos(ZOOKEEPER_CONF, false);
#endif

#ifdef PREFETCHING
  multimap<double, TxnProto*> fetching_txns;
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
  std::cout << "Starting sequencer.\n" << std::flush;
  
unordered_map<string, TxnProto*> pending_txns;

  // Set up batch messages for each system node.
  MessageProto batch;
  batch.set_destination_channel("sequencer");
  batch.set_destination_node(-1);
  string batch_string;
  batch.set_type(MessageProto::TXN_BATCH);

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
      if (txn_id_offset < MAX_BATCH_SIZE && batch.data_size() < MAX_BATCH_SIZE) {
        TxnProto* txn;
        string txn_string;   
        MessageProto message;
        
        bool got_message = message_queues->Pop(&message);
        if(got_message == true) {
          if (message.type() == MessageProto::REMOTE_INDEX_RESULT) {

            assert(pending_txns.count(message.txn_id()) > 0);
            txn = pending_txns[message.txn_id()];
            pending_txns.erase(message.txn_id());
            if (txn->txn_type() == 3) {
              for (int i = 0; i < message.keys_size(); i++) {
                txn->add_read_write_set(message.keys(i));
              }
            }           
            txn->SerializeToString(&txn_string);
            batch.add_data(txn_string);
            delete txn;
          }
        } else {

          if (rand() % 100 < DEPENDENT_PERCENT) {
              client_->GetTxn(&txn, batch_number * MAX_BATCH_SIZE + txn_id_offset, 1);
              
              MessageProto message;
              message.set_type(MessageProto::REMOTE_INDEX_REQUEST);
              message.set_destination_channel("remote_index");
              message.set_destination_node(configuration_->this_node_id);
              message.set_index_number(txn->index_number());
              message.set_source_node(configuration_->this_node_id);
              message.set_source_channel("sequencer");
              message.set_txn_id(IntToString(txn->txn_id()));
              connection_->Send(message);
            
              pending_txns[IntToString(txn->txn_id())] = txn;
              txn_id_offset++;

          } else {
            client_->GetTxn(&txn, batch_number * MAX_BATCH_SIZE + txn_id_offset, 0);
            txn->SerializeToString(&txn_string);
            batch.add_data(txn_string);
            txn_id_offset++;
            delete txn;
          }
        }
      } else if (txn_id_offset >= MAX_BATCH_SIZE && batch.data_size() < MAX_BATCH_SIZE) {
        TxnProto* txn;
        string txn_string;   
        MessageProto message;
        
        bool got_message = message_queues->Pop(&message);
        if(got_message == true) {
          if (message.type() == MessageProto::REMOTE_INDEX_RESULT) {
           assert(pending_txns.count(message.txn_id()) > 0);
            txn = pending_txns[message.txn_id()];
            pending_txns.erase(message.txn_id());
            if (txn->txn_type() == 3) {
              for (int i = 0; i < message.keys_size(); i++) {
                txn->add_read_write_set(message.keys(i));
              }
            }           
            txn->SerializeToString(&txn_string);
            batch.add_data(txn_string);
            delete txn;
          }
        }
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
  for (map<int, Node*>::iterator it = configuration_->all_nodes.begin();
       it != configuration_->all_nodes.end(); ++it) {
    batches[it->first].set_destination_channel("scheduler_");
    batches[it->first].set_destination_node(it->first);
    batches[it->first].set_type(MessageProto::TXN_BATCH);
  }

  double time = GetTime();
  int txn_count = 0;
  int batch_count = 0;
  int batch_number = configuration_->this_node_id;

#ifdef LATENCY_TEST
  int watched_txn = -1;
#endif

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
//assert(batch_message.data_size() > 0);
    batch_message.ParseFromString(batch_string);
    for (int i = 0; i < batch_message.data_size(); i++) {
      TxnProto txn;
      txn.ParseFromString(batch_message.data(i));
#ifdef LATENCY_TEST
      if (txn.txn_id() % SAMPLE_RATE == 0)
        watched_txn = txn.txn_id();
#endif
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
assert(txn.writers_size() == 1);
assert(txn.readers_size() == 1);

      bytes txn_data;
      txn.SerializeToString(&txn_data);

      // Compute union of 'readers' and 'writers' (store in 'readers').
      for (set<int>::iterator it = writers.begin(); it != writers.end(); ++it)
        readers.insert(*it);

      // Insert txn into appropriate batches.
      for (set<int>::iterator it = readers.begin(); it != readers.end(); ++it)
        batches[*it].add_data(txn_data);

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

#ifdef LATENCY_TEST
    if (watched_txn != -1) {
      sequencer_send[watched_txn] = GetTime();
      watched_txn = -1;
    }
#endif

    // Report output.
    if (GetTime() > time + 1) {
#ifdef VERBOSE_SEQUENCER
      std::cout << "Submitted " << txn_count << " txns in "
                << batch_count << " batches,\n" << std::flush;
#endif
      // Reset txn count.
      time = GetTime();
      txn_count = 0;
      batch_count = 0;
    }
  }
  Spin(1);
}
