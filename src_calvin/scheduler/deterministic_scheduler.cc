// Author: Kun Ren (kun@cs.yale.edu)
// Author: Alexander Thomson (thomson@cs.yale.edu)
//
// The deterministic lock manager implements deterministic locking as described
// in 'The Case for Determinism in Database Systems', VLDB 2010. Each
// transaction must request all locks it will ever need before the next
// transaction in the specified order may acquire any locks. Each lock is then
// granted to transactions in the order in which they requested them (i.e. in
// the global transaction order).
//
// TODO(scw): replace iostream with cstdio

#include "scheduler/deterministic_scheduler.h"

#include <cstdlib>
#include <iostream>
#include <string>
#include <tr1/unordered_map>
#include <utility>
#include <sched.h>
#include <map>

#include "applications/application.h"
#include "common/utils.h"
#include "common/zmq.hpp"
#include "common/connection.h"
#include "backend/storage.h"
#include "backend/storage_manager.h"
#include "proto/message.pb.h"
#include "proto/txn.pb.h"
#include "scheduler/deterministic_lock_manager.h"
#include "applications/tpcc.h"

// XXX(scw): why the F do we include from a separate component
//           to get COLD_CUTOFF
#include "sequencer/sequencer.h"  // COLD_CUTOFF and buffers in LATENCY_TEST

using std::pair;
using std::string;
using std::tr1::unordered_map;
using zmq::socket_t;
using std::map;

static void DeleteTxnPtr(void* data, void* hint) { free(data); }

void DeterministicScheduler::SendTxnPtr(socket_t* socket, TxnProto* txn) {
  TxnProto** txn_ptr = reinterpret_cast<TxnProto**>(malloc(sizeof(txn)));
  *txn_ptr = txn;
  zmq::message_t msg(txn_ptr, sizeof(*txn_ptr), DeleteTxnPtr, NULL);
  socket->send(msg);
}

TxnProto* DeterministicScheduler::GetTxnPtr(socket_t* socket,
                                            zmq::message_t* msg) {
  if (!socket->recv(msg, ZMQ_NOBLOCK))
    return NULL;
  TxnProto* txn = *reinterpret_cast<TxnProto**>(msg->data());
  return txn;
}

DeterministicScheduler::DeterministicScheduler(Configuration* conf,
                                               Connection* batch_connection,
                                               Storage* storage,
                                               const Application* application)
    : configuration_(conf), batch_connection_(batch_connection),
      storage_(storage), application_(application) {
      ready_txns_ = new std::deque<TxnProto*>();
  lock_manager_ = new DeterministicLockManager(ready_txns_, configuration_);
  
  txns_queue = new AtomicQueue<TxnProto*>();
  done_queue = new AtomicQueue<TxnProto*>();

  for (int i = 0; i < NUM_THREADS; i++) {
    message_queues[i] = new AtomicQueue<MessageProto>();
  }

  Spin(1);

  // start lock manager thread
  cpu_set_t cpuset;
  pthread_attr_t attr1;
  pthread_attr_init(&attr1);
  //pthread_attr_setdetachstate(&attr1, PTHREAD_CREATE_DETACHED);
  
  CPU_ZERO(&cpuset);
  CPU_SET(7, &cpuset);
  pthread_attr_setaffinity_np(&attr1, sizeof(cpu_set_t), &cpuset);
  pthread_create(&lock_manager_thread_, &attr1, LockManagerThread,
                 reinterpret_cast<void*>(this));


  // Start all worker threads.
  for (int i = 0; i < NUM_THREADS; i++) {
    string channel("scheduler");
    channel.append(IntToString(i));
    thread_connections_[i] = batch_connection_->multiplexer()->NewConnection(channel, &message_queues[i]);

    pthread_attr_t attr;
    pthread_attr_init(&attr);
    CPU_ZERO(&cpuset);
    if (i == 0 || i == 1)
      CPU_SET(i, &cpuset);
    else
      CPU_SET(i+2, &cpuset);
    pthread_attr_setaffinity_np(&attr, sizeof(cpu_set_t), &cpuset);

    pthread_create(&(threads_[i]), &attr, RunWorkerThread,
                   reinterpret_cast<void*>(
                   new pair<int, DeterministicScheduler*>(i, this)));
  }

}

void UnfetchAll(Storage* storage, TxnProto* txn) {
  for (int i = 0; i < txn->read_set_size(); i++)
    if (StringToInt(txn->read_set(i)) > COLD_CUTOFF)
      storage->Unfetch(txn->read_set(i));
  for (int i = 0; i < txn->read_write_set_size(); i++)
    if (StringToInt(txn->read_write_set(i)) > COLD_CUTOFF)
      storage->Unfetch(txn->read_write_set(i));
  for (int i = 0; i < txn->write_set_size(); i++)
    if (StringToInt(txn->write_set(i)) > COLD_CUTOFF)
      storage->Unfetch(txn->write_set(i));
}

void* DeterministicScheduler::RunWorkerThread(void* arg) {
  int thread =
      reinterpret_cast<pair<int, DeterministicScheduler*>*>(arg)->first;
  DeterministicScheduler* scheduler =
      reinterpret_cast<pair<int, DeterministicScheduler*>*>(arg)->second;

  unordered_map<string, StorageManager*> active_txns;

  // Begin main loop.
  MessageProto message;
  while (true) {
    bool got_message = scheduler->message_queues[thread]->Pop(&message);
    if (got_message == true) {
      // Remote read result.
      assert(message.type() == MessageProto::READ_RESULT);
      StorageManager* manager = active_txns[message.destination_channel()];
      manager->HandleReadResult(message);
      if (manager->ReadyToExecute()) {
        // Execute and clean up.
        TxnProto* txn = manager->txn_;
        scheduler->application_->Execute(txn, manager);
        delete manager;

        scheduler->thread_connections_[thread]->
            UnlinkChannel(IntToString(txn->txn_id()));
        active_txns.erase(message.destination_channel());
        // Respond to scheduler;
        //scheduler->SendTxnPtr(scheduler->responses_out_[thread], txn);
        scheduler->done_queue->Push(txn);
      }
    } else {
      // No remote read result found, start on next txn if one is waiting.
     TxnProto* txn;
     bool got_it = scheduler->txns_queue->Pop(&txn);
      if (got_it == true) {
        // Create manager.
        StorageManager* manager =
            new StorageManager(scheduler->configuration_,
                               scheduler->thread_connections_[thread],
                               scheduler->storage_, txn);

          // Writes occur at this node.
          if (manager->ReadyToExecute()) {
            // No remote reads. Execute and clean up.
            scheduler->application_->Execute(txn, manager);
            delete manager;

            // Respond to scheduler;
            //scheduler->SendTxnPtr(scheduler->responses_out_[thread], txn);
            scheduler->done_queue->Push(txn);
          } else {
        scheduler->thread_connections_[thread]->
            LinkChannel(IntToString(txn->txn_id()));
            // There are outstanding remote reads.
            active_txns[IntToString(txn->txn_id())] = manager;
          }
      }
    }
  }
  return NULL;
}

DeterministicScheduler::~DeterministicScheduler() {
}

// Returns ptr to heap-allocated
unordered_map<int, MessageProto*> batches;
MessageProto* GetBatch(int batch_id, Connection* connection) {
  if (batches.count(batch_id) > 0) {
    // Requested batch has already been received.
    MessageProto* batch = batches[batch_id];
    batches.erase(batch_id);
    return batch;
  } else {
    MessageProto* message = new MessageProto();
    while (connection->GetMessage(message)) {
      assert(message->type() == MessageProto::TXN_BATCH);
      if (message->batch_number() == batch_id) {
        return message;
      } else {
        batches[message->batch_number()] = message;
        message = new MessageProto();
      }
    }
    delete message;
    return NULL;
  }
}

void* DeterministicScheduler::LockManagerThread(void* arg) {
  DeterministicScheduler* scheduler = reinterpret_cast<DeterministicScheduler*>(arg);

  // Run main loop.
  MessageProto message;
  MessageProto* batch_message = NULL;
  int txns = 0;
  double time = GetTime();
  int executing_txns = 0;
  int pending_txns = 0;
  int batch_offset = 0;
  int batch_number = 0;
//int test = 0;
  while (true) {
    TxnProto* done_txn;
    bool got_it = scheduler->done_queue->Pop(&done_txn);
    if (got_it == true) {
      // We have received a finished transaction back, release the lock
      scheduler->lock_manager_->Release(done_txn);
      executing_txns--;

      if(done_txn->writers_size() == 0 || rand() % done_txn->writers_size() == 0)
        txns++;       

      delete done_txn;

    } else {
      // Have we run out of txns in our batch? Let's get some new ones.
      if (batch_message == NULL) {
        batch_message = GetBatch(batch_number, scheduler->batch_connection_);

      // Done with current batch, get next.
      } else if (batch_offset >= batch_message->data_size()) {
        batch_offset = 0;
        batch_number++;
        delete batch_message;
        batch_message = GetBatch(batch_number, scheduler->batch_connection_);

      // Current batch has remaining txns, grab up to 10.
      } else if (executing_txns + pending_txns < 2000) {

        for (int i = 0; i < 100; i++) {
          if (batch_offset >= batch_message->data_size()) {
            // Oops we ran out of txns in this batch. Stop adding txns for now.
            break;
          }
          TxnProto* txn = new TxnProto();
          txn->ParseFromString(batch_message->data(batch_offset));
          batch_offset++;

          scheduler->lock_manager_->Lock(txn);
          pending_txns++;
        }

      }
    }

    // Start executing any and all ready transactions to get them off our plate
    while (!scheduler->ready_txns_->empty()) {
      TxnProto* txn = scheduler->ready_txns_->front();
      scheduler->ready_txns_->pop_front();
      pending_txns--;
      executing_txns++;

      scheduler->txns_queue->Push(txn);
    }

    // Report throughput.
    if (GetTime() > time + 1) {
      double total_time = GetTime() - time;
      std::cout << "Completed " << (static_cast<double>(txns) / total_time)
                << " txns/sec, "
                //<< test<< " for drop speed , " 
                << executing_txns << " executing, "
                << pending_txns << " pending\n" << std::flush;
      // Reset txn count.
      time = GetTime();
      txns = 0;
      //test ++;
    }
  }
  return NULL;
}
