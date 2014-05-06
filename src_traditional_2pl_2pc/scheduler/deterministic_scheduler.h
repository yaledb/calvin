// Author: Alexander Thomson (thomson@cs.yale.edu)
// Author: Kun Ren (kun@cs.yale.edu)
//
// The deterministic lock manager implements deterministic locking as described
// in 'The Case for Determinism in Database Systems', VLDB 2010. Each
// transaction must request all locks it will ever need before the next
// transaction in the specified order may acquire any locks. Each lock is then
// granted to transactions in the order in which they requested them (i.e. in
// the global transaction order).

#ifndef _DB_SCHEDULER_DETERMINISTIC_SCHEDULER_H_
#define _DB_SCHEDULER_DETERMINISTIC_SCHEDULER_H_

#include <pthread.h>

#include <deque>
#include <atomic>

#include "scheduler/scheduler.h"
#include <tr1/unordered_map>
#include "common/connection.h"
#include "common/configuration.h"
#include "scheduler/deterministic_lock_manager.h"
#include "common/utils.h"
#include "proto/txn.pb.h"
#include "proto/message.pb.h"

using std::deque;
using std::tr1::unordered_map;
using std::atomic;

class Configuration;
class Connection;
class ConnectionMultiplexer;
class DeterministicLockManager;
class Storage;
class TxnProto;
class Application;

class DeterministicScheduler : public Scheduler {
 public:
  DeterministicScheduler(Configuration* conf, ConnectionMultiplexer* multiplexer,
                         Storage* storage, const Application* application);
  virtual ~DeterministicScheduler();
  
  // Connection for receiving txn batches from sequencer.
  Connection* batch_connections_;
  pthread_mutex_t test_mutex_;
  pthread_mutex_t batch_mutex_; 
  AtomicQueue<TxnProto*>* batch_txns;
  int batch_id;

 private:
  // Application currently being run.
  const Application* application_;

  // Function for starting main loops in a separate pthreads.
  static void* RunWorkerThread(void* arg);

  // Configuration specifying node & system settings.
  Configuration* configuration_;

  // Thread contexts and their associated Connection objects.
  pthread_t threads_[WorkersNumber];
  Connection* thread_connections_[WorkersNumber];
  
  ConnectionMultiplexer* multiplexer_;

  // Storage layer used in application execution.
  Storage* storage_;

  DeterministicLockManager* lock_manager_;
  
  AtomicQueue<MessageProto>* message_queues[WorkersNumber];
  
  AtomicQueue<TxnProto*>* ready_txns_;
  
  atomic<int> blocked;
  
  pthread_mutex_t deadlock_mutex_;
  
  Connection* distributed_deadlock_;
  AtomicQueue<MessageProto>* distributed_deadlock_queue;
  
  Connection* abort_txn_;
  AtomicQueue<MessageProto>* abort_txn_queue;
  
};
#endif  // _DB_SCHEDULER_DETERMINISTIC_SCHEDULER_H_
