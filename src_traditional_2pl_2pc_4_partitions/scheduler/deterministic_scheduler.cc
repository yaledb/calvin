// Author: Kun Ren (kun@cs.yale.edu)
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
#include <bitset>

#include "applications/application.h"
#include "common/utils.h"
#include "common/connection.h"
#include "backend/storage.h"
#include "backend/storage_manager.h"
#include "proto/message.pb.h"
#include "proto/txn.pb.h"
#include "scheduler/deterministic_lock_manager.h"

// XXX(scw): why the F do we include from a separate component
//           to get COLD_CUTOFF
#include "sequencer/sequencer.h"  // COLD_CUTOFF and buffers in LATENCY_TEST

using std::pair;
using std::string;
using std::tr1::unordered_map;
using std::bitset;


DeterministicScheduler::DeterministicScheduler(Configuration* conf,
                                               ConnectionMultiplexer* multiplexer,
                                               Storage* storage,
                                               const Application* application)
    : configuration_(conf), multiplexer_(multiplexer), storage_(storage){
   application_ = application;
pthread_mutex_init(&test_mutex_, NULL);
pthread_mutex_init(&batch_mutex_, NULL);   
   for (int i = 0; i < WorkersNumber; i++) {
     message_queues[i] = new AtomicQueue<MessageProto>();
   }
   
   batch_txns = new AtomicQueue<TxnProto*>();
   batch_id = 0;
     
     string channel_batch("scheduler_");
     batch_connections_ = multiplexer_->NewConnection(channel_batch);

cpu_set_t cpuset;

   for (int i = 0; i < WorkersNumber; i++) {
     string channel_worker("worker");
     channel_worker.append(IntToString(i));
     thread_connections_[i] = multiplexer_->NewConnection(channel_worker, &message_queues[i]);
CPU_ZERO(&cpuset);
pthread_attr_t attr;
pthread_attr_init(&attr);
if (i == 0 || i == 1)
CPU_SET(i, &cpuset);
else if (i == 2 || i == 3)
CPU_SET(i+2, &cpuset);
else
CPU_SET(7, &cpuset);
pthread_attr_setaffinity_np(&attr, sizeof(cpu_set_t), &cpuset);
     pthread_create(&(threads_[i]), &attr, RunWorkerThread, reinterpret_cast<void*>(
            new pair<int, DeterministicScheduler*>(configuration_->this_node_id * WorkersNumber + i, this)));
   }
   
   ready_txns_ = new AtomicQueue<TxnProto*>();
   lock_manager_ = new DeterministicLockManager(ready_txns_, configuration_);
   
  pthread_mutex_init(&deadlock_mutex_, NULL);
  
  if (configuration_->this_node_id == 0) {
    string channel_worker("wait_for_graph");
    distributed_deadlock_queue = new AtomicQueue<MessageProto>(); 
    distributed_deadlock_ = multiplexer_->NewConnection(channel_worker, &distributed_deadlock_queue);
  }
  
  string channel_worker("force_abort_unactive_txn");
  abort_txn_queue = new AtomicQueue<MessageProto>(); 
  abort_txn_ = multiplexer_->NewConnection(channel_worker, &abort_txn_queue);
}

DeterministicScheduler::~DeterministicScheduler() {
}

// Returns ptr to heap-allocated
unordered_map<int, MessageProto*> batches;
int last_txn = -1;;
void GetBatch(DeterministicScheduler* scheduler) {
  if(pthread_mutex_trylock(&scheduler->batch_mutex_) == 0) {
    MessageProto* message = new MessageProto();
    if (scheduler->batch_connections_->GetMessage(message)) {
      assert(message->type() == MessageProto::TXN_BATCH);
      batches[message->batch_number()] = message;
      if (batches.count(scheduler->batch_id) > 0) {
        message = batches[scheduler->batch_id];
        for (int i = 0; i < message->data_size(); i++) {
          TxnProto* txn = new TxnProto();
          txn->ParseFromString(message->data(i));
          scheduler->batch_txns->Push(txn);
          last_txn = txn->txn_id();
        }
        batches.erase(scheduler->batch_id);
        scheduler->batch_id ++;
        delete message;
      }
    } else {
      delete message;
    }
    
    pthread_mutex_unlock(&scheduler->batch_mutex_);
  }
}
  
void* DeterministicScheduler::RunWorkerThread(void* arg) {

  int partition_id =
      reinterpret_cast<pair<int, DeterministicScheduler*>*>(arg)->first;
  DeterministicScheduler* scheduler =
      reinterpret_cast<pair<int, DeterministicScheduler*>*>(arg)->second;

  int thread = partition_id % WorkersNumber;
  MessageProto message;

  int txns = 0;
  double start_time = GetTime();
  double time = GetTime();

  Storage* this_storage = scheduler->storage_;
  int this_node_id = scheduler->configuration_->this_node_id;

  unordered_map<string, StorageManager*> active_txns;
   
  Spin(2);
  
  while(true) {    
    bool got_message = scheduler->abort_txn_queue->Pop(&message);
    if (got_message == true) {
      TxnProto* txn = scheduler->lock_manager_->GetBlockedTxn(message.destination_channel());
      if (txn == NULL) {
        continue;
      }
      scheduler->lock_manager_->Release(txn, scheduler->thread_connections_[thread]);
      doing_deadlocks.erase(IntToString(txn->txn_id()));
      if (txn->txn_node() == this_node_id) {
        MessageProto restart;
        restart.set_destination_channel("sequencer");
        restart.set_destination_node(this_node_id);
        restart.set_type(MessageProto::TXN_RESTART);
        bytes txn_data;
        txn->clear_readers();
        txn->clear_writers();
        txn->SerializeToString(&txn_data);
        restart.add_data(txn_data);

        scheduler->thread_connections_[thread]->Send(restart);   
      }
      delete txn;
    }
  
    got_message = scheduler->message_queues[thread]->Pop(&message);
    if (got_message == true) {
      switch (message.type()) {      
        case MessageProto::TXN_ABORT:
        { 
          assert(active_txns.count(message.destination_channel())>0);
          StorageManager* manager = active_txns[message.destination_channel()];
          TxnProto* txn = manager->txn_;
          assert(txn != NULL); 
          active_txns.erase(message.destination_channel());
          scheduler->thread_connections_[thread]->UnlinkChannel(message.destination_channel());
          scheduler->lock_manager_->Release(txn, scheduler->thread_connections_[thread]);
          if (txn->txn_node() == this_node_id) {
            MessageProto restart;
            restart.set_destination_channel("sequencer");
            restart.set_destination_node(this_node_id);
            restart.set_type(MessageProto::TXN_RESTART);
            bytes txn_data;
            txn->clear_readers();
            txn->clear_writers();
            txn->SerializeToString(&txn_data);
            restart.add_data(txn_data);
            scheduler->thread_connections_[thread]->Send(restart);     
          }
          doing_deadlocks.erase(IntToString(txn->txn_id()));
          break;
        }

        case MessageProto::READ_RESULT:      
        {   
          if (active_txns.count(message.destination_channel()) == 0) {
            break;
          }
          assert(active_txns.count(message.destination_channel())>0);
          StorageManager* manager = active_txns[message.destination_channel()];
          assert(manager != NULL);
          manager->HandleReadResult(message);
          TxnProto* txn = manager->txn_;
          if (manager->ReadyToExecute()) {
            scheduler->application_->Execute(txn, manager);
            // If this node is the coordinator, broadcast prepared messages       
            if (txn->txn_node() == this_node_id) {
              manager->BroadcastPreparedMessages();
            }  
            if (manager->ReceivedPreparedMessages() == true) {
              manager->SendPreparedReplyMessages();
            }
          }
          break;
        }
      
        case MessageProto::PREPARED:
        {
          assert(active_txns.count(message.destination_channel())>0);
          StorageManager* manager = active_txns[message.destination_channel()];
          assert(manager != NULL);
          manager->HandlePreparedMessages(message);
          if (manager->ReadyToExecute()) {
            manager->SendPreparedReplyMessages();
          }
          break;
        }
      
        case MessageProto::PREPARED_REPLY:
        {
          assert(active_txns.count(message.destination_channel())>0);
          StorageManager* manager = active_txns[message.destination_channel()];
          assert(manager != NULL);
          manager->HandlePreparedReplyMessages(message);
          if (manager->ReceivedAllPreparedReplyMessages()) {
            manager->BroadcastCommitMessages();
          }
          break;
        }
        
        case MessageProto::COMMIT:
        {
          assert(active_txns.count(message.destination_channel())>0);
          StorageManager* manager = active_txns[message.destination_channel()];
          assert(manager != NULL);
          TxnProto* txn = manager->txn_;
          assert(txn != NULL);
          manager->HandleCommitMessages(message);
          manager->SendCommitReplyMessages();
          // Done the transaction
          active_txns.erase(message.destination_channel());
          scheduler->thread_connections_[thread]->UnlinkChannel(message.destination_channel());
          scheduler->lock_manager_->Release(txn, scheduler->thread_connections_[thread]);
          if((txn->writers_size() == 0) && (rand() % txn->readers_size() == 0)) {
            txns++;
          } else if(rand() % txn->writers_size() == 0) {
            txns++;
          }       
          delete txn;
          delete manager;
          break;
        }
  
        case MessageProto::COMMIT_REPLY:
        {
          assert(active_txns.count(message.destination_channel())>0);
          StorageManager* manager = active_txns[message.destination_channel()];
          assert(manager != NULL);
          TxnProto* txn = manager->txn_;
          manager->HandleCommitReplyMessages(message);
          if (manager->ReceivedAllCommitReplyMessages()) {
            // Done the transaction
            active_txns.erase(message.destination_channel());
            scheduler->thread_connections_[thread]->UnlinkChannel(message.destination_channel());
        
            scheduler->lock_manager_->Release(txn, scheduler->thread_connections_[thread]);
       
            if((txn->writers_size() == 0) && (rand() % txn->readers_size() == 0)) {
              txns++;
            } else if(rand() % txn->writers_size() == 0) {
              txns++;
            }         
            delete txn;
            delete manager;
          }
          break;
        }
        
      default:
        break;
      }

    }

if ((int)active_txns.size() < 100 && scheduler->lock_manager_->txn_waits_.size() < 40) {
   TxnProto* txn;
   bool got_message = scheduler->batch_txns->Pop(&txn);
   if (got_message == true) {
             if (doing_deadlocks.count(IntToString(txn->txn_id())) > 0) {
            doing_deadlocks.erase(IntToString(txn->txn_id()));
            continue;
          }

      int ret = scheduler->lock_manager_->Lock(txn, scheduler->thread_connections_[thread]);
      // Have acquired all locks, execute the txn now.
      if(ret == 0) {

        StorageManager* manager = new StorageManager(scheduler->configuration_,
                                    scheduler->thread_connections_[thread],
                                    this_storage, txn, this_node_id); 

        if (manager->ReadyToExecute()) {
          // Execute the txn.
          scheduler->application_->Execute(txn, manager);
          
          // If don't happen deadlocks, just release all locks
          scheduler->lock_manager_->Release(txn, scheduler->thread_connections_[thread]);         
          delete manager;
          delete txn;
          txns++;
        } else {
          string channel = IntToString(txn->txn_id());
          scheduler->thread_connections_[thread]->LinkChannel(channel);
          active_txns[channel] = manager;
        }

     } else if (ret == -1) {
        scheduler->lock_manager_->Release(txn, scheduler->thread_connections_[thread]);   
        if (txn->multipartition() == false) {
        // Single-partition txn
          MessageProto restart;
          restart.set_destination_channel("sequencer");
          restart.set_destination_node(this_node_id);
          restart.set_type(MessageProto::TXN_RESTART);
          bytes txn_data;
          txn->clear_readers();
          txn->clear_writers();
          txn->SerializeToString(&txn_data);
          restart.add_data(txn_data); 
          scheduler->thread_connections_[thread]->Send(restart);
        } else if (txn->multipartition() == true) {
        // Multi-partition txn
          if (txn->txn_node() == this_node_id) {
            for (int i = 0; i < txn->txn_other_node_size(); i++) {
              MessageProto message;
              message.set_type(MessageProto::TXN_ABORT);
              message.set_destination_channel(IntToString(txn->txn_id()));
              message.set_destination_node(txn->txn_other_node(i));
              scheduler->thread_connections_[thread]->Send1(message);  
            }
            
            MessageProto restart;
            restart.set_destination_channel("sequencer");
            restart.set_destination_node(this_node_id);
            restart.set_type(MessageProto::TXN_RESTART);
            bytes txn_data;
            txn->clear_readers();
            txn->clear_writers();
            txn->SerializeToString(&txn_data);
            restart.add_data(txn_data);

            scheduler->thread_connections_[thread]->Send(restart);
          } else {
            MessageProto message;
            message.set_type(MessageProto::TXN_ABORT);
            message.set_destination_channel(IntToString(txn->txn_id()));
            message.set_destination_node(txn->txn_node());
            scheduler->thread_connections_[thread]->Send1(message);
            
            for (int i = 0; i < txn->txn_other_node_size(); i++) {
              if (txn->txn_other_node(i) != this_node_id) {
                MessageProto message;
                message.set_type(MessageProto::TXN_ABORT);
                message.set_destination_channel(IntToString(txn->txn_id()));
                message.set_destination_node(txn->txn_other_node(i));
                scheduler->thread_connections_[thread]->Send1(message);
              }
            }
          }
        
        }
     }
  }
}


      if (!scheduler->ready_txns_->Empty()) {
        TxnProto* txn; 
        bool got_it = scheduler->ready_txns_->Pop(&txn);
        if (got_it == true) {
          if (doing_deadlocks.count(IntToString(txn->txn_id())) > 0) {
            doing_deadlocks.erase(IntToString(txn->txn_id()));
            continue;
          }
          int ret = scheduler->lock_manager_->ContinueLock(txn, scheduler->thread_connections_[thread]);
          if (ret == 0) {  
            StorageManager* manager = new StorageManager(scheduler->configuration_,
                                          scheduler->thread_connections_[thread],
                                          this_storage, txn, this_node_id);
            if (manager->ReadyToExecute()) {
              // Execute the txn.
              scheduler->application_->Execute(txn, manager);
          
              // If don't happen deadlocks, just release all locks
              scheduler->lock_manager_->Release(txn, scheduler->thread_connections_[thread]);
          
              delete manager;
              delete txn;
              txns++;
            } else {
              string channel = IntToString(txn->txn_id());
              scheduler->thread_connections_[thread]->LinkChannel(channel);
              active_txns[channel] = manager;
            }
          } else if (ret == -1) {
            scheduler->lock_manager_->Release(txn, scheduler->thread_connections_[thread]);
            if (txn->multipartition() == false) { 
              // Single-partition txn
              MessageProto restart;
              restart.set_destination_channel("sequencer");
              restart.set_destination_node(this_node_id);
              restart.set_type(MessageProto::TXN_RESTART);
              bytes txn_data;
              txn->clear_readers();
              txn->clear_writers();
              txn->SerializeToString(&txn_data);
              restart.add_data(txn_data); 
              scheduler->thread_connections_[thread]->Send(restart);
            } else if (txn->multipartition() == true) {
              // Multi-partition txn
              if (txn->txn_node() == this_node_id) {
                for (int i = 0; i < txn->txn_other_node_size(); i++) {
                  MessageProto message;
                  message.set_type(MessageProto::TXN_ABORT);
                  message.set_destination_channel(IntToString(txn->txn_id()));
                  message.set_destination_node(txn->txn_other_node(i));
                  scheduler->thread_connections_[thread]->Send1(message);  
                }
           
                MessageProto restart;
                restart.set_destination_channel("sequencer");
                restart.set_destination_node(this_node_id);
                restart.set_type(MessageProto::TXN_RESTART);
                bytes txn_data;
                txn->clear_readers();
                txn->clear_writers();
                txn->SerializeToString(&txn_data);
                restart.add_data(txn_data);
                scheduler->thread_connections_[thread]->Send(restart);
              } else {
                MessageProto message;
                message.set_type(MessageProto::TXN_ABORT);
                message.set_destination_channel(IntToString(txn->txn_id()));
                message.set_destination_node(txn->txn_node());
                scheduler->thread_connections_[thread]->Send1(message);
            
                for (int i = 0; i < txn->txn_other_node_size(); i++) {
                  if (txn->txn_other_node(i) != this_node_id) {
                    MessageProto message;
                    message.set_type(MessageProto::TXN_ABORT);
                    message.set_destination_channel(IntToString(txn->txn_id()));
                    message.set_destination_node(txn->txn_other_node(i));
                    scheduler->thread_connections_[thread]->Send1(message);
                  }
                }
              }
            }
          }
        }
      }

      if (this_node_id == 0 && scheduler->lock_manager_->try_deadlock_mutex() == 0) {
        got_message = scheduler->distributed_deadlock_queue->Pop(&message);
        if (got_message == true) {
          scheduler->lock_manager_->HandleDistributedDeadlock(message, scheduler->thread_connections_[thread]);
          scheduler->lock_manager_->release_deadlock_mutex();
        } else {
          scheduler->lock_manager_->release_deadlock_mutex();
        }
      }
      
      if (txns % 10 == 0) {
        GetBatch(scheduler);
      }
   
    if(GetTime() > time + 1) {
      double total_time = GetTime() - time;
      std::cout<<"Partition "<<partition_id<<" Completed "<<(static_cast<double>(txns)/total_time)<<" txns/sec. "<<(int)active_txns.size()<<"  "<<scheduler->lock_manager_->txn_waits_.size()<<"\n"<<std::flush;
      time = GetTime();
      txns = 0;
    }

    if (GetTime() > start_time + 240)
      exit(0);
}
  
  }
  
  
