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
#include <vector>
#include "applications/tpcc.h"
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

unordered_map<int, MessageProto*> batches[WorkersNumber];


DeterministicScheduler::DeterministicScheduler(Configuration* conf,
                                               ConnectionMultiplexer* multiplexer,
                                               Storage* storage[],
                                               const Application* application)
    : configuration_(conf), multiplexer_(multiplexer){
printf("Enter DeterministicScheduler\n");
   for (int i = 0; i  < WorkersNumber; i++) 
      storage_[i] = storage[i];
   application_ = application;
   for (int i = 0; i < WorkersNumber; i++) {
     string channel_batch("scheduler");
     channel_batch.append(IntToString(i));
     batch_connections_[i] = multiplexer_->NewConnection(channel_batch);
   }

  for (int i = 0; i < WorkersNumber; i++) {
    message_queues[i] = new AtomicQueue<MessageProto>();
  }

cpu_set_t cpuset;

   for (int i = 0; i  < WorkersNumber; i++) {
     string channel_worker("worker");
     channel_worker.append(IntToString(i));
     thread_connections_[i] = multiplexer_->NewConnection(channel_worker, &message_queues[i]);

pthread_attr_t attr;
pthread_attr_init(&attr);
CPU_ZERO(&cpuset);
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
}

DeterministicScheduler::~DeterministicScheduler() {
}

MessageProto* GetBatch(int partition_id, int batch_id, Connection* connection) {
  if (batches[partition_id % WorkersNumber].count(batch_id) > 0) {
    // Requested batch has already been received.
    MessageProto* batch = batches[partition_id % WorkersNumber][batch_id];
    batches[partition_id % WorkersNumber].erase(batch_id);
    return batch;
  } else {
    MessageProto* message = new MessageProto();
    while (connection->GetMessage(message)) {
      assert(message->type() == MessageProto::TXN_BATCH);
      if (message->batch_number() == batch_id) {
        return message;
      } else {
        batches[partition_id % WorkersNumber][message->batch_number()] = message;
        message = new MessageProto();
      }
    }
    delete message;
    return NULL;
  }
}

int Hash(const Key& key) {
    uint64 hash = 2166136261;
    for (size_t i = 0; i < key.size(); i++) {
      hash = hash ^ (key[i]);
      hash = hash * 16777619;
    }
    return hash % 1000000;
}
 
void* DeterministicScheduler::RunWorkerThread(void* arg) {

  int partition_id =
      reinterpret_cast<pair<int, DeterministicScheduler*>*>(arg)->first;
  DeterministicScheduler* scheduler =
      reinterpret_cast<pair<int, DeterministicScheduler*>*>(arg)->second;
      
  int thread = partition_id % WorkersNumber;

  MessageProto* batch_message = NULL;
  MessageProto message;
  int batch_offset = 0;
  int batch_number = 0;
  map<int64, TxnProto*> TxnsQueue;
  // The txn id of the current running txn
  int txns = 0;
  double time = GetTime();
  // double interal = GetTime();

  int add = 0;
  int idel = 0;

printf("%d:thread assigned to CPU %d\n",partition_id, sched_getcpu());

  // The active multi-partition txn
  unordered_map<string, StorageManager*> active_txns;
  Connection* this_connection = scheduler->thread_connections_[thread];
  Storage* this_storage = scheduler->storage_[thread];
  
  ValueStore* cache_objects_[20];
  // For SCA
  bitset<1000000> Dx;
  bitset<1000000> Ds;
  int blocked_txn = 0;
Spin(2);  
  while(true) {

    // First we check whether we can receive remote read
    bool got_message = scheduler->message_queues[thread]->Pop(&message);
    if (got_message == true) {
      assert(message.type() == MessageProto::READ_RESULT);
      StorageManager* manager = active_txns[message.destination_channel()];
      manager->HandleReadResult(message);
      if (manager->ReadyToExecute()) {
        TxnProto* txn = manager->txn_;
        scheduler->application_->Execute(txn, manager);
        if(rand() % txn->writers_size() == 0) {
          txns++;
        }
	delete manager;
	this_connection->UnlinkChannel(message.destination_channel());
  
        // Remove the txn from active_txns and wait_txns_ queue
        active_txns.erase(message.destination_channel());

        TxnsQueue.erase(txn->txn_id());

      }
    } else {
      if (batch_message == NULL) {
        batch_message = GetBatch(partition_id, batch_number, scheduler->batch_connections_[thread]);
      } else if (batch_offset >= batch_message->data_size()) {
        batch_offset = 0;
        batch_number++;
        delete batch_message;
        batch_message = GetBatch(partition_id, batch_number, scheduler->batch_connections_[thread]);
      } else {
        // Execute the front of wait_txns queue or execute the txn from the Batch
        // Check whether we could execute the head of wait_txns. 
        // (wait_txns is not empty and the front transaction is a BLOCKED transation)
        if (!TxnsQueue.empty() && TxnsQueue.begin()->second->status()== TxnProto::BLOCKED) {
          blocked_txn--;
          TxnProto* txn = TxnsQueue.begin()->second;
          int j = 0;

          for(int i = 0; i < txn->read_write_set_size(); i++) {
            if (scheduler->configuration_->LookupPartition(txn->read_write_set(i)) == partition_id) {
              cache_objects_[j++] = this_storage->ReadObject(txn->read_write_set(i));
            }
          }

          for(int i = 0; i < txn->read_set_size(); i++) {
            if (scheduler->configuration_->LookupPartition(txn->read_set(i)) == partition_id) {
              cache_objects_[j++] = this_storage->ReadObject(txn->read_set(i));
            }
          }

	        StorageManager* manager = new StorageManager(scheduler->configuration_,
                                                       this_connection,this_storage,
                                                       txn, partition_id,cache_objects_);
          if (manager->ReadyToExecute()) {
	          scheduler->application_->Execute(txn, manager);
            txns++;
            TxnsQueue.erase(txn->txn_id());
	          delete manager;
	        } else {
            string channel = IntToString(partition_id);
            channel.append(IntToString(txn->txn_id()));
	          this_connection->LinkChannel(channel);
	          active_txns[channel] = manager;
            txn->set_status(TxnProto::ACTIVE);
	        }
        } else if(blocked_txn <= 25){
          // If the number of BLOCKED transactions in the wait_txns queue is smaller than a Threshold, then execute the next transaction from batch 
	        TxnProto* txn = new TxnProto();
          txn->ParseFromString(batch_message->data(batch_offset));
          batch_offset++;
	        bool execute_now = true;
          int j = 0;

          // Check whether can acquire all locks
	        for(int i = 0; i < txn->read_write_set_size(); i++) {
	          if (scheduler->configuration_->LookupPartition(txn->read_write_set(i)) == partition_id) {
              cache_objects_[j] = this_storage->ReadObject(txn->read_write_set(i));
	            if(cache_objects_[j]->exclusive_lock_number != 0 || cache_objects_[j]->share_lock_number != 0) {
	              execute_now = false;
	            }
	            cache_objects_[j]->exclusive_lock_number++;
              j++;
	          }		  
	        }
	        
	        for (int i = 0; i < txn->read_set_size(); i++) {
	          if (scheduler->configuration_->LookupPartition(txn->read_set(i)) == partition_id) {
	            cache_objects_[j] = this_storage->ReadObject(txn->read_set(i));
	            if(cache_objects_[j]->exclusive_lock_number != 0) {
	              execute_now = false;
	            }
	            cache_objects_[j]->share_lock_number++;
              j++;
	          }
	        }
          
          // If can acquire all locks 
	        if (execute_now == true) {
            StorageManager* manager = new StorageManager(scheduler->configuration_,
                                                         this_connection,this_storage,
                                                         txn, partition_id,cache_objects_);
            // If the txn is single-partition txn
	          if (manager->ReadyToExecute()) {
              scheduler->application_->Execute(txn, manager);
              txns ++;
              delete manager;
	          } else {
              // IF the txn is a multi-partition txn, add it to the active_txns and wait_txn.
              string channel = IntToString(partition_id);
              channel.append(IntToString(txn->txn_id()));
	            this_connection->LinkChannel(channel);

		          active_txns[channel] = manager;
	            TxnsQueue.insert(std::pair<int64, TxnProto*>(txn->txn_id(), txn));
              txn->set_status(TxnProto::ACTIVE);
              
	          }
          } else {
            // Blocked txn, put it to wait_txns_ queue.
            blocked_txn++;
	          TxnsQueue.insert(std::pair<int64, TxnProto*>(txn->txn_id(), txn));
            txn->set_status(TxnProto::BLOCKED);
          }
          
          
        } else if(blocked_txn > 25) {
          // Do SCA optimization
          Dx.reset();
          Ds.reset();
          set<int64> erase_txns;
          int hash_index;
          
          for(std::map<int64, TxnProto*>::iterator it = TxnsQueue.begin(); it != TxnsQueue.end();it++) {
            TxnProto* txn = it->second;
          
            // If the BLOCKED txn can be executed now, just execute it.
            if(txn->status()== TxnProto::BLOCKED) {
              bool execute_now = true;
              
              // Check for conflicts in WriteSet
              for (int i = 0; i < txn->read_write_set_size(); i++) {
                if (scheduler->configuration_->LookupPartition(txn->read_write_set(i)) == partition_id) {
                  hash_index = Hash(txn->read_write_set(i));
                  if (Dx[hash_index] == 1 || Ds[hash_index] == 1) {
                    execute_now = false;
                  }
                  Dx[hash_index] = 1;
                }
              }
            
              // Check for conflicts in ReadSet
              for (int i = 0; i < txn->read_set_size(); i++) {
                if (scheduler->configuration_->LookupPartition(txn->read_set(i)) == partition_id) {
                  hash_index = Hash(txn->read_set(i));
                  if (Dx[hash_index] == 1) {
                    execute_now = false;
                  }
                  Ds[hash_index] = 1;
                }
              }
	            
              if(execute_now == true) {
                add++;
                blocked_txn--;
                int j = 0;
                
                for(int i = 0; i < txn->read_write_set_size(); i++) {
                  if (scheduler->configuration_->LookupPartition(txn->read_write_set(i)) == partition_id) {
                    cache_objects_[j++] = this_storage->ReadObject(txn->read_write_set(i));
                  }
                }
                
                for(int i = 0; i < txn->read_set_size(); i++) {
                  if (scheduler->configuration_->LookupPartition(txn->read_set(i)) == partition_id) {
                    cache_objects_[j++] = this_storage->ReadObject(txn->read_set(i));
                  }
                }
                
                StorageManager* manager = new StorageManager(scheduler->configuration_,
                                                             this_connection,this_storage,
                                                             txn, partition_id,cache_objects_);
                // If the txn is single-partition txn
	              if (manager->ReadyToExecute()) {
                  scheduler->application_->Execute(txn, manager);
                  txns ++;
                  delete manager;
                  erase_txns.insert(txn->txn_id());
	              } else {
                  string channel = IntToString(partition_id);
                  channel.append(IntToString(txn->txn_id()));
	                this_connection->LinkChannel(channel);
                  
                  // Add it to the active_txns
		              active_txns[channel] = manager;
                  txn->set_status(TxnProto::ACTIVE);
	              }
	            }                
            } else {
              // If the transaction is free, just mark the bit-array
              for (int i = 0; i < txn->read_write_set_size(); i++) {
                if (scheduler->configuration_->LookupPartition(txn->read_write_set(i)) == partition_id) {
                  hash_index = Hash(txn->read_write_set(i));
                  Dx[hash_index] = 1;
                }
              }
              for (int i = 0; i < txn->read_set_size(); i++) {
                if (scheduler->configuration_->LookupPartition(txn->read_set(i)) == partition_id) {
                  hash_index = Hash(txn->read_set(i));
                  Ds[hash_index] = 1;
                }
              } 
            }
          }

          // Delete the single-partition blocked txn(multi-partition txn is still in wait_txn queue) 
          for(set<int64>::iterator it = erase_txns.begin(); it != erase_txns.end();it++) {
            TxnsQueue.erase(*it);
          }
        } else {
          idel++;
        }
      }
    }
      if(GetTime() > time + 1) {
        double total_time = GetTime() - time;
        std::cout<<"Partition:"<<partition_id<<": Completed "<<(static_cast<double>(txns)/total_time)<<" txns/sec."<<(int)TxnsQueue.size()<<"  "<<(int)active_txns.size()<<"  "<<(static_cast<double>(add)/total_time)<<"  "<<(static_cast<double>(idel)/total_time)<<"\n"<<std::flush;
        time = GetTime();
        txns = 0;
        add = 0;
        idel = 0;
      }
  }
}
  
  
  
  
