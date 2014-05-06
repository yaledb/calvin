// Author: Kun Ren (kun@cs.yale.edu)
//
// Lock manager implementing deterministic two-phase locking as described in
// 'The Case for Determinism in Database Systems'.

#include <cstdlib>
#include <iostream>

#include "scheduler/deterministic_lock_manager.h"
#include "sequencer/sequencer.h"

#include <vector>

#include "proto/txn.pb.h"
#include "proto/message.pb.h"

using std::vector;

typedef DeterministicLockManager::Latch Latch;

DeterministicLockManager::DeterministicLockManager(AtomicQueue<TxnProto*>* ready_txns, Configuration* config)
  : configuration_(config), ready_txns_(ready_txns) {
  for (int i = 0; i < TABLE_SIZE; i++)
    lock_table_[i] = new deque<KeysList>();
  latches_ = new Latch[TABLE_SIZE];
  cond = new pthread_cond_t[WorkersNumber];
  mutex = new pthread_mutex_t[WorkersNumber];
  for(int i = 0; i < WorkersNumber; i++) {
    pthread_cond_init(&cond[i], NULL);
    pthread_mutex_init(&mutex[i], NULL);
  }
  pthread_mutex_init(&new_mutex_, NULL);
  pthread_mutex_init(&distributed_deadlock_mutex_, NULL);  
}

Latch* DeterministicLockManager::LatchFor(const int& index) {
  return latches_ + index;
}

int DeterministicLockManager::Lock(TxnProto* txn, Connection* connection) {
  int not_acquired = 0;

  // Handle read/write lock requests.
  for (int i = 0; i < txn->read_write_set_size(); i++) {
    // Only lock local keys.
    if (IsLocal(txn->read_write_set(i))) {
      int hash_index = Hash(txn->read_write_set(i));
      // Latch acquisitions 
      Latch* latch = LatchFor(hash_index);  
      pthread_mutex_lock(&latch->lock_);

      deque<KeysList>* key_requests = lock_table_[hash_index];
      
      deque<KeysList>::iterator it;
      for(it = key_requests->begin();
          it != key_requests->end() && it->key != txn->read_write_set(i); ++it) { 
      }
      deque<LockRequest>* requests;
      if (it == key_requests->end()) {
        requests = new deque<LockRequest>();
        key_requests->push_back(KeysList(txn->read_write_set(i), requests));
      } else {
        requests = it->locksrequest;
      }
      
      // Only need to request this if lock txn hasn't already requested it.
      if (requests->empty() || txn != requests->back().txn) {
        requests->push_back(LockRequest(WRITE, txn));
        // Write lock request fails if there is any previous request at all.
        if (requests->size() > 1) {
          not_acquired++;
          pthread_mutex_lock(&new_mutex_);
          txn_waits_[txn] = txn->read_write_set(i);
          deque<LockRequest>::iterator it = requests->begin() + requests->size() - 2;             
          wait_for_graph[txn->txn_id()] = it->txn->txn_id();
          // Check whether there is a local deadlock       
          bool deadlock = CheckDeadlock(txn);
          if (deadlock == true) {
            not_acquired = -1;
            txn_waits_.erase(txn);
            wait_for_graph.erase(txn->txn_id());
std::cout<<"~~~~~~ Find a local deadlock, txn id is "<<txn->txn_id()<<" related to "<<it->txn->txn_id()<<" \n"<<std::flush;
          }  else {
          // If not a local deadlock, send the wait-for lines to the global wait_for_graphs(that located at machine ID 0)
            MessageProto message;
            message.set_type(MessageProto::WAIT_FOR_GRAPH);
            message.set_destination_channel("wait_for_graph");
            message.set_destination_node(0);
            message.set_source_node(configuration_->this_node_id);
            message.add_wait_txns(it->txn->txn_id());
            bytes txn_data;
            txn->SerializeToString(&txn_data);
            message.add_data(txn_data); 
            message.set_lock_or_release(0);
            if (configuration_->this_node_id == 0)
              connection->Send(message);
            else
              connection->Send1(message);    
          } 
          pthread_mutex_unlock(&new_mutex_);  
          pthread_mutex_unlock(&latch->lock_);
          return not_acquired;
        }
      }
      pthread_mutex_unlock(&latch->lock_);
    }
  }

  // Handle read lock requests. This is last so that we don't have to deal with
  // upgrading lock requests from read to write on hash collisions.
  for (int i = 0; i < txn->read_set_size(); i++) {
    // Only lock local keys.
    if (IsLocal(txn->read_set(i))) {
      int hash_index = Hash(txn->read_set(i));
      Latch* latch = LatchFor(hash_index);  
      pthread_mutex_lock(&latch->lock_);

      deque<KeysList>* key_requests = lock_table_[hash_index];
      
      deque<KeysList>::iterator it;
      for(it = key_requests->begin();
          it != key_requests->end() && it->key != txn->read_set(i); ++it) { 
      }
      deque<LockRequest>* requests;
      if (it == key_requests->end()) {
        requests = new deque<LockRequest>();
        key_requests->push_back(KeysList(txn->read_set(i), requests));
      } else {
        requests = it->locksrequest;
      }
      
      // Only need to request this if lock txn hasn't already requested it.
      if (requests->empty() || txn != requests->back().txn) {
        requests->push_back(LockRequest(READ, txn));
        // Read lock request fails if there is any previous write request.
        for (deque<LockRequest>::iterator it = requests->begin();
             it != requests->end(); ++it) {
          if (it->mode == WRITE) {
            not_acquired++;           
            pthread_mutex_lock(&new_mutex_);
            txn_waits_[txn] = txn->read_set(i);
            deque<LockRequest>::iterator it = requests->begin() + requests->size() - 2;     
            wait_for_graph[txn->txn_id()] = it->txn->txn_id();         
            bool deadlock = CheckDeadlock(txn);
            if (deadlock == true) {
              not_acquired = -1;
              txn_waits_.erase(txn);
              wait_for_graph.erase(txn->txn_id());
std::cout<<"~~~~~~ Find a local deadlock, txn id is "<<txn->txn_id()<<" related to "<<it->txn->txn_id()<<" \n"<<std::flush;
            } else {
              MessageProto message;
              message.set_type(MessageProto::WAIT_FOR_GRAPH);
              message.set_destination_channel("wait_for_graph");
              message.set_destination_node(0);
              message.set_source_node(configuration_->this_node_id);
              message.add_wait_txns(it->txn->txn_id());
              bytes txn_data;
              txn->SerializeToString(&txn_data);
              message.add_data(txn_data); 
              message.set_lock_or_release(0);
              if (configuration_->this_node_id == 0)
                connection->Send(message);
              else
                connection->Send1(message); 
            }   
            pthread_mutex_unlock(&new_mutex_);  
            pthread_mutex_unlock(&latch->lock_);
            return not_acquired;
          }
        }
      }
      pthread_mutex_unlock(&latch->lock_);
    }
  }

  // Record and return the number of locks that the txn is blocked on.
  return not_acquired;
}

int DeterministicLockManager::ContinueLock(TxnProto* txn, Connection* connection) {
  int not_acquired = 0;
  bool find_point = false;
  pthread_mutex_lock(&new_mutex_);
  Key point_key = txn_waits_[txn];
  pthread_mutex_unlock(&new_mutex_);

  // Handle read/write lock requests.
  for (int i = 0; i < txn->read_write_set_size(); i++) {
    // Only lock local keys.
    if (IsLocal(txn->read_write_set(i))) {
      if (find_point == false) {
        if (txn->read_write_set(i) == point_key) {
          find_point = true;
          continue;
        } else {
          continue;
        }
      }
      
      int hash_index = Hash(txn->read_write_set(i));
      Latch* latch = LatchFor(hash_index);  
      pthread_mutex_lock(&latch->lock_);

      deque<KeysList>* key_requests = lock_table_[hash_index];
      
      deque<KeysList>::iterator it;
      for(it = key_requests->begin();
          it != key_requests->end() && it->key != txn->read_write_set(i); ++it) { 
      }
      deque<LockRequest>* requests;
      if (it == key_requests->end()) {
        requests = new deque<LockRequest>();
        key_requests->push_back(KeysList(txn->read_write_set(i), requests));
      } else {
        requests = it->locksrequest;
      }
      
      // Only need to request this if lock txn hasn't already requested it.
      if (requests->empty() || txn != requests->back().txn) {
        requests->push_back(LockRequest(WRITE, txn));
        // Write lock request fails if there is any previous request at all.
        if (requests->size() > 1) {
          not_acquired++;
          pthread_mutex_lock(&new_mutex_);
          txn_waits_[txn] = txn->read_write_set(i);
          deque<LockRequest>::iterator it = requests->begin() + requests->size() - 2;
          wait_for_graph[txn->txn_id()] = it->txn->txn_id();   

          bool deadlock = CheckDeadlock(txn);
          if (deadlock == true) {
            not_acquired = -1;
            txn_waits_.erase(txn);
            wait_for_graph.erase(txn->txn_id());
std::cout<<"~~~~~~ Find a local deadlock, txn id is "<<txn->txn_id()<<" related to "<<it->txn->txn_id()<<" \n"<<std::flush;
          } else {
            MessageProto message;
            message.set_type(MessageProto::WAIT_FOR_GRAPH);
            message.set_destination_channel("wait_for_graph");
            message.set_destination_node(0);
            message.set_source_node(configuration_->this_node_id);
            message.add_wait_txns(it->txn->txn_id());
            bytes txn_data;
            txn->SerializeToString(&txn_data);
            message.add_data(txn_data); 
            message.set_lock_or_release(0);
            if (configuration_->this_node_id == 0)
              connection->Send(message);
            else
              connection->Send1(message);
          }
          
          pthread_mutex_unlock(&new_mutex_);
          pthread_mutex_unlock(&latch->lock_);
          return not_acquired;
        }
      }
      pthread_mutex_unlock(&latch->lock_);
    }
  }

  // Handle read lock requests. This is last so that we don't have to deal with
  // upgrading lock requests from read to write on hash collisions.
  for (int i = 0; i < txn->read_set_size(); i++) {
    // Only lock local keys.
    if (IsLocal(txn->read_set(i))) {
      if (find_point == false) {
        if (txn->read_set(i) == point_key) {
          find_point = true;
          continue;
        } else {
          continue;
        }
      }
      
      int hash_index = Hash(txn->read_set(i));
      Latch* latch = LatchFor(hash_index);  
      pthread_mutex_lock(&latch->lock_);

      deque<KeysList>* key_requests = lock_table_[hash_index];
      
      deque<KeysList>::iterator it;
      for(it = key_requests->begin();
          it != key_requests->end() && it->key != txn->read_set(i); ++it) { 
      }
      deque<LockRequest>* requests;
      if (it == key_requests->end()) {
        requests = new deque<LockRequest>();
        key_requests->push_back(KeysList(txn->read_set(i), requests));
      } else {
        requests = it->locksrequest;
      }
      
      // Only need to request this if lock txn hasn't already requested it.
      if (requests->empty() || txn != requests->back().txn) {
        requests->push_back(LockRequest(READ, txn));
        // Read lock request fails if there is any previous write request.
        for (deque<LockRequest>::iterator it = requests->begin();
             it != requests->end(); ++it) {
          if (it->mode == WRITE) {
            not_acquired++;
            pthread_mutex_lock(&new_mutex_);
            txn_waits_[txn] = txn->read_set(i);
            deque<LockRequest>::iterator it = requests->begin() + requests->size() - 2;
            wait_for_graph[txn->txn_id()] = it->txn->txn_id();

            bool deadlock = CheckDeadlock(txn);
            if (deadlock == true) {
              not_acquired = -1;
              txn_waits_.erase(txn);
              wait_for_graph.erase(txn->txn_id());
std::cout<<"~~~~~~ Find a local deadlock, txn id is "<<txn->txn_id()<<" related to "<<it->txn->txn_id()<<" \n"<<std::flush;
            } else {
              MessageProto message;
              message.set_type(MessageProto::WAIT_FOR_GRAPH);
              message.set_destination_channel("wait_for_graph");
              message.set_destination_node(0);
              message.set_source_node(configuration_->this_node_id);
              message.add_wait_txns(it->txn->txn_id());
              bytes txn_data;
              txn->SerializeToString(&txn_data);
              message.add_data(txn_data); 
              message.set_lock_or_release(0);
              if (configuration_->this_node_id == 0)
                connection->Send(message);
              else
                connection->Send1(message);
            }
          
            pthread_mutex_unlock(&new_mutex_);
            pthread_mutex_unlock(&latch->lock_);
            return not_acquired;
          }
        }
      }
      pthread_mutex_unlock(&latch->lock_);
    }
  }

  if (not_acquired == 0) {
    pthread_mutex_lock(&new_mutex_);
    txn_waits_.erase(txn);
    pthread_mutex_unlock(&new_mutex_);
  }
  // Record and return the number of locks that the txn is blocked on.
  return not_acquired;
}

void DeterministicLockManager::Release(TxnProto* txn, Connection* connection) {
  for (int i = 0; i < txn->read_set_size(); i++)
    if (IsLocal(txn->read_set(i)))
      Release(txn->read_set(i), txn, connection);

  for (int i = 0; i < txn->read_write_set_size(); i++)
    if (IsLocal(txn->read_write_set(i)))
      Release(txn->read_write_set(i), txn, connection);
}

void DeterministicLockManager::Release(const Key& key, TxnProto* txn, Connection* connection) {
  int hash_index = Hash(key);
  Latch* latch = LatchFor(hash_index);  
  pthread_mutex_lock(&latch->lock_);
  // Avoid repeatedly looking up key in the unordered_map.
  deque<KeysList>* key_requests = lock_table_[hash_index];
      
  deque<KeysList>::iterator it1;
  for(it1 = key_requests->begin();
    it1 != key_requests->end() && it1->key != key; ++it1) { 
  }
  
  if (it1 == key_requests->end()) {
    pthread_mutex_unlock(&latch->lock_);
    return ;
  }

  deque<LockRequest>* requests = it1->locksrequest;

  // Seek to the target request. Note whether any write lock requests precede
  // the target.
  bool write_requests_precede_target = false;
  deque<LockRequest>::iterator it;
  for (it = requests->begin();
       it != requests->end() && it->txn != txn; ++it) {
    if (it->mode == WRITE)
      write_requests_precede_target = true;
  }

  // If we found the request, erase it. No need to do anything otherwise.
  if (it != requests->end()) {
    // Save an iterator pointing to the target to call erase on after handling
    // lock inheritence, since erase(...) trashes all iterators.
    deque<LockRequest>::iterator target = it;

    // If there are more requests following the target request, one or more
    // may need to be granted as a result of the target's release.
    ++it;
    if (it != requests->end()) {
      vector<TxnProto*> new_owners;
      // Grant subsequent request(s) if:
      //  (a) The canceled request held a write lock.
      //  (b) The canceled request held a read lock ALONE.
      //  (c) The canceled request was a write request preceded only by read
      //      requests and followed by one or more read requests.
      if (target == requests->begin() &&
          (target->mode == WRITE ||
           (target->mode == READ && it->mode == WRITE))) {  // (a) or (b)
        // If a write lock request follows, grant it.
        if (it->mode == WRITE)
          new_owners.push_back(it->txn);
        // If a sequence of read lock requests follows, grant all of them.
        for (; it != requests->end() && it->mode == READ; ++it)
          new_owners.push_back(it->txn);
      } else if (!write_requests_precede_target &&
                 target->mode == WRITE && it->mode == READ) {  // (c)
        // If a sequence of read lock requests follows, grant all of them.
        for (; it != requests->end() && it->mode == READ; ++it)
          new_owners.push_back(it->txn);
      }

      // Handle txns with newly granted requests that may now be ready to run.
      pthread_mutex_lock(&new_mutex_);
      for (uint64 j = 0; j < new_owners.size(); j++) {
          // The txn that just acquired the released lock is no longer waiting
          // on any lock requests.
          if (doing_deadlocks.count(IntToString(new_owners[j]->txn_id())) == 0) {
            ready_txns_->Push(new_owners[j]);
          }
            
          if (wait_for_graph.count(new_owners[j]->txn_id()) > 0)
            wait_for_graph.erase(new_owners[j]->txn_id());
          
          // Send message to machine 0, to delete blocked information from global wait for graph.
            MessageProto message;
            message.set_type(MessageProto::WAIT_FOR_GRAPH);
            message.set_destination_channel("wait_for_graph");
            message.set_destination_node(0);
            message.set_source_node(configuration_->this_node_id);
            message.add_wait_txns(new_owners[j]->txn_id());
            message.add_wait_txns(txn->txn_id());
            message.set_lock_or_release(1);
            if (configuration_->this_node_id == 0)
              connection->Send(message);
            else
              connection->Send1(message);

      }
      pthread_mutex_unlock(&new_mutex_);
      
    }

    // Now it is safe to actually erase the target request.
    requests->erase(target);
    if (requests->size() == 0) {
      delete requests;
      key_requests->erase(it1);    
    }
  }
  pthread_mutex_unlock(&latch->lock_);
}

// Local deadlock detection
bool DeterministicLockManager::CheckDeadlock(TxnProto* txn) {
  bool deadlock = false;
  int64 next_txn = wait_for_graph[txn->txn_id()];
  while (true) {
    if (wait_for_graph.count(next_txn) == 0) {
      deadlock = false;
      break;
    } else if (wait_for_graph[next_txn] == txn->txn_id()) {
      deadlock = true;
      break;
    }   
    next_txn = wait_for_graph[next_txn];
  }
  
  return deadlock;
}

// Distributed deadlock detection
bool DeterministicLockManager::CheckDistributedDeadlock(int64 txn1_id, int64 txn2_id) {
  bool deadlock = false;
  
  vector<int64> aa = distributed_wait_for_graph[txn2_id];
set<int64> tt;  
    for (int i = 0; i < (int)aa.size(); i ++) {
      int64 bb = aa[i];
      if (bb == txn1_id) {
        deadlock = true;
        return deadlock;
      }
      
      if (distributed_wait_for_graph.count(bb) > 0 && tt.count(bb) == 0) {
        deadlock = CheckDistributedDeadlock(txn1_id, bb);
        if (deadlock == true) {
          return deadlock;
        }
        tt.insert(bb);
      }
    }
  
  return deadlock;
}

void DeterministicLockManager::HandleDistributedDeadlock(MessageProto message, Connection* connection) {

  int lock_or_release = message.lock_or_release();
  
  if (lock_or_release == 0) {
    TxnProto* txn1 = new TxnProto();
    txn1->ParseFromString(message.data(0));
    int64 txn1_id = txn1->txn_id();
    int64 txn2_id = message.wait_txns(0);
    
    if (distributed_wait_for_graph.count(txn1_id) > 0) {
      vector<int64> aa = distributed_wait_for_graph[txn1_id];
      for(int i = 0; i < (int)aa.size(); i++) {
        if (aa[i] == txn2_id) {
          distributed_wait_for_graph[txn1_id].push_back(txn2_id);
          return;
        }
      }
    }
    
    distributed_wait_for_graph[txn1_id].push_back(txn2_id);    
    bool deadlock = CheckDistributedDeadlock(txn1_id, txn2_id);
    if (deadlock == true) {

      vector<int64> aa = distributed_wait_for_graph[txn1_id];
      for (int i = 0; i < (int)aa.size(); i++) {
        if (aa[i] == txn2_id) {
          distributed_wait_for_graph[txn1_id].erase(distributed_wait_for_graph[txn1_id].begin() + i); 
        }
      }
      if (distributed_wait_for_graph[txn1_id].size() == 0) {
        distributed_wait_for_graph.erase(txn1_id); 
      }
    
std::cout<<"!!!!!!: find a distributed deadlock, txn is "<<txn1_id<<" related to "<<txn2_id<<" \n"<<std::flush;
      MessageProto message1;
      message1.set_type(MessageProto::TXN_ABORT);
      message1.set_destination_channel(IntToString(txn1_id));
      message1.set_destination_node(txn1->txn_node());
      connection->Send(message1);
      
      for (int i = 0; i < txn1->txn_other_node_size(); i++) {
        MessageProto message2;
        message2.set_type(MessageProto::TXN_ABORT);
        message2.set_destination_channel(IntToString(txn1_id));
        message2.set_destination_node(txn1->txn_other_node(i));
        connection->Send(message2);
      }

    } 
  } else {
    int64 txn1_id = message.wait_txns(0);
    int64 txn2_id = message.wait_txns(1);
    if (distributed_wait_for_graph.count(txn1_id) > 0) {
      vector<int64> aa = distributed_wait_for_graph[txn1_id];
      for (int i = 0; i < (int)aa.size(); i++) {
        if (aa[i] == txn2_id) {
          distributed_wait_for_graph[txn1_id].erase(distributed_wait_for_graph[txn1_id].begin() + i);
          break;
        }
      }
      if (distributed_wait_for_graph[txn1_id].size() == 0) {
        distributed_wait_for_graph.erase(txn1_id); 
      }
    }
  }
}

int DeterministicLockManager::try_deadlock_mutex() {
  int ret = pthread_mutex_trylock(&distributed_deadlock_mutex_);
  return ret;
}

void DeterministicLockManager::release_deadlock_mutex() {
  pthread_mutex_unlock(&distributed_deadlock_mutex_);
}

TxnProto* DeterministicLockManager::GetBlockedTxn(string txn_id) {
  pthread_mutex_lock(&new_mutex_);
  TxnProto* txn = NULL;
  for (unordered_map<TxnProto*, Key>::iterator it = txn_waits_.begin();
       it != txn_waits_.end(); ++it) {
    if (it->first->txn_id() == StringToInt(txn_id)) {
      txn = it->first;
      break;
    }
  }

  if (txn != NULL) {
    txn_waits_.erase(txn);
    wait_for_graph.erase(txn->txn_id());  
  }

  pthread_mutex_unlock(&new_mutex_);
  return txn;
}



