// Author: Alexander Thomson (thomson@cs.yale.edu)
//
// A wrapper for a storage layer that can be used by an Application to simplify
// application code by hiding all inter-node communication logic. By using this
// class as the primary interface for applications to interact with storage of
// actual data objects, applications can be written without paying any attention
// to partitioning at all.
//
// StorageManager use:
//  - Each transaction execution creates a new StorageManager and deletes it
//    upon completion.
//  - No ReadObject call takes as an argument any value that depends on the
//    result of a previous ReadObject call.
//  - In any transaction execution, a call to DoneReading must follow ALL calls
//    to ReadObject and must precede BOTH (a) any actual interaction with the
//    values 'read' by earlier calls to ReadObject and (b) any calls to
//    PutObject or DeleteObject.

#ifndef _DB_BACKEND_STORAGE_MANAGER_H_
#define _DB_BACKEND_STORAGE_MANAGER_H_

#include <ucontext.h>

#include <tr1/unordered_map>
#include <vector>

#include "common/types.h"
#include "backend/storage.h"

using std::vector;
using std::tr1::unordered_map;

class Configuration;
class Connection;
class MessageProto;
class Scheduler;
class Storage;
class TxnProto;

class StorageManager {
 public:
  // TODO(alex): Document this class correctly.
  StorageManager(Configuration* config, Connection* connection,
                 Storage* actual_storage, TxnProto* txn, int partition_id);

  ~StorageManager();

  Value* ReadObject(const Key& key);
  bool PutObject(const Key& key, Value* value);
  bool DeleteObject(const Key& key);

  void HandleReadResult(const MessageProto& message);
  bool ReadyToExecute();

  Storage* GetStorage() { return actual_storage_; }

  bool ReceivedAllPreparedReplyMessages();
  void BroadcastPreparedMessages();
  void HandlePreparedReplyMessages(const MessageProto& message);
  bool ReceivedPreparedMessages();
  void HandlePreparedMessages(const MessageProto& message);
  void SendPreparedReplyMessages();

  bool ReceivedAllCommitReplyMessages();
  void BroadcastCommitMessages();
  void HandleCommitReplyMessages(const MessageProto& message);
  bool ReceivedCommitMessages();
  void HandleCommitMessages(const MessageProto& message);
  void SendCommitReplyMessages();

  // Set by the constructor, indicating whether 'txn' involves any writes at
  // this node.
  bool writer;

// private:
  friend class DeterministicScheduler;

  // Pointer to the configuration object for this node.
  Configuration* configuration_;

  // A Connection object that can be used to send and receive messages.
  Connection* connection_;

  // Storage layer that *actually* stores data objects on this node.
  Storage* actual_storage_;

  // Transaction that corresponds to this instance of a StorageManager.
  TxnProto* txn_;

  int partition_id_;

  // Local copy of all data objects read/written by 'txn_', populated at
  // StorageManager construction time.
  //
  // TODO(alex): Should these be pointers to reduce object copying overhead?
  unordered_map<Key, Value*> objects_;

  vector<Value*> remote_reads_;

  int received_prepared_reply_number;
  
  bool received_prepared_message;

  int received_commit_reply_number;

  bool received_commit_message;


};

#endif  // _DB_BACKEND_STORAGE_MANAGER_H_

