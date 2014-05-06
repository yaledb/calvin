// Author: Kun Ren (kun.ren@yale.edu)
//         Alexander Thomson (thomson@cs.yale.edu)
// The Paxos object allows batches to be registered with a running zookeeper
// instance, inserting them into a globally consistent batch order.

#ifndef _DB_PAXOS_PAXOS_H_
#define _DB_PAXOS_PAXOS_H_

#include <zookeeper.h>

#include <map>
#include <string>

#include "common/types.h"

using std::map;
using std::string;

// The path of zookeeper config file
#define ZOOKEEPER_CONF "paxos/zookeeper.conf"

// Number of concurrently get batches from the zookeeper servers
// at any given time.
#define CONCURRENT_GETS 128

class Paxos {
 public:
  // Construct and initialize a Paxos object. Configuration of the associated
  // zookeeper instance is read from the file whose path is identified by
  // 'zookeeper_conf_file'. If 'reader' is not set to true, GetNextBatch may
  // never be called on this Paxos object.
  Paxos(const string& zookeeper_config_file, bool reader);

  // Deconstructor closes the connection with the zookeeper service.
  ~Paxos();

  // Sends a new batch to the associated zookeeper instance. Does NOT block.
  // The zookeeper service will create a new znode whose data is 'batch_data',
  // thus inserting the batch into the global order. Once a quorum of zookeeper
  // nodes have agreed on an insertion, it will appear in the same place in
  // the global order to all readers.
  void SubmitBatch(const string& batch_data);

  // Attempts to read the next batch in the global sequence into '*batch_data'.
  // Returns true on successful read of the next batch. Like SubmitBatch,
  // GetNextBatch does NOT block if the next batch is not immediately known,
  // but rather returns false immediately.
  bool GetNextBatch(string* batch_data);

  // Reads the next batch in the global sequence into '*batch_data'. If it is
  // not immediately known, GetNextBatchBlocking blocks until it is received.
  void GetNextBatchBlocking(string* batch_data);

 private:
  // The zookeeper handle obtained by a call to zookeeper_init.
  zhandle_t *zh_;

  // Record the serial number of the batch which will be read next time.
  uint64 next_read_batch_index_;

  // The mutex lock of every concurrent get(because the map container
  // is not thread safe).
  pthread_mutex_t mutexes_[CONCURRENT_GETS];

  // The map array save the batches which are concurrently got from zookeeper.
  map<uint64, string> batch_tables_[CONCURRENT_GETS];

  // For zoo_aget completion function, this method will be invoked
  // at the end of a asynchronous call( zoo_aget is asynchronous call
  // which get data from zookeeper).
  static void get_data_completion(int rc, const char *value, int value_len,
                                  const struct Stat *stat, const void *data);

  // For zoo_acreate completion function, this method will be invoked
  // at the end of zoo_acreate function.
  static void acreate_completion(int rc, const char *name, const void * data);
};
#endif  // _DB_PAXOS_PAXOS_H_
