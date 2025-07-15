// Author: Philip Shao (philip.shao@yale.edu)
//
// An implementation of the storage interface taking into account
// main memory, disk, and swapping algorithms.

#ifndef _DB_BACKEND_FETCHING_STORAGE_H_
#define _DB_BACKEND_FETCHING_STORAGE_H_

#include <aio.h>
#include <pthread.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <cassert>
#include <cstdlib>
#include <cstring>
#include <string>

#include "common/utils.h"
#include "backend/storage.h"
#include "backend/simple_storage.h"

#define PAGE_SIZE 1000
#define STORAGE_PATH "../db/storage/"

#define COLD_CUTOFF 990000

class FetchingStorage : public Storage {
 public:
  static FetchingStorage* BuildStorage();
  ~FetchingStorage();
  virtual Value* ReadObject(const Key& key, int64 txn_id = 0);
  virtual bool PutObject(const Key& key, Value* value, int64 txn_id = 0);
  virtual bool DeleteObject(const Key& key, int64 txn_id = 0);
  virtual bool Prefetch(const Key &key, double* wait_time);
  virtual bool Unfetch(const Key &key);
  bool HardUnfetch(const Key& key);

  // Latch object that stores a counter for readlocks and a boolean for write
  // locks.
  enum State {
    UNINITIALIZED, IN_MEMORY, ON_DISK, FETCHING, RELEASING
  };
  class Latch {
   public:
    int active_requests;  // Can be as many as you want.
    State state;
    pthread_mutex_t lock_;

    Latch() {
      active_requests = 0;
      state = UNINITIALIZED;
      pthread_mutex_init(&lock_, NULL);
    }
  };
  Latch* LatchFor(const Key &key);
  static void PrefetchCompletionHandler(sigval_t sigval);
  static void UnfetchCompletionHandler(sigval_t sigval);
  static void GetKey(int fd, Key* key);

 private:
  FetchingStorage();
  /*
   * The following functions are bogus hacks that should be factored out into
   * a separate disk layer. To make the callbacks play nice for now, everything
   * is being placed here. After November 1, we can swap out backends.
   */

  enum Operation {
    FETCH, RELEASE
  };

  // Registers an asynchronous read.
  bool FileRead(const Key& key, char* result, int size);

  // Registers an asynchronous write.
  bool FilePut(const Key& key, char* value, int size);

  // XXX(scw): Document? `int fd' used to be `int& fd'. Move it to the last
  //           argument and retype `int* fd' if used as an output argument.
  aiocb* generateControlBlock(int fd, char* buf, const int size, Operation op);

  static FetchingStorage* self;

  // Additional State

  Storage* main_memory_;
  Latch* latches_;

  // GC thread stuff.
  static void* RunGCThread(void *arg);
  pthread_t gc_thread_;
};
#endif  // _DB_BACKEND_FETCHING_STORAGE_H_
