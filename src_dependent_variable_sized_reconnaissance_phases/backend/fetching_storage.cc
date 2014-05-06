// Author: Philip Shao (philip.shao@yale.edu)
//
// An implementation of the storage interface taking into account
// main memory, disk, and swapping algorithms.

#include "backend/fetching_storage.h"

typedef FetchingStorage::Latch Latch;

////////////////// Constructors/Destructors  //////////////////////

// Singleton constructor

FetchingStorage* FetchingStorage::self = NULL;

FetchingStorage* FetchingStorage::BuildStorage() {
  if (self == NULL)
    self = new FetchingStorage();
  return self;
}

// Private constructor

FetchingStorage::FetchingStorage() {
  main_memory_ = new SimpleStorage();
  // 1 MILLION LATCHES!
  latches_ = new Latch[1000000];

  pthread_create(&gc_thread_, NULL, RunGCThread,
    reinterpret_cast<void*>(this));
}

FetchingStorage::~FetchingStorage() {
  delete main_memory_;
  delete[] latches_;
}

////////////////// Private utility functions  //////////////////////

Latch* FetchingStorage::LatchFor(const Key& key) {
  // Just fail miserably if we are passed a non-int.
  // assert(atoi(key.c_str()) != 0);
  // An array is a nice threadsafe hashtable.
  return latches_ + atoi(key.c_str());
}

void FetchingStorage::GetKey(int fd, Key* key) {
    char path[255];
    snprintf(path, sizeof(path), "/proc/self/fd/%d", fd);
    char key_c_str[255];
    memset(&key_c_str, 0, 255);
    readlink(path, key_c_str, 255);
    *key = string((strrchr(key_c_str, '/') + 1));
}

void* FetchingStorage::RunGCThread(void *arg) {
  FetchingStorage* storage = reinterpret_cast<FetchingStorage*>(arg);
  while (true) {
    double start_time = GetTime();
    for (int i = COLD_CUTOFF; i < 1000000; i++) {
      storage->HardUnfetch(IntToString(i));
    }
    usleep(static_cast<int>(1000000*(GetTime()-start_time)));
  }
  return NULL;
}

///////////// The meat and potato public interface methods.  ///////////

Value* FetchingStorage::ReadObject(const Key& key, int64 txn_id) {
  Latch* latch = LatchFor(key);
  // Must call a Prefetch before transaction.
  pthread_mutex_lock(&latch->lock_);
  assert(latch->state != ON_DISK);
  assert(latch->state != RELEASING);
  assert(latch->state == FETCHING || latch->state == IN_MEMORY);
  assert(latch->active_requests > 0);
  pthread_mutex_unlock(&latch->lock_);
  // Block thread until pre-fetch on this key is done.
  while (latch->state == FETCHING) {}
  return main_memory_->ReadObject(key);
}

// Write data to memory.
bool FetchingStorage::PutObject(const Key& key, Value* value, int64 txn_id) {
  Latch* latch = LatchFor(key);
  pthread_mutex_lock(&latch->lock_);
  // Must call a prefetch before transaction
  assert(latch->active_requests > 0);
  latch->state = IN_MEMORY;
  pthread_mutex_unlock(&latch->lock_);
  main_memory_->PutObject(key, value);
  return true;
}

// Put null, change state to uninitialized.
bool FetchingStorage::DeleteObject(const Key& key, int64 txn_id) {
  return PutObject(key, NULL, txn_id);
}

// Return false if file does not exist.
bool FetchingStorage::Prefetch(const Key& key, double* wait_time) {
  Latch* latch = LatchFor(key);

  pthread_mutex_lock(&latch->lock_);

  int active_requests = latch->active_requests;
  latch->active_requests++;

  State previous_state = latch->state;
  if (previous_state == ON_DISK)
    latch->state = FETCHING;
  if (previous_state == UNINITIALIZED) {
    main_memory_->PutObject(key, new Value());
    latch->state = IN_MEMORY;
  }
  if (previous_state == RELEASING)
    latch->state = IN_MEMORY;
  State current_state = latch->state;

  pthread_mutex_unlock(&latch->lock_);

  // Pre-fetch and in memory pre-states are no-ops.
  if (current_state == IN_MEMORY) {
    *wait_time = 0;  // You're good to go.
    return true;
  } else if (previous_state == FETCHING) {
    // We already have another prefetching attempt.
    *wait_time = 0.100;  // arbitrary nonzero result.
    return true;
  } else {
    // Not in memory: cold call to prefetch.
    assert(active_requests == 0);
    *wait_time = 0.100;  // somewhat larger arbitrary nonzero result.
    char* buf = new char[PAGE_SIZE];
    return FileRead(key, buf, PAGE_SIZE);
  }
}

bool FetchingStorage::HardUnfetch(const Key& key) {
  // Since we have a write lock, we know there are no concurrent
  // reads to this key, so we can freely read as well.
  Latch* latch = LatchFor(key);
  pthread_mutex_lock(&latch->lock_);

  State previous_state = latch->state;
  int active_requests = latch->active_requests;

  // Only one of the following two conditions can be true.
  if (active_requests == 0)
    latch->state = RELEASING;
  if (active_requests == 0 && latch->state == FETCHING)
    latch->state = ON_DISK;

  pthread_mutex_unlock(&latch->lock_);

  if (active_requests == 0 && previous_state == IN_MEMORY) {
    Value* result = main_memory_->ReadObject(key);
    int len = strlen(result->c_str());
    char* c_result = new char[len+1];
    strcpy(c_result, result->c_str());
    return FilePut(key, c_result, len);
  } else {
    return true;
  }
}

bool FetchingStorage::Unfetch(const Key& key) {
  Latch* latch = LatchFor(key);
  pthread_mutex_lock(&latch->lock_);
  State state = latch->state;
  latch->active_requests--;
  assert(latch->active_requests >= 0);
  assert(latch->state == FETCHING || latch->state == RELEASING ||
         latch->state == IN_MEMORY);
  pthread_mutex_unlock(&latch->lock_);
  if (state == UNINITIALIZED)
    HardUnfetch(key);
  return true;
}

///////////////// Asynchronous Callbacks ////////////////////////

void FetchingStorage::PrefetchCompletionHandler(sigval_t sigval) {
  struct aiocb *req;
  req = (struct aiocb *)sigval.sival_ptr;
  /* Did the request complete? */
  if (aio_error(req) == 0) {
    /* Request completed successfully, get the return status */
    string key;
    char* buf =
        const_cast<char*>(reinterpret_cast<volatile char*>(req->aio_buf));
    GetKey(req->aio_fildes, &key);
    FetchingStorage* store = FetchingStorage::BuildStorage();
    Latch* latch = store->LatchFor(key);
    pthread_mutex_lock(&latch->lock_);
    State prev_state = latch->state;
    latch->state = IN_MEMORY;
    pthread_mutex_unlock(&latch->lock_);
    /* Nothing interfered with our fetch */
    if (prev_state == FETCHING) {
      string* value = new string(buf, PAGE_SIZE);
      store->main_memory_->PutObject(key, value);
    }
    delete[] buf;
    close(req->aio_fildes);
    delete req;
  }
}

void FetchingStorage::UnfetchCompletionHandler(sigval_t sigval) {
  struct aiocb *req;
  req = (struct aiocb *)sigval.sival_ptr;
  /* Did the request complete? */
  if (aio_error(req) == 0) {
    /* Request completed successfully, get the return status */
    string key;
    GetKey(req->aio_fildes, &key);
    FetchingStorage* store = FetchingStorage::BuildStorage();
    Latch* latch = store->LatchFor(key);
    pthread_mutex_lock(&latch->lock_);
    // Hasn't been fetched.
    assert(latch->active_requests >= 0);
    int active_requests = latch->active_requests;
    State state = latch->state;
    pthread_mutex_unlock(&latch->lock_);
    if (state == RELEASING && active_requests <= 0) {
       store->main_memory_->DeleteObject(key);
       latch->state = ON_DISK;
    }
    close(req->aio_fildes);
    delete[] reinterpret_cast<volatile char*>(req->aio_buf);
    delete req;
  }
  return;
}

/*
 * Here live the bogus hacks.
 */

bool FetchingStorage::FileRead(const Key& key, char* result, int size) {
  string fileName(STORAGE_PATH);
  fileName.append(key);
  int fd = open(fileName.c_str(), O_RDONLY|O_NONBLOCK);
  if (fd == -1)
    return false;
  return aio_read(generateControlBlock(fd, result, size, FETCH)) >= 0;
}

bool FetchingStorage::FilePut(const Key& key, char* value, int size) {
  string fileName(STORAGE_PATH);
  fileName.append(key);
  mode_t mode = S_IRUSR | S_IWUSR | S_IRGRP | S_IROTH;
  int fd = open(fileName.c_str(), O_RDWR|O_CREAT|O_TRUNC|O_NONBLOCK, mode);
  if (fd == -1)
    return false;
  return aio_write(generateControlBlock(fd, value, size, RELEASE)) >= 0;
}

aiocb* FetchingStorage::generateControlBlock(
    int fd, char* buf, const int size, Operation op) {
  aiocb* aiocbp = new aiocb();
  aiocbp->aio_fildes = fd;
  aiocbp->aio_offset = 0;
  aiocbp->aio_buf = buf;
  aiocbp->aio_nbytes = size;
  aiocbp->aio_reqprio = 0;
  /* Link the AIO request with a thread callback */
  aiocbp->aio_sigevent.sigev_notify = SIGEV_THREAD;
  if (op == FETCH)
    aiocbp->aio_sigevent.sigev_notify_function =
        &PrefetchCompletionHandler;
  else
    aiocbp->aio_sigevent.sigev_notify_function =
        &UnfetchCompletionHandler;
  aiocbp->aio_sigevent.sigev_notify_attributes = NULL;
  aiocbp->aio_sigevent.sigev_value.sival_ptr = aiocbp;
  return aiocbp;
}
