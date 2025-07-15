// Author: Alexander Thomson (thomson@cs.yale.edu)
// Author: Kun Ren (kun.ren@yale.edu)
//
// TODO(alex): UNIT TESTING!

#ifndef _DB_COMMON_UTILS_H_
#define _DB_COMMON_UTILS_H_

#include <assert.h>
#include <sys/time.h>
#include <unistd.h>
#include <cstdio>
#include <cstdlib>
#include <string>
#include <cmath>
#include <vector>
#include <tr1/unordered_map>

#include "common/types.h"

using std::string;
using std::vector;
using std::tr1::unordered_map;

#define ASSERTS_ON true

#define DCHECK(ARG) do { if (ASSERTS_ON) assert(ARG); } while (0)

// Status code for return values.
struct Status {
  // Represents overall status state.
  enum Code {
    ERROR = 0,
    OKAY = 1,
    DONE = 2,
  };
  Code code;

  // Optional explanation.
  string message;

  // Constructors.
  explicit Status(Code c) : code(c) {}
  Status(Code c, const string& s) : code(c), message(s) {}
  static Status Error() { return Status(ERROR); }
  static Status Error(const string& s) { return Status(ERROR, s); }
  static Status Okay() { return Status(OKAY); }
  static Status Done() { return Status(DONE); }

  // Pretty printing.
  string ToString() {
    string out;
    if (code == ERROR) out.append("Error");
    if (code == OKAY) out.append("Okay");
    if (code == DONE) out.append("Done");
    if (message.size()) {
      out.append(": ");
      out.append(message);
    }
    return out;
  }
};

// Returns the number of seconds since midnight according to local system time,
// to the nearest microsecond.
static inline double GetTime() {
  struct timeval tv;
  gettimeofday(&tv, NULL);
  return tv.tv_sec + tv.tv_usec/1e6;
}

// Busy-wait for 'duration' seconds.
static inline void Spin(double duration) {
  usleep(1000000 * duration);
//  double start = GetTime();
//  while (GetTime() < start + duration) {}
}

// Busy-wait until GetTime() >= time.
static inline void SpinUntil(double time) {
  while (GetTime() >= time) {}
}

// Produces a random alphabet string of the specified length
static inline string RandomString(int length) {
  string random_string;
  for (int i = 0; i < length; i++)
    random_string += rand() % 26 + 'A';

  return random_string;
}

// Returns a human-readable string representation of an int.
static inline string IntToString(int n) {
  char s[64];
  snprintf(s, sizeof(s), "%d", n);
  return string(s);
}

// Converts a human-readable numeric string to an int.
static inline int StringToInt(const string& s) {
  return atoi(s.c_str());
}

static inline string DoubleToString(double n) {
  char s[64];
  snprintf(s, sizeof(s), "%lf", n);
  return string(s);
}

static inline double StringToDouble(const string& s) {
  return atof(s.c_str());
}

static inline double RandomDoubleBetween(double fMin, double fMax) {
  double f = (double)rand()/RAND_MAX;
  return fMin + f*(fMax - fMin);
}

// Converts a human-readable numeric sub-string (starting at the 'n'th position
// of 's') to an int.
static inline int OffsetStringToInt(const string& s, int n) {
  return atoi(s.c_str() + n);
}

// Function for deleting a heap-allocated string after it has been sent on a
// zmq socket connection. E.g., if you want to send a heap-allocated
// string '*s' on a socket 'sock':
//
//  zmq::message_t msg((void*) s->data(), s->size(), DeleteString, (void*) s);
//  sock.send(msg);
//
static inline void DeleteString(void* data, void* hint) {
  delete reinterpret_cast<string*>(hint);
}
static inline void Noop(void* data, void* hint) {}

////////////////////////////////
class Mutex {
 public:
  // Mutexes come into the world unlocked.
  Mutex() {
    pthread_mutex_init(&mutex_, NULL);
  }

 private:
  friend class Lock;
  // Actual pthread mutex wrapped by Mutex class.
  pthread_mutex_t mutex_;

  // DISALLOW_COPY_AND_ASSIGN
  Mutex(const Mutex&);
  Mutex& operator=(const Mutex&);
};

class Lock {
 public:
  explicit Lock(Mutex* mutex) : mutex_(mutex) {
    pthread_mutex_lock(&mutex_->mutex_);
  }
  ~Lock() {
    pthread_mutex_unlock(&mutex_->mutex_);
  }

 private:
  Mutex* mutex_;

  // DISALLOW_DEFAULT_CONSTRUCTOR
  Lock();

  // DISALLOW_COPY_AND_ASSIGN
  Lock(const Lock&);
  Lock& operator=(const Lock&);
};

////////////////////////////////////////////////////////////////

template<typename T>
class AtomicQueue {
 public:
  AtomicQueue() {
    queue_.resize(256);
    size_ = 256;
    front_ = 0;
    back_ = 0;
  }

  // Returns the number of elements currently in the queue.
  inline size_t Size() {
    Lock l(&size_mutex_);
    return (back_ + size_ - front_) % size_;
  }

  // Returns true iff the queue is empty.
  inline bool Empty() {
    return front_ == back_;
  }

  // Atomically pushes 'item' onto the queue.
  inline void Push(const T& item) {
    Lock l(&back_mutex_);
    // Check if the buffer has filled up. Acquire all locks and resize if so.
    if (front_ == (back_+1) % size_) {
      Lock m(&front_mutex_);
      Lock n(&size_mutex_);
      uint32 count = (back_ + size_ - front_) % size_;
      queue_.resize(size_ * 2);
      for (uint32 i = 0; i < count; i++) {
        queue_[size_+i] = queue_[(front_ + i) % size_];
      }
      front_ = size_;
      back_ = size_ + count;
      size_ *= 2;
    }
    // Push item to back of queue.
    queue_[back_] = item;
    back_ = (back_ + 1) % size_;
  }

  // If the queue is non-empty, (atomically) sets '*result' equal to the front
  // element, pops the front element from the queue, and returns true,
  // otherwise returns false.
  inline bool Pop(T* result) {
    Lock l(&front_mutex_);
    if (front_ != back_) {
      *result = queue_[front_];
      front_ = (front_ + 1) % size_;
      return true;
    }
    return false;
  }

  // Sets *result equal to the front element and returns true, unless the
  // queue is empty, in which case does nothing and returns false.
  inline bool Front(T* result) {
    Lock l(&front_mutex_);
    if (front_ != back_) {
      *result = queue_[front_];
      return true;
    }
    return false;
  }

 private:
  vector<T> queue_;  // Circular buffer containing elements.
  uint32 size_;      // Allocated size of queue_, not number of elements.
  uint32 front_;     // Offset of first (oldest) element.
  uint32 back_;      // First offset following all elements.

  // Mutexes for synchronization.
  Mutex front_mutex_;
  Mutex back_mutex_;
  Mutex size_mutex_;

  // DISALLOW_COPY_AND_ASSIGN
  AtomicQueue(const AtomicQueue<T>&);
  AtomicQueue& operator=(const AtomicQueue<T>&);
};

class MutexRW {
 public:
  // Mutexes come into the world unlocked.
  MutexRW() {
    pthread_rwlock_init(&mutex_, NULL);
  }

 private:
  friend class ReadLock;
  friend class WriteLock;
  // Actual pthread rwlock wrapped by MutexRW class.
  pthread_rwlock_t mutex_;

  // DISALLOW_COPY_AND_ASSIGN
  MutexRW(const MutexRW&);
  MutexRW& operator=(const MutexRW&);
};

class ReadLock {
 public:
  explicit ReadLock(MutexRW* mutex) : mutex_(mutex) {
    pthread_rwlock_rdlock(&mutex_->mutex_);
  }
  ~ReadLock() {
    pthread_rwlock_unlock(&mutex_->mutex_);
  }

 private:
  MutexRW* mutex_;

  // DISALLOW_DEFAULT_CONSTRUCTOR
  ReadLock();

  // DISALLOW_COPY_AND_ASSIGN
  ReadLock(const ReadLock&);
  ReadLock& operator=(const ReadLock&);
};

class WriteLock {
 public:
  explicit WriteLock(MutexRW* mutex) : mutex_(mutex) {
    pthread_rwlock_wrlock(&mutex_->mutex_);
  }
  ~WriteLock() {
    pthread_rwlock_unlock(&mutex_->mutex_);
  }

 private:
  MutexRW* mutex_;

  // DISALLOW_DEFAULT_CONSTRUCTOR
  WriteLock();

  // DISALLOW_COPY_AND_ASSIGN
  WriteLock(const WriteLock&);
  WriteLock& operator=(const WriteLock&);
};

template<typename K, typename V>
class AtomicMap {
 public:
  AtomicMap() {}
  ~AtomicMap() {}

  inline bool Lookup(const K& k, V* v) {
    ReadLock l(&mutex_);
    typename unordered_map<K, V>::const_iterator lookup = map_.find(k);
    if (lookup == map_.end()) {
      return false;
    }
    *v = lookup->second;
    return true;
  }

  inline void Put(const K& k, const V& v) {
    WriteLock l(&mutex_);
    map_.insert(std::make_pair(k, v));
  }

  inline void Erase(const K& k) {
    WriteLock l(&mutex_);
    map_.erase(k);
  }

  // Puts (k, v) if there is no record for k. Returns the value of v that is
  // associated with k afterwards (either the inserted value or the one that
  // was there already).
  inline V PutNoClobber(const K& k, const V& v) {
    WriteLock l(&mutex_);
    typename unordered_map<K, V>::const_iterator lookup = map_.find(k);
    if (lookup != map_.end()) {
      return lookup->second;
    }
    map_.insert(std::make_pair(k, v));
    return v;
  }

  inline uint32 Size() {
    ReadLock l(&mutex_);
    return map_.size();
  }

  inline void DeleteVAndClear() {
    WriteLock l(&mutex_);
    for (typename unordered_map<K, V>::iterator it = map_.begin();
       it != map_.end(); ++it) {
      delete it->second;
    }
    map_.clear();
  }

 private:
  unordered_map<K, V> map_;
  MutexRW mutex_;

  // DISALLOW_COPY_AND_ASSIGN
  AtomicMap(const AtomicMap<K, V>&);
  AtomicMap& operator=(const AtomicMap<K, V>&);
};

#endif  // _DB_COMMON_UTILS_H_

