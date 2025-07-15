// Author: Thaddeus Diamond (diamond@cs.yale.edu)
//
// This is the implementation for a versioned database backend

#include "backend/collapsed_versioned_storage.h"

#include <cstdio>
#include <cstdlib>
#include <string>

using std::string;

#define TPCCHACK

#ifdef TPCCHACK
#define MAXARRAYSIZE 1000000
  // For inserted objects we need to make a representation that is thread-safe
  // (i.e. an array).  This is kind of hacky, but since this only corresponds
  // to TPCC, we'll be okay
  Value* NewOrderStore[MAXARRAYSIZE];
  Value* OrderStore[MAXARRAYSIZE];
  Value* OrderLineStore[MAXARRAYSIZE * 15];
  Value* HistoryStore[MAXARRAYSIZE];
#endif

Value* CollapsedVersionedStorage::ReadObject(const Key& key, int64 txn_id) {
#ifdef TPCCHACK
  if (key.find("ol") != string::npos) {
    return OrderLineStore[txn_id * 15 + atoi(&key[key.find("line(") + 5])];
  } else if (key.find("no") != string::npos) {
    return NewOrderStore[txn_id];
  } else if (key.find("o") != string::npos) {
    return OrderStore[txn_id];
  } else if (key.find("h") != string::npos) {
    return HistoryStore[txn_id];
  } else {
#endif

  // Check to see if a match even exists
  if (objects_.count(key) != 0) {
    for (DataNode* list = objects_[key]; list; list = list->next) {
      if (list->txn_id <= txn_id) {
        return list->value;
      }
    }
  }

#ifdef TPCCHACK
  }
#endif

  // No match found
  return NULL;
}

bool CollapsedVersionedStorage::PutObject(const Key& key, Value* value,
                                          int64 txn_id) {
#ifdef TPCCHACK
  if (key.find("ol") != string::npos) {
    OrderLineStore[txn_id * 15 + atoi(&key[key.find("line(") + 5])] =
      value;
  } else if (key.find("no") != string::npos) {
    NewOrderStore[txn_id] = value;
  } else if (key.find("o") != string::npos) {
    OrderStore[txn_id] = value;
  } else if (key.find("h") != string::npos) {
    HistoryStore[txn_id] = value;
  } else {
#endif

  // Create the new version to insert into the list
  DataNode* item = new DataNode();
  item->txn_id = txn_id;
  item->value = value;
  item->next = NULL;

  // Is the most recent value a candidate for pruning?
  DataNode* current;
  if (objects_.count(key) != 0 && (current = objects_[key]) != NULL) {
    int64 most_recent = current->txn_id;

    if ((most_recent > stable_ && txn_id > stable_) ||
        (most_recent <= stable_ && txn_id <= stable_)) {
      item->next = current->next;
      delete current;
    } else {
      item->next = current;
    }
  }
  objects_[key] = item;

#ifdef TPCCHACK
  }
#endif

  return true;
}

bool CollapsedVersionedStorage::DeleteObject(const Key& key, int64 txn_id) {
#ifdef TPCCHACK
  if (key.find("o") != string::npos || key.find("h") != string::npos)
    return false;
#endif

  DataNode* list = (objects_.count(key) == 0 ? NULL : objects_[key]);

  while (list != NULL) {
    if ((list->txn_id > stable_ && txn_id > stable_) ||
        (list->txn_id <= stable_ && txn_id <= stable_))
      break;

    list = list->next;
  }

  // First we need to insert an empty string when there is >1 item
  if (list != NULL && objects_[key] == list && list->next != NULL) {
    objects_[key]->txn_id = txn_id;
    objects_[key]->value = NULL;

  // Otherwise we need to free the head
  } else if (list != NULL && objects_[key] == list) {
    delete objects_[key];
    objects_[key] = NULL;

  // Lastly, we may only want to free the tail
  } else if (list != NULL) {
    delete list;
    objects_[key]->next = NULL;
  }

  return true;
}

int CollapsedVersionedStorage::Checkpoint() {
  pthread_t checkpointing_daemon;
  int thread_status = pthread_create(&checkpointing_daemon, NULL,
                                     &RunCheckpointer, this);

  return thread_status;
}

void CollapsedVersionedStorage::CaptureCheckpoint() {
  // Give the user output
  fprintf(stdout, "Beginning checkpoint capture...\n");

  // First, we open the file for writing
  char log_name[200];
  snprintf(log_name, sizeof(log_name), "%s/%ld.checkpoint", CHKPNTDIR, stable_);
  FILE* checkpoint = fopen(log_name, "w");

  // Next we iterate through all of the objects and write the stable version
  // to disk
  unordered_map<Key, DataNode*>::iterator it;
  for (it = objects_.begin(); it != objects_.end(); it++) {
    // Read in the stable value
    Key key = it->first;
    Value* result = ReadObject(key, stable_);

    // Write <len_key_bytes|key|len_value_bytes|value> to disk
    int key_length = key.length();
    int val_length = result->length();
    fprintf(checkpoint, "%c%c%c%c%s%c%c%c%c%s",
            static_cast<char>(key_length >> 24),
            static_cast<char>(key_length >> 16),
            static_cast<char>(key_length >> 8),
            static_cast<char>(key_length),
            key.c_str(),
            static_cast<char>(val_length >> 24),
            static_cast<char>(val_length >> 16),
            static_cast<char>(val_length >> 8),
            static_cast<char>(val_length),
            result->c_str());

    // Remove object from tree if there's an old version
    if (it->second->next != NULL)
      DeleteObject(key, stable_);
  }

#ifdef TPCCHACK
  fprintf(checkpoint, "\nNewOrder\n");
  for (int64 i = 0; i < MAXARRAYSIZE; i++) {
    if (NewOrderStore[i] != NULL) {
      fprintf(checkpoint, "%ld%s",
              static_cast<int64>((*NewOrderStore[i]).length()),
              (*NewOrderStore[i]).c_str());
    }
  }

  fprintf(checkpoint, "\nOrder\n");
  for (int64 i = 0; i < MAXARRAYSIZE; i++) {
    if (OrderStore[i] != NULL) {
      fprintf(checkpoint, "%ld%s",
              static_cast<int64>((*OrderStore[i]).length()),
              (*OrderStore[i]).c_str());
    }
  }

  fprintf(checkpoint, "\nOrderLine\n");
  for (int64 i = 0; i < MAXARRAYSIZE * 15; i++) {
    if (OrderLineStore[i] != NULL) {
      fprintf(checkpoint, "%ld%s",
              static_cast<int64>((*OrderLineStore[i]).length()),
              (*OrderLineStore[i]).c_str());
    }
  }

  fprintf(checkpoint, "\nHistory\n");
  for (int64 i = 0; i < MAXARRAYSIZE; i++) {
    if (HistoryStore[i] != NULL) {
      fprintf(checkpoint, "%ld%s",
              static_cast<int64>((*HistoryStore[i]).length()),
              (*HistoryStore[i]).c_str());
    }
  }
#endif

  // Close the file
  fclose(checkpoint);

  // Give the user output
  fprintf(stdout, "Finished checkpointing\n");
}
