#ifndef _DB_BACKEND_STORAGE_MANAGER_H_
#define _DB_BACKEND_STORAGE_MANAGER_H_

#include <ucontext.h>
#include <tr1/unordered_map>
#include <vector>

#include "common/types.h"

using std::vector;
using std::tr1::unordered_map;

class Configuration;
class Connection;
class MessageProto;
class Scheduler;
class Storage;
class TxnProto;

class StorageManager
{
public:
  StorageManager(Configuration *config, Connection *connection,
                 Storage *actual_storage, TxnProto *txn);

  ~StorageManager();

  Value *ReadObject(const Key &key);
  bool PutObject(const Key &key, Value *value);
  bool DeleteObject(const Key &key);

  void HandleReadResult(const MessageProto &message);
  bool ReadyToExecute();

  Storage *GetStorage() { return actual_storage_; }

  // ✅ Versioned methods
  string ReadWithVersion(const Key &key, int *version);
  void PeekVersion(const Key &key, int *version);

  bool writer;

  // 実行中トランザクション
  TxnProto *txn_;

private:
  Configuration *configuration_;
  Connection *connection_;
  Storage *actual_storage_;

  unordered_map<Key, Value *> objects_;
  unordered_map<Key, int> version_map_; // ✅ key -> version のマップ
  vector<Value *> remote_reads_;
};

#endif // _DB_BACKEND_STORAGE_MANAGER_H_
