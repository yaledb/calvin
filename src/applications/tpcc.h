// Author: Kun Ren (kun.ren@yale.edu)
// Author: Thaddeus Diamond (diamond@cs.yale.edu)
//
// A concrete implementation of TPC-C (application subclass)

#ifndef _DB_APPLICATIONS_TPCC_H_
#define _DB_APPLICATIONS_TPCC_H_

#include <string>

#include "applications/application.h"
#include "proto/txn.pb.h"
#include "common/configuration.h"

#define WAREHOUSES_PER_NODE 12
#define DISTRICTS_PER_WAREHOUSE 10
#define DISTRICTS_PER_NODE (WAREHOUSES_PER_NODE * DISTRICTS_PER_WAREHOUSE)
#define CUSTOMERS_PER_DISTRICT 3000
#define CUSTOMERS_PER_NODE (DISTRICTS_PER_NODE * CUSTOMERS_PER_DISTRICT)
#define NUMBER_OF_ITEMS 100000

using std::string;

class Warehouse;
class District;
class Customer;
class Item;
class Stock;

class TPCC : public Application {
 public:
  enum TxnType {
    INITIALIZE = 0,
    NEW_ORDER = 1,
    PAYMENT = 2,
    ORDER_STATUS = 3,
    DELIVERY = 4,
    STOCK_LEVEL = 5,
  };

  virtual ~TPCC() {}

  // Load generator for a new transaction
  virtual TxnProto* NewTxn(int64 txn_id, int txn_type, string args,
                           Configuration* config) const;

  // The key converter takes a valid key (string) and converts it to an id
  // for the checkpoint to use
  static int CheckpointID(Key key) {
    // Initial dissection of the key
    size_t id_idx;

    // Switch based on key type
    size_t bad = string::npos;
    if ((id_idx = key.find("s")) != bad) {
      size_t ware = key.find("w");
      return 1000000 + NUMBER_OF_ITEMS * atoi(&key[ware + 1]) +
             atoi(&key[id_idx + 2]);
    } else if ((id_idx = key.find("c")) != bad) {
      return WAREHOUSES_PER_NODE + DISTRICTS_PER_NODE + atoi(&key[id_idx + 1]);
    } else if ((id_idx = key.find("d")) != bad && key.find("y") == bad) {
      return WAREHOUSES_PER_NODE + atoi(&key[id_idx + 1]);
    } else if ((id_idx = key.find("w")) != bad) {
      return atoi(&key[id_idx + 1]);
    } else if ((id_idx = key.find("i")) != bad) {
      return 3000000 + atoi(&key[id_idx + 1]);
    } else if ((id_idx = key.find("ol")) != bad) {
      return 4000000 + atoi(&key[id_idx + 2]);
    } else if ((id_idx = key.find("no")) != bad) {
      return 5000000 + atoi(&key[id_idx + 2]);
    } else if ((id_idx = key.find("o")) != bad) {
      return 6000000 + atoi(&key[id_idx + 1]);
    } else if ((id_idx = key.find("h")) != bad) {
      return 7000000 + atoi(&key[id_idx + 1]);
    } else if ((id_idx = key.find("ln")) != bad) {
      return 8000000 + atoi(&key[id_idx + 2]);
    }

    // Invalid key
    return -1;
  }

  // Simple execution of a transaction using a given storage
  virtual int Execute(TxnProto* txn, StorageManager* storage) const;

/* TODO(Thad): Uncomment once testing friend class exists
 private: */
  // When the first transaction is called, the following function initializes
  // a set of fake data for use in the application
  virtual void InitializeStorage(Storage* storage, Configuration* conf) const;

  // The following methods are simple randomized initializers that provide us
  // fake data for our TPC-C function
  Warehouse* CreateWarehouse(Key id) const;
  District* CreateDistrict(Key id, Key warehouse_id) const;
  Customer* CreateCustomer(Key id, Key district_id, Key warehouse_id) const;
  Item* CreateItem(Key id) const;
  Stock* CreateStock(Key id, Key warehouse_id) const;

  // A NewOrder call takes a set of args and a transaction id and performs
  // the new order transaction as specified by TPC-C.  The return is 1 for
  // success or 0 for failure.
  int NewOrderTransaction(TxnProto* txn, StorageManager* storage) const;

  // A Payment call takes a set of args as the parameter and performs the
  // payment transaction, returning a 1 for success or 0 for failure.
  int PaymentTransaction(TxnProto* txn, StorageManager* storage) const;

  int OrderStatusTransaction(TxnProto* txn, StorageManager* storage) const;

  int StockLevelTransaction(TxnProto* txn, StorageManager* storage) const;

  int DeliveryTransaction(TxnProto* txn, StorageManager* storage) const;

  // The following are implementations of retrieval and writing for local items
  Value* GetItem(Key key) const;
  void SetItem(Key key, Value* value) const;
};

#endif  // _DB_APPLICATIONS_TPCC_H_
