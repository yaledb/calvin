// Author: Alexander Thomson (thomson@cs.yale.edu)

#include "backend/storage_manager.h"

#include <string>

#include "backend/simple_storage.h"
#include "common/configuration.h"
#include "common/connection.h"
#include "common/testing.h"
#include "common/utils.h"
#include "proto/txn.pb.h"

TEST(SingleNode) {
  Configuration config(0, "common/configuration_test_one_node.conf");
  ConnectionMultiplexer* multiplexer = new ConnectionMultiplexer(&config);
  Spin(0.1);
  Connection* connection = multiplexer->NewConnection("storage_manager");
  SimpleStorage storage;

  string a = "a";
  string c = "c";
  storage.PutObject("0", &a);
  storage.PutObject("2", &c);
  TxnProto txn;
  txn.set_txn_id(1);
  txn.add_read_set("0");
  txn.add_write_set("2");
  txn.add_readers(1);
  txn.add_writers(1);

  StorageManager* storage_manager =
      new StorageManager(&config, connection, &storage, &txn);

  Value* result_x;
  result_x = storage_manager->ReadObject("0");
  EXPECT_TRUE(storage_manager->PutObject("2", result_x));

  result_x = storage.ReadObject("2");
  EXPECT_EQ("a", *result_x);

  delete storage_manager;
  delete connection;
  delete multiplexer;

  END;
}

// TODO(alex): Global variables of class type are prohibited by the google style
//             guide.
Configuration config1(1, "common/configuration_test.conf");
Configuration config2(2, "common/configuration_test.conf");
ConnectionMultiplexer* multiplexer1;
ConnectionMultiplexer* multiplexer2;
Connection* c1;
Connection* c2;
SimpleStorage storage1;
SimpleStorage storage2;
TxnProto txn;

void* ExecuteTxn(void* arg) {
  int node = *reinterpret_cast<int*>(arg);

  StorageManager* manager;
  if (node == 1)
    manager = new StorageManager(&config1, c1, &storage1, &txn);
  else
    manager = new StorageManager(&config2, c2, &storage2, &txn);

  Value* result_x;
  Value* result_xy;
  result_x = manager->ReadObject("0");
  result_xy = manager->ReadObject("1");
  EXPECT_EQ("a", *result_x);
  EXPECT_EQ("b", *result_xy);
  EXPECT_TRUE(manager->PutObject("2", result_x));
  EXPECT_TRUE(manager->PutObject("3", result_xy));
  result_x = manager->ReadObject("2");
  result_xy = manager->ReadObject("3");
  EXPECT_EQ("a", *result_x);
  EXPECT_EQ("b", *result_xy);

  delete manager;

  return NULL;
}

TEST(TwoNodes) {
  multiplexer1 = new ConnectionMultiplexer(&config1);
  multiplexer2 = new ConnectionMultiplexer(&config2);
  Spin(0.1);
  c1 = multiplexer1->NewConnection("1");
  c2 = multiplexer2->NewConnection("1");

  string a = "a";
  string b = "b";
  string c = "c";
  string d = "d";
  storage1.PutObject("0", &a);
  storage2.PutObject("1", &b);
  storage1.PutObject("2", &c);
  storage2.PutObject("3", &d);
  txn.set_txn_id(1);
  txn.add_read_set("0");
  txn.add_read_set("1");
  txn.add_write_set("2");
  txn.add_write_set("3");
  txn.add_readers(1);
  txn.add_readers(2);
  txn.add_writers(1);
  txn.add_writers(2);

  int node1 = 1;
  int node2 = 2;
  pthread_t thread_1;
  pthread_t thread_2;
  pthread_create(&thread_1, NULL, ExecuteTxn, reinterpret_cast<void*>(&node1));
  pthread_create(&thread_2, NULL, ExecuteTxn, reinterpret_cast<void*>(&node2));
  pthread_join(thread_1, NULL);
  pthread_join(thread_2, NULL);

  Value* result_x;
  Value* result_xy;
  result_x = storage1.ReadObject("2");
  result_xy = storage2.ReadObject("3");
  EXPECT_EQ("a", *result_x);
  EXPECT_EQ("b", *result_xy);

  delete c1;
  delete c2;
  delete multiplexer1;
  delete multiplexer2;

  END;
}

int main(int argc, char** argv) {
// TODO(alex): Fix these tests!
//  SingleNode();
//  TwoNodes();
}

