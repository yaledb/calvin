// Author: Thaddeus Diamond (diamond@cs.yale.edu)

#include "applications/tpcc.h"

#include "backend/simple_storage.h"
#include "backend/storage_manager.h"
#include "common/configuration.h"
#include "common/connection.h"
#include "common/testing.h"
#include "common/utils.h"

// We make these global variables to avoid weird pointer passing and code
// redundancy
Configuration* config =
  new Configuration(0, "common/configuration_test_one_node.conf");
ConnectionMultiplexer* multiplexer;
Connection* connection;
SimpleStorage* simple_store;
TPCC* tpcc;

// Test the id generation
TEST(IdGenerationTest) {
  EXPECT_EQ(tpcc->CheckpointID("w1"), 1);
  EXPECT_EQ(tpcc->CheckpointID("d1"), WAREHOUSES_PER_NODE + 1);
  EXPECT_EQ(tpcc->CheckpointID("c1"), WAREHOUSES_PER_NODE +
                                               DISTRICTS_PER_NODE + 1);
  EXPECT_EQ(tpcc->CheckpointID("w2si1"),
                               1000000 + 2 * NUMBER_OF_ITEMS + 1);
  EXPECT_EQ(tpcc->CheckpointID("i1"), 3000001);
  EXPECT_EQ(tpcc->CheckpointID("ol1"), 4000001);
  EXPECT_EQ(tpcc->CheckpointID("no1"), 5000001);
  EXPECT_EQ(tpcc->CheckpointID("o1"), 6000001);
  EXPECT_EQ(tpcc->CheckpointID("h1"), 7000001);
  EXPECT_EQ(tpcc->CheckpointID("ln1"), 8000001);

  END
}

// Test for creation of a warehouse, ensure the attributes are correct
TEST(WarehouseTest) {
  Warehouse* warehouse = tpcc->CreateWarehouse("w1");

  EXPECT_EQ(warehouse->id(), "w1");
  EXPECT_TRUE(warehouse->has_name());
  EXPECT_TRUE(warehouse->has_street_1());
  EXPECT_TRUE(warehouse->has_street_2());
  EXPECT_TRUE(warehouse->has_city());
  EXPECT_TRUE(warehouse->has_state());
  EXPECT_TRUE(warehouse->has_zip());
  EXPECT_EQ(warehouse->tax(), 0.05);
  EXPECT_EQ(warehouse->year_to_date(), 0.0);

  // Finish
  delete warehouse;
  END
}

// Test for creation of a district, ensure the attributes are correct
TEST(DistrictTest) {
  District* district = tpcc->CreateDistrict("d1", "w1");

  EXPECT_EQ(district->id(), "d1");
  EXPECT_EQ(district->warehouse_id(), "w1");
  EXPECT_TRUE(district->has_name());
  EXPECT_TRUE(district->has_street_1());
  EXPECT_TRUE(district->has_street_2());
  EXPECT_TRUE(district->has_city());
  EXPECT_TRUE(district->has_state());
  EXPECT_TRUE(district->has_zip());
  EXPECT_EQ(district->tax(), 0.05);
  EXPECT_EQ(district->year_to_date(), 0.0);
  EXPECT_EQ(district->next_order_id(), 1);

  // Finish
  delete district;
  END
}

// Test for creation of a customer, ensure the attributes are correct
TEST(CustomerTest) {
  // Create a transaction so the customer creation can do secondary insertion
  TxnProto* secondary_keying = new TxnProto();
  secondary_keying->set_txn_id(1);
  Customer* customer = tpcc->CreateCustomer("c1", "d1", "w1");

  EXPECT_EQ(strcmp(customer->id().c_str(), "c1"), 0);
  EXPECT_EQ(strcmp(customer->district_id().c_str(), "d1"), 0);
  EXPECT_EQ(strcmp(customer->warehouse_id().c_str(), "w1"), 0);
  EXPECT_TRUE(customer->has_first());
  EXPECT_TRUE(customer->has_middle());
  EXPECT_TRUE(customer->has_last());
  EXPECT_TRUE(customer->has_street_1());
  EXPECT_TRUE(customer->has_street_2());
  EXPECT_TRUE(customer->has_city());
  EXPECT_TRUE(customer->has_state());
  EXPECT_TRUE(customer->has_zip());
  EXPECT_TRUE(customer->has_data());
  EXPECT_EQ(customer->since(), 0);
  EXPECT_EQ(customer->credit(), "GC");
  EXPECT_EQ(customer->credit_limit(), 0.01);
  EXPECT_EQ(customer->discount(), 0.5);
  EXPECT_EQ(customer->balance(), 0);
  EXPECT_EQ(customer->year_to_date_payment(), 0);
  EXPECT_EQ(customer->payment_count(), 0);
  EXPECT_EQ(customer->delivery_count(), 0);

  // Finish
  delete secondary_keying;
  delete customer;
  END
}

// Test for creation of an item, ensure the attributes are correct
TEST(ItemTest) {
  Item* item = tpcc->CreateItem("i1");

  EXPECT_EQ(item->id(), "i1");
  EXPECT_TRUE(item->has_name());
  EXPECT_TRUE(item->has_price());
  EXPECT_TRUE(item->has_data());

  // Finish
  delete item;
  END
}

// Test for creation of a stock, ensure the attributes are correct
TEST(StockTest) {
  Stock* stock = tpcc->CreateStock("i1", "w1");

  EXPECT_EQ(stock->id(), "w1si1");
  EXPECT_EQ(stock->warehouse_id(), "w1");
  EXPECT_EQ(stock->item_id(), "i1");
  EXPECT_TRUE(stock->has_quantity());
  EXPECT_TRUE(stock->has_data());
  EXPECT_EQ(stock->year_to_date(), 0);
  EXPECT_EQ(stock->order_count(), 0);
  EXPECT_EQ(stock->remote_count(), 0);

  // Finish
  delete stock;
  END
}

// This initializes a new transaction and ensures it has the desired properties
TEST(NewTxnTest) {
  TPCCArgs* tpcc_args = new TPCCArgs();
  tpcc_args->set_system_time(GetTime());

  // Txn args w/number of warehouses
  TPCCArgs* txn_args = new TPCCArgs();
  txn_args->set_multipartition(false);
  txn_args->set_system_time(GetTime());
  Value txn_args_value;
  assert(txn_args->SerializeToString(&txn_args_value));

  // Initialize Transaction Generation
  TxnProto* txn = tpcc->NewTxn(1, TPCC::INITIALIZE, txn_args_value, config);
  EXPECT_EQ(txn->txn_id(), 1);
  EXPECT_EQ(txn->txn_type(), TPCC::INITIALIZE);
  EXPECT_EQ(txn->isolation_level(), TxnProto::SERIALIZABLE);
  EXPECT_EQ(txn->status(), TxnProto::NEW);

  // New Order Transaction Generation
  delete txn;
  txn = tpcc->NewTxn(2, TPCC::NEW_ORDER, txn_args_value, config);
  EXPECT_EQ(txn->txn_id(), 2);
  EXPECT_EQ(txn->txn_type(), TPCC::NEW_ORDER);
  EXPECT_EQ(txn->isolation_level(), TxnProto::SERIALIZABLE);
  EXPECT_EQ(txn->status(), TxnProto::NEW);

  EXPECT_TRUE(tpcc_args->ParseFromString(txn->arg()));
  EXPECT_TRUE(tpcc_args->order_line_count() >= 5 &&
              tpcc_args->order_line_count() <= 15);
  EXPECT_TRUE(txn->write_set_size() == tpcc_args->order_line_count() + 2);
  for (int i = 0; i < tpcc_args->order_line_count(); i++)
    EXPECT_TRUE(tpcc_args->quantities(i) <= 10 && tpcc_args->quantities(i) > 0);

  // Payment Transaction Generation
  delete txn;
  txn = tpcc->NewTxn(4, TPCC::PAYMENT, txn_args_value, config);
  EXPECT_EQ(txn->txn_id(), 4);
  EXPECT_EQ(txn->txn_type(), TPCC::PAYMENT);
  EXPECT_EQ(txn->isolation_level(), TxnProto::SERIALIZABLE);
  EXPECT_EQ(txn->status(), TxnProto::NEW);

  EXPECT_TRUE(tpcc_args->ParseFromString(txn->arg()));
  EXPECT_TRUE(tpcc_args->amount() >= 1 && tpcc_args->amount() <= 5000);
  EXPECT_EQ(txn->write_set_size(), 1);
  EXPECT_TRUE(txn->read_set_size() == 0 || txn->read_set_size() == 1);

  // Finish
  delete txn;
  delete txn_args;
  delete tpcc_args;
  END
}

// Initialize the database and ensure that there are the correct
// objects actually in the database
TEST(InitializeTest) {
  // Run initialization method.
  tpcc->InitializeStorage(simple_store, config);

  // Expect all the warehouses to be there
  for (int i = 0; i < WAREHOUSES_PER_NODE; i++) {
    char warehouse_key[128];
    Value* warehouse_value;
    snprintf(warehouse_key, sizeof(warehouse_key), "w%d", i);
    warehouse_value = simple_store->ReadObject(warehouse_key);

    Warehouse* dummy_warehouse = new Warehouse();
    EXPECT_TRUE(dummy_warehouse->ParseFromString(*warehouse_value));
    delete dummy_warehouse;

    // Expect all the districts to be there
    for (int j = 0; j < DISTRICTS_PER_WAREHOUSE; j++) {
      char district_key[128];
      Value* district_value;
      snprintf(district_key, sizeof(district_key), "w%dd%d",
               i, j);
      district_value = simple_store->ReadObject(district_key);

      District* dummy_district = new District();
      EXPECT_TRUE(dummy_district->ParseFromString(*district_value));
      delete dummy_district;

      // Expect all the customers to be there
      for (int k = 0; k < CUSTOMERS_PER_DISTRICT; k++) {
        char customer_key[128];
        Value* customer_value;
        snprintf(customer_key, sizeof(customer_key),
                 "w%dd%dc%d", i, j, k);
        customer_value = simple_store->ReadObject(customer_key);

        Customer* dummy_customer = new Customer();
        EXPECT_TRUE(dummy_customer->ParseFromString(*customer_value));
        delete dummy_customer;
      }
    }

    // Expect all stock to be there
    for (int j = 0; j < NUMBER_OF_ITEMS; j++) {
      char item_key[128], stock_key[128];
      Value* stock_value;
      snprintf(item_key, sizeof(item_key), "i%d", j);
      snprintf(stock_key, sizeof(stock_key), "%ss%s",
               warehouse_key, item_key);
      stock_value = simple_store->ReadObject(stock_key);

      Stock* dummy_stock = new Stock();
      EXPECT_TRUE(dummy_stock->ParseFromString(*stock_value));
      delete dummy_stock;
    }
  }

  // Expect all items to be there
  for (int i = 0; i < NUMBER_OF_ITEMS; i++) {
      char item_key[128];
      Value item_value;
      snprintf(item_key, sizeof(item_key), "i%d", i);
      item_value = *(tpcc->GetItem(string(item_key)));

      Item* dummy_item = new Item();
      EXPECT_TRUE(dummy_item->ParseFromString(item_value));
      delete dummy_item;
  }

  END;
}

TEST(NewOrderTest) {
  // Txn args w/number of warehouses
  TPCCArgs* txn_args = new TPCCArgs();
  txn_args->set_multipartition(false);
  txn_args->set_system_time(GetTime());
  Value txn_args_value;
  assert(txn_args->SerializeToString(&txn_args_value));

  // Do work here to confirm new orders are satisfying TPC-C standards
  TxnProto* txn;
  bool invalid;
  do {
    txn = tpcc->NewTxn(2, TPCC::NEW_ORDER, txn_args_value, config);
    assert(txn_args->ParseFromString(txn->arg()));
    invalid = false;
    for (int i = 0; i < txn_args->order_line_count(); i++) {
      if (txn->read_write_set(i + 1).find("i-1") != string::npos)
        invalid = true;
    }
  } while (invalid);

  txn->add_readers(0);
  txn->add_writers(0);
  StorageManager* storage = new StorageManager(config, connection, simple_store,
                                               txn);

  // Prefetch some values in order to ensure our ACIDity after
  District *district = new District();
  Value* district_value;
  district_value = storage->ReadObject(txn->read_write_set(0));
  assert(district->ParseFromString(*district_value));

  // Prefetch the stocks
  Stock* old_stocks[txn_args->order_line_count()];
  for (int i = 0; i < txn_args->order_line_count(); i++) {
    Value* stock_value;
    Stock* stock = new Stock();
    stock_value = storage->ReadObject(txn->read_write_set(i + 1));
    assert(stock->ParseFromString(*stock_value));
    old_stocks[i] = stock;
  }

  // Prefetch the actual values
  int old_next_order_id = district->next_order_id();

  // Execute the transaction
  tpcc->Execute(txn, storage);

  // Let's prefetch the keys we need for the post-check
  Key district_key = txn->read_write_set(0);
  Key new_order_key = txn->write_set(txn_args->order_line_count());
  Key order_key = txn->write_set(txn_args->order_line_count() + 1);

  // Add in all the keys and re-initialize the storage manager
  txn->add_read_set(new_order_key);
  txn->add_read_set(order_key);
  for (int i = 0; i < txn_args->order_line_count(); i++) {
    txn->add_read_set(txn->write_set(i));
  }
  delete storage;
  storage = new StorageManager(config, connection, simple_store, txn);

  // Ensure that D_NEXT_O_ID is incremented for district
  district_value = storage->ReadObject(district_key);
  assert(district->ParseFromString(*district_value));
  EXPECT_EQ(old_next_order_id + 1, district->next_order_id());

  // TPCC::NEW_ORDER row was inserted with appropriate fields
  Value* new_order_value;
  NewOrder* new_order = new NewOrder();
  new_order_value = storage->ReadObject(new_order_key);
  EXPECT_TRUE(new_order->ParseFromString(*new_order_value));

  // ORDER row was inserted with appropriate fields
  Value* order_value;
  Order* order = new Order();
  order_value = storage->ReadObject(order_key);
  EXPECT_TRUE(order->ParseFromString(*order_value));

  // For each item in O_OL_CNT
  for (int i = 0; i < txn_args->order_line_count(); i++) {
    Value* stock_value;
    Stock* stock = new Stock();
    stock_value = storage->ReadObject(txn->read_write_set(i + 1));
    EXPECT_TRUE(stock->ParseFromString(*stock_value));

    // Check YTD, order_count, and remote_count
    int corrected_year_to_date = old_stocks[i]->year_to_date();
    for (int j = 0; j < txn_args->order_line_count(); j++) {
      if (txn->read_write_set(j + 1) == txn->read_write_set(i + 1))
        corrected_year_to_date += txn_args->quantities(j);
    }
    EXPECT_EQ(stock->year_to_date(), corrected_year_to_date);

    // Check order_count
    int corrected_order_count = old_stocks[i]->order_count();
    for (int j = 0; j < txn_args->order_line_count(); j++) {
      if (txn->read_write_set(j + 1) == txn->read_write_set(i + 1))
        corrected_order_count--;
    }
    EXPECT_EQ(stock->order_count(), corrected_order_count);

    // Check remote_count
    if (txn->multipartition()) {
      int corrected_remote_count = old_stocks[i]->remote_count();
      for (int j = 0; j < txn_args->order_line_count(); j++) {
        if (txn->read_write_set(j + 1) == txn->read_write_set(i + 1))
          corrected_remote_count++;
      }
      EXPECT_EQ(stock->remote_count(), corrected_remote_count);
    }

    // Check stock supply decrease
    int corrected_quantity = old_stocks[i]->quantity();
    for (int j = 0; j < txn_args->order_line_count(); j++) {
      if (txn->read_write_set(j + 1) == txn->read_write_set(i + 1)) {
        if (old_stocks[i]->quantity() >= txn_args->quantities(i) + 10)
          corrected_quantity -= txn_args->quantities(j);
        else
          corrected_quantity -= txn_args->quantities(j) - 91;
      }
    }
    EXPECT_EQ(stock->quantity(), corrected_quantity);

    // First, we check if the item is valid
    size_t item_idx = txn->read_write_set(i + 1).find("i");
    Key item_key = txn->read_write_set(i + 1).substr(item_idx, string::npos);
    Value item_value = *(tpcc->GetItem(item_key));
    Item* item = new Item();
    EXPECT_TRUE(item->ParseFromString(item_value));

    // Check the order line
    // TODO(Thad): Get order_line_ptr from Order protobuf and deserialize from
    // there
//    Value* order_line_value;
//    OrderLine* order_line = new OrderLine();
//    order_line_value = storage->ReadObject(txn->write_set(i));
//    EXPECT_TRUE(order_line->ParseFromString(*order_line_value));
//    EXPECT_EQ(order_line->amount(), item->price() * txn_args->quantities(i));
//    EXPECT_EQ(order_line->number(), i);

    // Free memory
//    delete order_line;
    delete item;
    delete stock;
  }

  // Free memory
  for (int i = 0; i < txn_args->order_line_count(); i++)
    delete old_stocks[i];
  delete txn_args;
  delete storage;
  delete district;
  delete order;
  delete new_order;
  delete txn;

  END
}

TEST(PaymentTest) {
  // Txn args w/number of warehouses
  TPCCArgs* txn_args = new TPCCArgs();
  txn_args->set_multipartition(false);
  txn_args->set_system_time(GetTime());
  Value txn_args_value;
  assert(txn_args->SerializeToString(&txn_args_value));

  // Do work here to confirm payment transactions are satisfying standards
  TxnProto* txn = new TxnProto();
  do {
    delete txn;
    txn = tpcc->NewTxn(4, TPCC::PAYMENT, txn_args_value, config);
    assert(txn_args->ParseFromString(txn->arg()));
  } while (txn->read_write_set_size() < 3);
  txn->add_read_set(txn->write_set(0));
  txn->add_readers(0);
  txn->add_writers(0);
  StorageManager* storage = new StorageManager(config, connection, simple_store,
                                               txn);

  // Prefetch some values in order to ensure our ACIDity after
  Warehouse *warehouse = new Warehouse();
  Value* warehouse_value;
  warehouse_value = storage->ReadObject(txn->read_write_set(0));
  assert(warehouse->ParseFromString(*warehouse_value));
  int old_warehouse_year_to_date = warehouse->year_to_date();

  // Prefetch district
  District *district = new District();
  Value* district_value;
  district_value = storage->ReadObject(txn->read_write_set(1));
  assert(district->ParseFromString(*district_value));
  int old_district_year_to_date = district->year_to_date();

  // Preetch customer
  Customer *customer = new Customer();
  Value* customer_value;
  customer_value = storage->ReadObject(txn->read_write_set(2));
  assert(customer->ParseFromString(*customer_value));
  int old_customer_year_to_date_payment = customer->year_to_date_payment();
  int old_customer_balance = customer->balance();
  int old_customer_payment_count = customer->payment_count();

  // Execute the transaction
  tpcc->Execute(txn, storage);

  // Get the data back from the database
  delete storage;
  storage = new StorageManager(config, connection, simple_store, txn);

  warehouse_value = storage->ReadObject(txn->read_write_set(0));
  assert(warehouse->ParseFromString(*warehouse_value));
  district_value = storage->ReadObject(txn->read_write_set(1));
  assert(district->ParseFromString(*district_value));
  customer_value = storage->ReadObject(txn->read_write_set(2));
  assert(customer->ParseFromString(*customer_value));

  // Check the old values against the new
  EXPECT_EQ(warehouse->year_to_date(), old_warehouse_year_to_date +
            txn_args->amount());
  EXPECT_EQ(district->year_to_date(), old_district_year_to_date +
            txn_args->amount());
  EXPECT_EQ(customer->year_to_date_payment(),
            old_customer_year_to_date_payment + txn_args->amount());
  EXPECT_EQ(customer->balance(), old_customer_balance - txn_args->amount());
  EXPECT_EQ(customer->payment_count(), old_customer_payment_count + 1);

  // Ensure the history record is valid
  History* history = new History();
  Value* history_value;
  history_value = storage->ReadObject(txn->read_set(0));
  EXPECT_TRUE(history->ParseFromString(*history_value));
  EXPECT_EQ(history->warehouse_id(), warehouse->id());
  EXPECT_EQ(history->district_id(), district->id());
  EXPECT_EQ(history->customer_id(), customer->id());
  EXPECT_EQ(history->customer_warehouse_id(), customer->warehouse_id());
  EXPECT_EQ(history->customer_district_id(), customer->district_id());

  // Free memory
  delete history;
  delete warehouse;
  delete customer;
  delete district;
  delete storage;
  delete txn_args;
  delete txn;

  END
}

TEST(MultipleTxnTest) {
  StorageManager* storage;
  TPCCArgs args;
  args.set_system_time(GetTime());
  args.set_multipartition(false);
  string args_string;
  args.SerializeToString(&args_string);

  TxnProto* txn = tpcc->NewTxn(0, TPCC::INITIALIZE, args_string, config);
  storage = new StorageManager(config, connection, simple_store, txn);
  tpcc->Execute(txn, storage);
  delete storage;
  delete txn;

  txn = tpcc->NewTxn(1, TPCC::NEW_ORDER, args_string, config);
  storage = new StorageManager(config, connection, simple_store, txn);
  tpcc->Execute(txn, storage);
  delete storage;
  delete txn;

  END;
}

int main(int argc, char** argv) {
  config = new Configuration(0, "common/configuration_test_one_node.conf");
  multiplexer = new ConnectionMultiplexer(config);
  connection = multiplexer->NewConnection("asdf");
  simple_store = new SimpleStorage();
  tpcc = new TPCC();

  InitializeTest();

  IdGenerationTest();

  WarehouseTest();
  DistrictTest();
  CustomerTest();
  ItemTest();
  StockTest();

  NewTxnTest();
  NewOrderTest();
  PaymentTest();

  // MultipleTxnTest();

  delete tpcc;
  delete simple_store;
  delete connection;
  delete multiplexer;
  delete config;

  return 0;
}

