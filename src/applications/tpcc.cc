// Author: Kun Ren (kun.ren@yale.edu)
// Author: Thaddeus Diamond (diamond@cs.yale.edu)
//
//
// A concrete implementation of TPC-C (application subclass)

#include "applications/tpcc.h"

#include <set>
#include <string>

#include "backend/storage.h"
#include "backend/storage_manager.h"
#include "common/configuration.h"
#include "common/utils.h"
#include "proto/tpcc.pb.h"
#include "proto/tpcc_args.pb.h"

using std::string;

// ---- THIS IS A HACK TO MAKE ITEMS WORK ON LOCAL MACHINE ---- //
unordered_map<Key, Value*> ItemList;
Value* TPCC::GetItem(Key key) const             { return ItemList[key]; }
void TPCC::SetItem(Key key, Value* value) const { ItemList[key] = value; }

// The load generator can be called externally to return a
// transaction proto containing a new type of transaction.
TxnProto* TPCC::NewTxn(int64 txn_id, int txn_type, string args,
                       Configuration* config) const {
  // Create the new transaction object
  TxnProto* txn = new TxnProto();

  // Set the transaction's standard attributes
  txn->set_txn_id(txn_id);
  txn->set_txn_type(txn_type);
  txn->set_isolation_level(TxnProto::SERIALIZABLE);
  txn->set_status(TxnProto::NEW);
  txn->set_multipartition(false);

  // Parse out the arguments to the transaction
  TPCCArgs* txn_args = new TPCCArgs();
  assert(txn_args->ParseFromString(args));
  bool mp = txn_args->multipartition();
  int remote_node;
  if (mp) {
    do {
      remote_node = rand() % config->all_nodes.size();
    } while (config->all_nodes.size() > 1 &&
             remote_node == config->this_node_id);
  }

  // Create an arg list
  TPCCArgs* tpcc_args = new TPCCArgs();
  tpcc_args->set_system_time(GetTime());

  // Because a switch is not scoped we declare our variables outside of it
  int warehouse_id, district_id, customer_id;
  char warehouse_key[128], district_key[128], customer_key[128];
  int order_line_count;
  bool invalid;
  Value customer_value;
  std::set<int> items_used;

  // We set the read and write set based on type
  switch (txn_type) {
    // Initialize
    case INITIALIZE:
      // Finished with INITIALIZE txn creation
      break;

    // New Order
    case NEW_ORDER:
      // First, we pick a local warehouse
        warehouse_id = (rand() % WAREHOUSES_PER_NODE) * config->all_nodes.size() + config->this_node_id;
        snprintf(warehouse_key, sizeof(warehouse_key), "w%d",
                 warehouse_id);

      txn->add_read_set(warehouse_key);


      // Next, we pick a random district
      district_id = rand() % DISTRICTS_PER_WAREHOUSE;
      snprintf(district_key, sizeof(district_key), "w%dd%d",
               warehouse_id, district_id);
      txn->add_read_write_set(district_key);


      // Finally, we pick a random customer
      customer_id = rand() % CUSTOMERS_PER_DISTRICT;
      snprintf(customer_key, sizeof(customer_key),
               "w%dd%dc%d",
               warehouse_id, district_id, customer_id);
      txn->add_read_set(customer_key);

      int order_number;
      if(next_order_id_for_district.count(district_key)>0) {
        order_number = next_order_id_for_district[district_key];
        next_order_id_for_district[district_key] ++;
      } else { 
        order_number = 0;
        next_order_id_for_district[district_key] = 1;
      }

      // We set the length of the read and write set uniformly between 5 and 15
      order_line_count = (rand() % 11) + 5;

      // Let's choose a bad transaction 1% of the time
      invalid = false;
//      if (rand() / (static_cast<double>(RAND_MAX + 1.0)) <= 0.00)
//        invalid = true;

      // Iterate through each order line
      for (int i = 0; i < order_line_count; i++) {
        // Set the item id (Invalid orders have the last item be -1)
        int item;
        do {
          item = rand() % NUMBER_OF_ITEMS;
        } while (items_used.count(item) > 0);
        items_used.insert(item);

        if (invalid && i == order_line_count - 1)
          item = -1;

        // Print the item key into a buffer
        char item_key[128];
        snprintf(item_key, sizeof(item_key), "i%d", item);

        // Create an order line warehouse key (default is local)
        char remote_warehouse_key[128];
        snprintf(remote_warehouse_key, sizeof(remote_warehouse_key),
                 "%s", warehouse_key);

        // We only do ~1% remote transactions
        if (mp) {
          txn->set_multipartition(true);

          // We loop until we actually get a remote one
          int remote_warehouse_id;
          do {
            remote_warehouse_id = rand() % (WAREHOUSES_PER_NODE *
                                            config->all_nodes.size());
            snprintf(remote_warehouse_key, sizeof(remote_warehouse_key),
                     "w%d", remote_warehouse_id);
          } while (config->all_nodes.size() > 1 &&
                   config->LookupPartition(remote_warehouse_key) !=
                     remote_node);
        }

        // Determine if we should add it to read set to avoid duplicates
        bool needed = true;
        for (int j = 0; j < txn->read_set_size(); j++) {
          if (txn->read_set(j) == remote_warehouse_key)
            needed = false;
        }
        if (needed)
          txn->add_read_set(remote_warehouse_key);

        // Finally, we set the stock key to the read and write set
        Key stock_key = string(remote_warehouse_key) + "s" + item_key;
        txn->add_read_write_set(stock_key);

        // Set the quantity randomly within [1..10]
        tpcc_args->add_quantities(rand() % 10 + 1);

        // Finally, we add the order line key to the write set
        char order_line_key[128];
        snprintf(order_line_key, sizeof(order_line_key), "%so%dol%d",
                 district_key, order_number, i);
        txn->add_write_set(order_line_key);

      }

      // Create a new order key to add to write set
      char new_order_key[128];
      snprintf(new_order_key, sizeof(new_order_key),
               "%sno%d", district_key, order_number);
      txn->add_write_set(new_order_key);

      // Create an order key to add to write set
      char order_key[128];
      snprintf(order_key, sizeof(order_key), "%so%d",
               district_key, order_number);
      txn->add_write_set(order_key);

      // Set the order line count in the args
      tpcc_args->add_order_line_count(order_line_count);
      tpcc_args->set_order_number(order_number);
      break;

    // Payment
    case PAYMENT:
      // Specify an amount for the payment
      tpcc_args->set_amount(rand() / (static_cast<double>(RAND_MAX + 1.0)) *
                            4999.0 + 1);

      // First, we pick a local warehouse

      warehouse_id = (rand() % WAREHOUSES_PER_NODE) * config->all_nodes.size() + config->this_node_id;
      snprintf(warehouse_key, sizeof(warehouse_key), "w%dy",
                 warehouse_id);
      txn->add_read_write_set(warehouse_key);

      // Next, we pick a district
      district_id = rand() % DISTRICTS_PER_WAREHOUSE;
      snprintf(district_key, sizeof(district_key), "w%dd%dy",
               warehouse_id, district_id);
      txn->add_read_write_set(district_key);

      // Add history key to write set
      char history_key[128];
      snprintf(history_key, sizeof(history_key), "w%dh%ld",
               warehouse_id, txn->txn_id());
      txn->add_write_set(history_key);

      // Next, we find the customer as a local one
      if (WAREHOUSES_PER_NODE * config->all_nodes.size() == 1 || !mp) {
        customer_id = rand() % CUSTOMERS_PER_DISTRICT;
        snprintf(customer_key, sizeof(customer_key),
                 "w%dd%dc%d",
                 warehouse_id, district_id, customer_id);

      // If the probability is 15%, we make it a remote customer
      } else {
        int remote_warehouse_id;
        int remote_district_id;
        int remote_customer_id;
        char remote_warehouse_key[40];
        do {
          remote_warehouse_id = rand() % (WAREHOUSES_PER_NODE *
                                          config->all_nodes.size());
          snprintf(remote_warehouse_key, sizeof(remote_warehouse_key), "w%d",
                   remote_warehouse_id);

          remote_district_id = rand() % DISTRICTS_PER_WAREHOUSE;

          remote_customer_id = rand() % CUSTOMERS_PER_DISTRICT;
          snprintf(customer_key, sizeof(customer_key), "w%dd%dc%d",
                   remote_warehouse_id, remote_district_id, remote_customer_id);
        } while (config->all_nodes.size() > 1 &&
                 config->LookupPartition(remote_warehouse_key) != remote_node);
      }

      // We only do secondary keying ~60% of the time
      if (rand() / (static_cast<double>(RAND_MAX + 1.0)) < 0.00) {
        // Now that we have the object, let's create the txn arg
        tpcc_args->set_last_name(customer_key);
        txn->add_read_set(customer_key);

      // Otherwise just give a customer key
      } else {
        txn->add_read_write_set(customer_key);
      }

      break;

     case ORDER_STATUS :
     {
       string customer_string;
       string customer_latest_order;
       string warehouse_string;
       string district_string;
       int customer_order_line_number;

       if(latest_order_id_for_customer.size() < 1) {
         txn->set_txn_id(-1);
         break;
       }

       pthread_mutex_lock(&mutex_);
       customer_string = (*involed_customers)[rand() % involed_customers->size()];
       pthread_mutex_unlock(&mutex_);
       customer_latest_order = latest_order_id_for_customer[customer_string];
       warehouse_string = customer_string.substr(0,customer_string.find("d"));
       district_string = customer_string.substr(0,customer_string.find("c"));
       snprintf(customer_key, sizeof(customer_key), "%s", customer_string.c_str());
       snprintf(warehouse_key, sizeof(warehouse_key), "%s",warehouse_string.c_str());
       snprintf(district_key, sizeof(district_key), "%s",district_string.c_str());

       customer_order_line_number = order_line_number[customer_latest_order];
       txn->add_read_set(warehouse_key);
       txn->add_read_set(district_key);
       txn->add_read_set(customer_key);

       snprintf(order_key, sizeof(order_key), "%s",customer_latest_order.c_str());
       txn->add_read_set(order_key);
       char order_line_key[128];
       for(int i = 0; i < customer_order_line_number; i++) {
         snprintf(order_line_key, sizeof(order_line_key), "%sol%d", order_key, i);
         txn->add_read_set(order_line_key);
       }

       tpcc_args->add_order_line_count(customer_order_line_number);

      break;
    }


      case STOCK_LEVEL:
      {
        warehouse_id = (rand() % WAREHOUSES_PER_NODE) * config->all_nodes.size() + config->this_node_id;
        snprintf(warehouse_key, sizeof(warehouse_key), "w%d",warehouse_id);
            
        // Next, we pick a random district
        district_id = rand() % DISTRICTS_PER_WAREHOUSE;
        snprintf(district_key, sizeof(district_key), "w%dd%d",warehouse_id, district_id);

       if(latest_order_id_for_district.count(district_key) == 0) {
         txn->set_txn_id(-1);
         break;
       } 
       
       txn->add_read_set(warehouse_key);
       txn->add_read_set(district_key);
       int latest_order_number = latest_order_id_for_district[district_key];
       char order_line_key[128];
       char stock_key[128];

       tpcc_args->set_lastest_order_number(latest_order_number);
       tpcc_args->set_threshold(rand()%10 + 10);

       for(int i = latest_order_number; (i >= 0) && (i > latest_order_number - 20); i--) {
         snprintf(order_key, sizeof(order_key),
                  "%so%d", district_key, i);
         int ol_number = order_line_number[order_key];

         for(int j = 0; j < ol_number;j++) {
           snprintf(order_line_key, sizeof(order_line_key), "%sol%d",
                    order_key, j);
           int item = item_for_order_line[order_line_key];
           if(items_used.count(item) > 0) {
             continue;
           }
           items_used.insert(item);
           txn->add_read_set(order_line_key);
           snprintf(stock_key, sizeof(stock_key), "%ssi%d",
                    warehouse_key, item);
           txn->add_read_set(stock_key);
         }
       }

       break;
     }

       case DELIVERY :
       {
           
         warehouse_id = (rand() % WAREHOUSES_PER_NODE) * config->all_nodes.size() + config->this_node_id;
         snprintf(warehouse_key, sizeof(warehouse_key), "w%d", warehouse_id);
         txn->add_read_set(warehouse_key);
         
         char order_line_key[128];
         int oldest_order;
       
         for(int i = 0; i < DISTRICTS_PER_WAREHOUSE; i++) {
           snprintf(district_key, sizeof(district_key), "%sd%d", warehouse_key, i); 
           if((smallest_order_id_for_district.count(district_key) == 0) || (smallest_order_id_for_district[district_key] > latest_order_id_for_district[district_key])){
             continue;
           } else {
             txn->add_read_set(district_key);
             oldest_order = smallest_order_id_for_district[district_key];
             smallest_order_id_for_district[district_key] ++;

             snprintf(new_order_key, sizeof(new_order_key), "%sno%d", district_key, oldest_order);
             txn->add_read_write_set(new_order_key);
           }

           snprintf(order_key, sizeof(order_key), "%so%d", district_key, oldest_order);
           txn->add_read_write_set(order_key);
           int ol_number = order_line_number[order_key];
           tpcc_args->add_order_line_count(ol_number);

           for(int j = 0; j < ol_number; j++) {
             snprintf(order_line_key, sizeof(order_line_key), "%sol%d", order_key, j);
             txn->add_read_write_set(order_line_key);
           }

           snprintf(customer_key, sizeof(customer_key), "%s", customer_for_order[order_key].c_str());
           txn->add_read_write_set(customer_key);
         }
     
         break;
       }

    // Invalid transaction
    default:
      break;
  }

  // Set the transaction's args field to a serialized version
  Value args_string;
  assert(tpcc_args->SerializeToString(&args_string));
  txn->set_arg(args_string);

  // Free memory
  delete tpcc_args;
  delete txn_args;

  return txn;
}

// The execute function takes a single transaction proto and executes it based
// on what the type of the transaction is.
int TPCC::Execute(TxnProto* txn, StorageManager* storage) const {
  switch (txn->txn_type()) {
    // Initialize
    case INITIALIZE:
      InitializeStorage(storage->GetStorage(), NULL);
      return SUCCESS;
      break;

    // New Order
    case NEW_ORDER:
      return NewOrderTransaction(txn, storage);
      break;

    // Payment
    case PAYMENT:
      return PaymentTransaction(txn, storage);
      break;

    case ORDER_STATUS:
      return OrderStatusTransaction(txn, storage);
      break;

    case STOCK_LEVEL:
      return StockLevelTransaction(txn, storage);
      break;

    case DELIVERY:
      return DeliveryTransaction(txn, storage);
      break;

    // Invalid transaction
    default:
      return FAILURE;
      break;
  }

  return FAILURE;
}

// The new order function is executed when the application receives a new order
// transaction.  This follows the TPC-C standard.
int TPCC::NewOrderTransaction(TxnProto* txn, StorageManager* storage) const {
  // First, we retrieve the warehouse from storage
  Value* warehouse_value;
  Warehouse* warehouse = new Warehouse();
  warehouse_value = storage->ReadObject(txn->read_set(0));
  assert(warehouse->ParseFromString(*warehouse_value));

  // Next, we retrieve the district
  District* district = new District();
  Value* district_value = storage->ReadObject(txn->read_write_set(0));
  assert(district->ParseFromString(*district_value));
  // Increment the district's next order ID and put to datastore
  district->set_next_order_id(district->next_order_id() + 1);
  assert(district->SerializeToString(district_value));
  // Not necessary since storage already has a pointer to district_value.
  //   storage->PutObject(district->id(), district_value);

  // Retrieve the customer we are looking for
  Value* customer_value;
  Customer* customer = new Customer();
  customer_value = storage->ReadObject(txn->read_set(1));
  assert(customer->ParseFromString(*customer_value));

  // Next, we get the order line count, system time, and other args from the
  // transaction proto
  TPCCArgs* tpcc_args = new TPCCArgs();
  tpcc_args->ParseFromString(txn->arg());
  int order_line_count = tpcc_args->order_line_count(0);
  int order_number =  tpcc_args->order_number();
  double system_time = tpcc_args->system_time();

  // Next we create an Order object
  Key order_key = txn->write_set(order_line_count + 1);
  Order* order = new Order();
  order->set_id(order_key);
  order->set_warehouse_id(warehouse->id());
  order->set_district_id(district->id());
  order->set_customer_id(customer->id());

  // Set some of the auxiliary data
  order->set_entry_date(system_time);
  order->set_carrier_id(-1);
  order->set_order_line_count(order_line_count);
  order->set_all_items_local(txn->multipartition());

  // We initialize the order line amount total to 0
  int order_line_amount_total = 0;

  for (int i = 0; i < order_line_count; i++) {
    // For each order line we parse out the three args

    string stock_key = txn->read_write_set(i + 1);
    string supply_warehouse_key = stock_key.substr(0, stock_key.find("s"));
    int quantity = tpcc_args->quantities(i);

    // Find the item key within the stock key
    size_t item_idx = stock_key.find("i");
    string item_key = stock_key.substr(item_idx, string::npos);

    // First, we check if the item number is valid
    Item* item = new Item();
    if (item_key == "i-1")
      return FAILURE;
    else
      assert(item->ParseFromString(*GetItem(item_key)));

    // Next, we create a new order line object with std attributes
    OrderLine* order_line = new OrderLine();
    Key order_line_key = txn->write_set(i);
    order_line->set_order_id(order_line_key);

    // Set the attributes for this order line
    order_line->set_district_id(district->id());
    order_line->set_warehouse_id(warehouse->id());
    order_line->set_number(i);
    order_line->set_item_id(item_key);
    order_line->set_supply_warehouse_id(supply_warehouse_key);
    order_line->set_quantity(quantity);
    order_line->set_delivery_date(system_time);

    // Next, we get the correct stock from the data store
    Stock* stock = new Stock();
    Value* stock_value = storage->ReadObject(stock_key);
    assert(stock->ParseFromString(*stock_value));

    // Once we have it we can increase the YTD, order_count, and remote_count
    stock->set_year_to_date(stock->year_to_date() + quantity);
    stock->set_order_count(stock->order_count() - 1);
    if (txn->multipartition())
      stock->set_remote_count(stock->remote_count() + 1);

    // And we decrease the stock's supply appropriately and rewrite to storage
    if (stock->quantity() >= quantity + 10)
      stock->set_quantity(stock->quantity() - quantity);
    else
      stock->set_quantity(stock->quantity() - quantity + 91);

    // Put the stock back into the database
    assert(stock->SerializeToString(stock_value));
    // Not necessary since storage already has a ptr to stock_value.
    //   storage->PutObject(stock_key, stock_value);
    delete stock;

    // Next, we update the order line's amount and add it to the running sum
    order_line->set_amount(quantity * item->price());
    order_line_amount_total += (quantity * item->price());

    // Finally, we write the order line to storage
    Value* order_line_value = new Value();
    assert(order_line->SerializeToString(order_line_value));
    storage->PutObject(order_line_key, order_line_value);
    //order->add_order_line_ptr(reinterpret_cast<uint64>(order_line_value));

    pthread_mutex_lock(&mutex_for_item);
    if (storage->configuration_->this_node_id == storage->configuration_->LookupPartition(txn->read_set(0)))
      item_for_order_line[order_line_key] = StringToInt(item_key);
    pthread_mutex_unlock(&mutex_for_item);
    // Free memory
    delete order_line;
    delete item;
  }

  // We create a new NewOrder object
  Key new_order_key = txn->write_set(order_line_count);
  NewOrder* new_order = new NewOrder();
  new_order->set_id(new_order_key);
  new_order->set_warehouse_id(warehouse->id());
  new_order->set_district_id(district->id());

  // Serialize it and put it in the datastore
  Value* new_order_value = new Value();
  assert(new_order->SerializeToString(new_order_value));
  storage->PutObject(new_order_key, new_order_value);

  // Serialize order and put it in the datastore
  Value* order_value = new Value();
  assert(order->SerializeToString(order_value));
  storage->PutObject(order_key, order_value);

  if(storage->configuration_->this_node_id == storage->configuration_->LookupPartition(txn->read_set(0))) {
    pthread_mutex_lock(&mutex_);
    if(latest_order_id_for_customer.count(txn->read_set(1)) == 0)
      involed_customers->push_back(txn->read_set(1));
    latest_order_id_for_customer[txn->read_set(1)] = order_key;

    if(smallest_order_id_for_district.count(txn->read_write_set(0)) == 0) {
      smallest_order_id_for_district[txn->read_write_set(0)] = order_number;
    }
    order_line_number[order_key] =  order_line_count;
    customer_for_order[order_key] = txn->read_set(1);

    latest_order_id_for_district[txn->read_write_set(0)] = order_number;
    pthread_mutex_unlock(&mutex_);
  }

  // Successfully completed transaction
  delete warehouse;
  delete district;
  delete customer;
  delete order;
  delete new_order;
  delete tpcc_args;
  return SUCCESS;
}

// The payment function is executed when the application receives a
// payment transaction.  This follows the TPC-C standard.
int TPCC::PaymentTransaction(TxnProto* txn, StorageManager* storage) const {
  // First, we parse out the transaction args from the TPCC proto
  TPCCArgs* tpcc_args = new TPCCArgs();
  tpcc_args->ParseFromString(txn->arg());
  int amount = tpcc_args->amount();

  // We create a string to hold up the customer object we look up
  Value* customer_value;
  Key customer_key;

  // If there's a last name we do secondary keying
  if (tpcc_args->has_last_name()) {
    Key secondary_key = tpcc_args->last_name();

    // If the RW set is not at least of size 3, then no customer key was
    // given to this transaction.  Otherwise, we perform a check to ensure
    // the secondary key we just looked up agrees with our previous lookup
    if (txn->read_write_set_size() < 3 || secondary_key != txn->read_write_set(2)) {
      // Append the newly read key to write set
      if (txn->read_write_set_size() < 3)
        txn->add_read_write_set(secondary_key);

      // Or the old one was incorrect so we overwrite it
      else
        txn->set_read_write_set(2, secondary_key);

      return REDO;
    // Otherwise, we look up the customer's key
    } else {
      customer_value = storage->ReadObject(tpcc_args->last_name());
    }

  // Otherwise we use the final argument
  } else {
    customer_key = txn->read_write_set(2);
    customer_value = storage->ReadObject(customer_key);
  }

  // Deserialize the warehouse object
  Key warehouse_key = txn->read_write_set(0);
  Value* warehouse_value = storage->ReadObject(warehouse_key);
  Warehouse* warehouse = new Warehouse();
  assert(warehouse->ParseFromString(*warehouse_value));

  // Next, we update the values of the warehouse and write it out
  warehouse->set_year_to_date(warehouse->year_to_date() + amount);
  assert(warehouse->SerializeToString(warehouse_value));
  // Not necessary since storage already has a pointer to warehouse_value.
  //   storage->PutObject(warehouse_key, warehouse_value);

  // Deserialize the district object
  Key district_key = txn->read_write_set(1);
  Value* district_value = storage->ReadObject(district_key);
  District* district = new District();
  assert(district->ParseFromString(*district_value));

  // Next, we update the values of the district and write it out
  district->set_year_to_date(district->year_to_date() + amount);
  assert(district->SerializeToString(district_value));
  // Not necessary since storage already has a pointer to district_value.
  //   storage->PutObject(district_key, district_value);

  // We deserialize the customer
  Customer* customer = new Customer();
  assert(customer->ParseFromString(*customer_value));

  // Next, we update the customer's balance, payment and payment count
  customer->set_balance(customer->balance() - amount);
  customer->set_year_to_date_payment(customer->year_to_date_payment() + amount);
  customer->set_payment_count(customer->payment_count() + 1);

  // If the customer has bad credit, we update the data information attached
  // to her
  if (customer->credit() == "BC") {
    string data = customer->data();
    char new_information[500];

    // Print the new_information into the buffer
    snprintf(new_information, sizeof(new_information), "%s%s%s%s%s%d%s",
             customer->id().c_str(), customer->warehouse_id().c_str(),
             customer->district_id().c_str(), district->id().c_str(),
             warehouse->id().c_str(), amount, customer->data().c_str());
  }

  // We write the customer to disk
  assert(customer->SerializeToString(customer_value));
  // Not necessary since storage already has a pointer to customer_value.
  //   storage->PutObject(customer_key, customer_value);

  // Finally, we create a history object and update the data
  History* history = new History();
  history->set_customer_id(customer_key);
  history->set_customer_warehouse_id(customer->warehouse_id());
  history->set_customer_district_id(customer->district_id());
  history->set_warehouse_id(warehouse_key);
  history->set_district_id(district_key);

  // Create the data for the history object
  char history_data[100];
  snprintf(history_data, sizeof(history_data), "%s    %s",
             warehouse->name().c_str(), district->name().c_str());
  history->set_data(history_data);

  // Write the history object to disk
  Value* history_value = new Value();
  assert(history->SerializeToString(history_value));
  storage->PutObject(txn->write_set(0), history_value);

  // Successfully completed transaction
  delete customer;
  delete history;
  delete district;
  delete warehouse;
  delete tpcc_args;
  return SUCCESS;
}


int TPCC::OrderStatusTransaction(TxnProto* txn, StorageManager* storage) const {
  TPCCArgs* tpcc_args = new TPCCArgs();
  tpcc_args->ParseFromString(txn->arg());
  int order_line_count = tpcc_args->order_line_count(0);

  Value* warehouse_value;
  Warehouse* warehouse = new Warehouse();
  warehouse_value = storage->ReadObject(txn->read_set(0));
  assert(warehouse->ParseFromString(*warehouse_value));

  District* district = new District();
  Value* district_value = storage->ReadObject(txn->read_set(1));
  assert(district->ParseFromString(*district_value));

  Value* customer_value;
  Customer* customer = new Customer();
  customer_value = storage->ReadObject(txn->read_set(2));
  assert(customer->ParseFromString(*customer_value));

  //  double customer_balance = customer->balance();
  string customer_first = customer->first();
  string customer_middle = customer->middle();
  string customer_last = customer->last();

  Order* order = new Order();
  Value* order_value = storage->ReadObject(txn->read_set(3));
  assert(order->ParseFromString(*order_value));
  //  int carrier_id = order->carrier_id();
  //  double entry_date = order->entry_date();


  for(int i = 0; i < order_line_count; i++) {
    OrderLine* order_line = new OrderLine();
    Value* order_line_value = storage->ReadObject(txn->read_set(4+i));
    assert(order_line->ParseFromString(*order_line_value));
    string item_key = order_line->item_id();
    string supply_warehouse_id = order_line->supply_warehouse_id();
    //    int quantity = order_line->quantity();
    //    double amount = order_line->amount();
    //    double delivery_date = order_line->delivery_date();
    
    delete order_line;
  }
   
  delete warehouse;
  delete district;
  delete customer;
  delete order;

  return SUCCESS;
}

int TPCC::StockLevelTransaction(TxnProto* txn, StorageManager* storage) const { 
  int low_stock = 0;
  TPCCArgs* tpcc_args = new TPCCArgs();
  tpcc_args->ParseFromString(txn->arg());
  int threshold = tpcc_args->threshold();
 
  Value* warehouse_value;
  Warehouse* warehouse = new Warehouse();
  warehouse_value = storage->ReadObject(txn->read_set(0));
  assert(warehouse->ParseFromString(*warehouse_value));

  District* district = new District();
  Value* district_value = storage->ReadObject(txn->read_set(1));
  assert(district->ParseFromString(*district_value));

  int index = 0;

  int cycle = (txn->read_set_size() - 2)/2;
  for(int i = 0; i < cycle; i++) {
    OrderLine* order_line = new OrderLine();
    Value* order_line_value = storage->ReadObject(txn->read_set(2+index));
    index ++;
    assert(order_line->ParseFromString(*order_line_value));
    string item_key = order_line->item_id();

    Stock* stock = new Stock();
    Value* stock_value = storage->ReadObject(txn->read_set(2+index));
    index ++;
    assert(stock->ParseFromString(*stock_value));
    if(stock->quantity() < threshold) {
      low_stock ++;
    }
  
    delete order_line;
    delete stock; 
  }
 
  delete warehouse;
  delete district;
  return SUCCESS;
}

int TPCC::DeliveryTransaction(TxnProto* txn, StorageManager* storage) const {      
  TPCCArgs* tpcc_args = new TPCCArgs();
  tpcc_args->ParseFromString(txn->arg());

  Value* warehouse_value;
  Warehouse* warehouse = new Warehouse();
  warehouse_value = storage->ReadObject(txn->read_set(0));
  assert(warehouse->ParseFromString(*warehouse_value));

  if(txn->read_set_size() == 1) {
    delete warehouse; 
    return SUCCESS;
  }


  int delivery_district_number = txn->read_set_size() - 1;
  int read_write_index = 0;
  int line_count_index = 0;
  for(int i = 1; i <= delivery_district_number; i++) {
    District* district = new District();
    Value* district_value = storage->ReadObject(txn->read_set(i));
    assert(district->ParseFromString(*district_value));
    
    storage->DeleteObject(txn->read_write_set(read_write_index));
    read_write_index ++;

    Order* order = new Order();
    Value* order_value = storage->ReadObject(txn->read_write_set(read_write_index));
    read_write_index ++;
    assert(order->ParseFromString(*order_value));
    char customer_key[128];
    snprintf(customer_key, sizeof(customer_key),
             "%s", order->customer_id().c_str());

    order->set_carrier_id(rand()%10);

    int ol_number = tpcc_args->order_line_count(line_count_index);
    line_count_index ++;
    double total_amount = 0;

    for(int j = 0; j < ol_number; j++) {
      OrderLine* order_line = new OrderLine();
      Value* order_line_value = storage->ReadObject(txn->read_write_set(read_write_index));
      read_write_index ++;
      assert(order_line->ParseFromString(*order_line_value));
      order_line->set_delivery_date(GetTime());
      total_amount = total_amount + order_line->amount();
      
      delete order_line;
    }
    

    Customer* customer = new Customer();
    Value* customer_value = storage->ReadObject(txn->read_write_set(read_write_index));
    read_write_index ++;
    assert(customer->ParseFromString(*customer_value));
    customer->set_balance(customer->balance() + total_amount);
    customer->set_delivery_count(customer->delivery_count() + 1);  
   
    delete district;
    delete order;
    delete customer;
   
  }

  delete warehouse;   
  return SUCCESS;
}


// The initialize function is executed when an initialize transaction comes
// through, indicating we should populate the database with fake data
void TPCC::InitializeStorage(Storage* storage, Configuration* conf) const {
  // We create and write out all of the warehouses
  for (int i = 0; i < (int)(WAREHOUSES_PER_NODE * conf->all_nodes.size()); i++) {
    // First, we create a key for the warehouse
    char warehouse_key[128], warehouse_key_ytd[128];
    Value* warehouse_value = new Value();
    snprintf(warehouse_key, sizeof(warehouse_key), "w%d", i);
    snprintf(warehouse_key_ytd, sizeof(warehouse_key_ytd), "w%dy", i);
    if (conf->LookupPartition(warehouse_key) != conf->this_node_id) {
      continue; 
    }
    // Next we initialize the object and serialize it
    Warehouse* warehouse = CreateWarehouse(warehouse_key);
    assert(warehouse->SerializeToString(warehouse_value));

    // Finally, we pass it off to the storage manager to write to disk
    if (conf->LookupPartition(warehouse_key) == conf->this_node_id) {
      storage->PutObject(warehouse_key, warehouse_value);
      storage->PutObject(warehouse_key_ytd, new Value(*warehouse_value));
    }

    // Next, we create and write out all of the districts
    for (int j = 0; j < DISTRICTS_PER_WAREHOUSE; j++) {
      // First, we create a key for the district
      char district_key[128], district_key_ytd[128];
      snprintf(district_key, sizeof(district_key), "w%dd%d",
               i, j);
      snprintf(district_key_ytd, sizeof(district_key_ytd), "w%dd%dy",
               i, j);

      // Next we initialize the object and serialize it
      Value* district_value = new Value();
      District* district = CreateDistrict(district_key, warehouse_key);
      assert(district->SerializeToString(district_value));

      // Finally, we pass it off to the storage manager to write to disk
      if (conf->LookupPartition(district_key) == conf->this_node_id) {
        storage->PutObject(district_key, district_value);
        storage->PutObject(district_key_ytd, new Value(*district_value));
      }

      // Next, we create and write out all of the customers
      for (int k = 0; k < CUSTOMERS_PER_DISTRICT; k++) {
        // First, we create a key for the customer
        char customer_key[128];
        snprintf(customer_key, sizeof(customer_key),
                 "w%dd%dc%d", i, j, k);

        // Next we initialize the object and serialize it
        Value* customer_value = new Value();
        Customer* customer = CreateCustomer(customer_key, district_key,
          warehouse_key);
        assert(customer->SerializeToString(customer_value));

        // Finally, we pass it off to the storage manager to write to disk
        if (conf->LookupPartition(customer_key) == conf->this_node_id)
          storage->PutObject(customer_key, customer_value);
        delete customer;
      }

      // Free storage
      delete district;
    }

    // Next, we create and write out all of the stock
    for (int j = 0; j < NUMBER_OF_ITEMS; j++) {
      // First, we create a key for the stock
      char item_key[128];
      Value* stock_value = new Value();
      snprintf(item_key, sizeof(item_key), "i%d", j);

      // Next we initialize the object and serialize it
      Stock* stock = CreateStock(item_key, warehouse_key);
      assert(stock->SerializeToString(stock_value));

      // Finally, we pass it off to the storage manager to write to disk
      if (conf->LookupPartition(stock->id()) == conf->this_node_id)
        storage->PutObject(stock->id(), stock_value);
      delete stock;
    }

    // Free storage
    delete warehouse;
  }

  // Finally, all the items are initialized
  srand(1000);
  for (int i = 0; i < NUMBER_OF_ITEMS; i++) {
    // First, we create a key for the item
    char item_key[128];
    Value* item_value = new Value();
    snprintf(item_key, sizeof(item_key), "i%d", i);

    // Next we initialize the object and serialize it
    Item* item = CreateItem(item_key);
    assert(item->SerializeToString(item_value));

    // Finally, we pass it off to the local record of items
    SetItem(string(item_key), item_value);
    delete item;
  }
}

// The following method is a dumb constructor for the warehouse protobuffer
Warehouse* TPCC::CreateWarehouse(Key warehouse_key) const {
  Warehouse* warehouse = new Warehouse();

  // We initialize the id and the name fields
  warehouse->set_id(warehouse_key);
  warehouse->set_name(RandomString(10));

  // Provide some information to make TPC-C happy
  warehouse->set_street_1(RandomString(20));
  warehouse->set_street_2(RandomString(20));
  warehouse->set_city(RandomString(20));
  warehouse->set_state(RandomString(2));
  warehouse->set_zip(RandomString(9));

  // Set default financial information
  warehouse->set_tax(0.05);
  warehouse->set_year_to_date(0.0);

  return warehouse;
}

District* TPCC::CreateDistrict(Key district_key, Key warehouse_key) const {
  District* district = new District();

  // We initialize the id and the name fields
  district->set_id(district_key);
  district->set_warehouse_id(warehouse_key);
  district->set_name(RandomString(10));

  // Provide some information to make TPC-C happy
  district->set_street_1(RandomString(20));
  district->set_street_2(RandomString(20));
  district->set_city(RandomString(20));
  district->set_state(RandomString(2));
  district->set_zip(RandomString(9));

  // Set default financial information
  district->set_tax(0.05);
  district->set_year_to_date(0.0);
  district->set_next_order_id(1);

  return district;
}

Customer* TPCC::CreateCustomer(Key customer_key, Key district_key,
                               Key warehouse_key) const {
  Customer* customer = new Customer();

  // We initialize the various keys
  customer->set_id(customer_key);
  customer->set_district_id(district_key);
  customer->set_warehouse_id(warehouse_key);

  // Next, we create a first and middle name
  customer->set_first(RandomString(20));
  customer->set_middle(RandomString(20));
  customer->set_last(customer_key);

  // Provide some information to make TPC-C happy
  customer->set_street_1(RandomString(20));
  customer->set_street_2(RandomString(20));
  customer->set_city(RandomString(20));
  customer->set_state(RandomString(2));
  customer->set_zip(RandomString(9));

  // Set default financial information
  customer->set_since(0);
  customer->set_credit("GC");
  customer->set_credit_limit(0.01);
  customer->set_discount(0.5);
  customer->set_balance(0);
  customer->set_year_to_date_payment(0);
  customer->set_payment_count(0);
  customer->set_delivery_count(0);

  // Set some miscellaneous data
  customer->set_data(RandomString(50));

  return customer;
}

Stock* TPCC::CreateStock(Key item_key, Key warehouse_key) const {
  Stock* stock = new Stock();

  // We initialize the various keys
  char stock_key[128];
  snprintf(stock_key, sizeof(stock_key), "%ss%s",
           warehouse_key.c_str(), item_key.c_str());
  stock->set_id(stock_key);
  stock->set_warehouse_id(warehouse_key);
  stock->set_item_id(item_key);

  // Next, we create a first and middle name
  stock->set_quantity(rand() % 100 + 100);

  // Set default financial information
  stock->set_year_to_date(0);
  stock->set_order_count(0);
  stock->set_remote_count(0);

  // Set some miscellaneous data
  stock->set_data(RandomString(50));

  return stock;
}

Item* TPCC::CreateItem(Key item_key) const {
  Item* item = new Item();

  // We initialize the item's key
  item->set_id(item_key);

  // Initialize some fake data for the name, price and data
  item->set_name(RandomString(24));
  item->set_price(rand() % 100);
  item->set_data(RandomString(50));

  return item;
}
