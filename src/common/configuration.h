// Author: Shu-chun Weng (scweng@cs.yale.edu)
// Author: Alexander Thomson (thomson@cs.yale.edu)
// Author: Kun Ren (kun@cs.yale.edu)
//
// Each node in the system has a Configuration, which stores the identity of
// that node, the system's current execution mode, and the set of all currently
// active nodes in the system.
//
// Config file format:
//  # (Lines starting with '#' are comments.)
//  # List all nodes in the system.
//  # Node<id>=<replica>:<partition>:<cores>:<host>:<port>
//  node13=1:3:16:4.8.15.16:1001:1002
//  node23=2:3:16:4.8.15.16:1004:1005
//
// Note: Epoch duration, application and other global global options are
//       specified as command line options at invocation time (see
//       deployment/main.cc).

#ifndef _DB_COMMON_CONFIGURATION_H_
#define _DB_COMMON_CONFIGURATION_H_

#include <stdint.h>

#include <map>
#include <string>
#include <vector>
#include <tr1/unordered_map>
#include <pthread.h>


#include "common/types.h"


using std::map;
using std::string;
using std::vector;
using std::tr1::unordered_map;

extern map<Key, Key> latest_order_id_for_customer;
extern map<Key, int> latest_order_id_for_district;
extern map<Key, int> smallest_order_id_for_district;
extern map<Key, Key> customer_for_order;
extern unordered_map<Key, int> next_order_id_for_district;
extern map<Key, int> item_for_order_line;
extern map<Key, int> order_line_number;

extern vector<Key>* involed_customers;

extern pthread_mutex_t mutex_;
extern pthread_mutex_t mutex_for_item;

#define ORDER_LINE_NUMBER 10

struct Node {
  // Globally unique node identifier.
  int node_id;
  int replica_id;
  int partition_id;

  // IP address of this node's machine.
  string host;

  // Port on which to listen for messages from other nodes.
  int port;

  // Total number of cores available for use by this node.
  // Note: Is this needed?
  int cores;
};

class Configuration {
 public:
  Configuration(int node_id, const string& filename);

  // Returns the node_id of the partition at which 'key' is stored.
  int LookupPartition(const Key& key) const;

  // Dump the current config into the file in key=value format.
  // Returns true when success.
  bool WriteToFile(const string& filename) const;

  // This node's node_id.
  int this_node_id;

  // Tracks the set of current active nodes in the system.
  map<int, Node*> all_nodes;

 private:
  // TODO(alex): Comments.
  void ProcessConfigLine(char key[], char value[]);
  int ReadFromFile(const string& filename);
};

#endif  // _DB_COMMON_CONFIGURATION_H_

