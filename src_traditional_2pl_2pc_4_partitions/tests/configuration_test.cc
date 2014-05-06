// Author: Alexander Thomson (thomson@cs.yale.edu)

#include "common/configuration.h"

#include "common/testing.h"

// common/configuration_test.conf:
//  # Node<id>=<replica>:<partition>:<cores>:<host>:<port>
//  node1=0:1:16:128.36.232.50:50001
//  node2=0:2:16:128.36.232.50:50002
TEST(ConfigurationTest_ReadFromFile) {
  Configuration config(1, "common/configuration_test.conf");
  EXPECT_EQ(1, config.this_node_id);
  EXPECT_EQ(2, config.all_nodes.size());  // 2 Nodes, node13 and node23.
  EXPECT_EQ(1, config.all_nodes[1]->node_id);
  EXPECT_EQ(50001, config.all_nodes[1]->port);
  EXPECT_EQ(2, config.all_nodes[2]->node_id);
  EXPECT_EQ(string("128.36.232.50"), config.all_nodes[2]->host);
  END;
}

// TODO(alex): Write proper test once partitioning is implemented.
TEST(ConfigurationTest_LookupPartition) {
  Configuration config(1, "common/configuration_test.conf");
  EXPECT_EQ(0, config.LookupPartition(Key("0")));
  END;
}

int main(int argc, char** argv) {
  ConfigurationTest_ReadFromFile();
  ConfigurationTest_LookupPartition();
}

