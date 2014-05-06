// Author: Alexander Thomson (thomson@cs.yale.edu)
//
// TODO(alex): Write some tests spanning multiple physical machines.

#include "common/connection.h"

#include <iostream>

#include "common/testing.h"
/*
TEST(InprocTest) {
  Configuration config(0, "common/configuration_test_one_node.conf");
  ConnectionMultiplexer* multiplexer = new ConnectionMultiplexer(&config);

  Spin(0.1);

  Connection* c1 = multiplexer->NewConnection("c1");
  Connection* c2 = multiplexer->NewConnection("c2");

  MessageProto message;
  message.set_destination_node(0);
  message.set_destination_channel("c2");
  message.set_type(MessageProto::EMPTY);
  message.add_data("foo bar baz");

  c1->Send(message);
  message.Clear();
  c2->GetMessageBlocking(&message, 60);

  EXPECT_EQ("foo bar baz", message.data(0));

  delete c1;
  delete c2;
  delete multiplexer;

  END;
}

TEST(RemoteTest) {
  Configuration config1(1, "common/configuration_test.conf");
  Configuration config2(2, "common/configuration_test.conf");

  ConnectionMultiplexer* multiplexer1 = new ConnectionMultiplexer(&config1);
  ConnectionMultiplexer* multiplexer2 = new ConnectionMultiplexer(&config2);

  Spin(0.1);

  Connection* c1 = multiplexer1->NewConnection("c1");
  Connection* c2 = multiplexer2->NewConnection("c2");

  MessageProto message;
  message.set_destination_node(2);
  message.set_destination_channel("c2");
  message.set_type(MessageProto::EMPTY);
  message.add_data("foo bar baz");

  c1->Send(message);
  message.Clear();
  c2->GetMessageBlocking(&message, 1);

  EXPECT_EQ("foo bar baz", message.data(0));

  delete c1;
  delete c2;
  delete multiplexer1;
  delete multiplexer2;

  END;
}

TEST(ChannelNotCreatedYetTest) {
  Configuration config(0, "common/configuration_test_one_node.conf");
  ConnectionMultiplexer* multiplexer = new ConnectionMultiplexer(&config);

  Spin(0.1);

  Connection* c1 = multiplexer->NewConnection("c1");

  MessageProto message;
  message.set_destination_node(0);
  message.set_destination_channel("c2");
  message.set_type(MessageProto::EMPTY);
  message.add_data("foo bar baz");

  c1->Send(message);
  message.Clear();
  Spin(0.1);

  // Create channel after message is sent.
  Connection* c2 = multiplexer->NewConnection("c2");
  c2->GetMessageBlocking(&message, 60);

  EXPECT_EQ("foo bar baz", message.data(0));

  delete c1;
  delete c2;
  delete multiplexer;

  END;
}

TEST(LinkUnlinkChannelTest) {
  Configuration config(0, "common/configuration_test_one_node.conf");
  ConnectionMultiplexer* multiplexer = new ConnectionMultiplexer(&config);
  Connection* c1 = multiplexer->NewConnection("c1");
  Connection* c2 = multiplexer->NewConnection("c2");
  c2->LinkChannel("c3");

  Spin(0.1);

  // Send message to newly linked channel.
  MessageProto message;
  message.set_destination_node(0);
  message.set_destination_channel("c3");
  message.set_type(MessageProto::EMPTY);
  message.add_data("foo bar baz");
  c1->Send(message);

  // Receive message.
  MessageProto m;
  c2->GetMessageBlocking(&m, 60);
  EXPECT_EQ("foo bar baz", m.data(0));

  // Send same message to channel before it is linked.
  message.set_destination_channel("c4");
  c1->Send(message);

  Spin(0.1);

  // Recipient should not receive the message until linking the channel "c4".
  EXPECT_FALSE(c2->GetMessage(&m));
  c2->LinkChannel("c4");
  Spin(0.1);  // Give multiplexer time to link.
  EXPECT_TRUE(c2->GetMessage(&m));
  EXPECT_EQ("foo bar baz", m.data(0));

  // Unlink a channel and check that it no longer works.
  c2->UnlinkChannel("c4");
  Spin(0.1);  // Give multiplexer time to unlink.
  c1->Send(message);
  Spin(0.1);  // Give multiplexer time to deliver the message.
  EXPECT_FALSE(c2->GetMessage(&m));

  // Deleting a connection should first free all its remaining links
  // (i.e. "c3").
  delete c2;
  Spin(0.1);  // Give multiplexer time to unlink.
  c2 = multiplexer->NewConnection("c3");
  message.set_destination_channel("c3");
  c1->Send(message);
  c2->GetMessageBlocking(&m, 60);

  delete c1;
  delete c2;
  delete multiplexer;

  END;
}
*/
int main(int argc, char** argv) {
//  InprocTest();
//  RemoteTest();
//  ChannelNotCreatedYetTest();
//  LinkUnlinkChannelTest();
}

