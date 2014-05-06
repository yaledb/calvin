// Author: Alexander Thomson (thomson@cs.yale.edu)

#include "backend/simple_storage.h"

#include "common/testing.h"

TEST(SimpleStorageTest) {
  SimpleStorage storage;
  Key key = bytes("key");
  Value value = bytes("value");
  Value* result;
  EXPECT_EQ(0, storage.ReadObject(key));
  EXPECT_TRUE(storage.PutObject(key, &value));
  result = storage.ReadObject(key);
  EXPECT_EQ(value, *result);

  EXPECT_TRUE(storage.DeleteObject(key));
  EXPECT_EQ(0, storage.ReadObject(key));

  END;
}

int main(int argc, char** argv) {
  SimpleStorageTest();
}


