// Author: Philip Shao (philip.shao@yale.edu)

#include "backend/fetching_storage.h"

#include <iostream>
#include <sstream>

#include "common/testing.h"


TEST(FetchingStorageTest) {
  system("rm ../db/storage/*");
  FetchingStorage* storage = FetchingStorage::BuildStorage();
  Key key = bytes("1");
  Value value = bytes("value");
  Value* result;
  double wait_time;
  EXPECT_FALSE(storage->Prefetch(key, &wait_time));
  EXPECT_TRUE(storage->PutObject(key, &value));
  result = storage->ReadObject(key);
  EXPECT_EQ(value, *result);
  EXPECT_TRUE(storage->Unfetch(key));
  sleep(1);
  EXPECT_TRUE(storage->Prefetch(key, &wait_time));
  sleep(1);
  result = storage->ReadObject(key);
  EXPECT_EQ(value, *result);
  EXPECT_TRUE(storage->Unfetch(key));
  END;
}

int main(int argc, char** argv) {
  FetchingStorageTest();
}


