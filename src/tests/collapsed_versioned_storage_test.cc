// Author: Thaddeus Diamond (diamond@cs.yale.edu)

#include "backend/collapsed_versioned_storage.h"

#include "common/testing.h"

TEST(CollapsedVersionedStorageTest) {
  CollapsedVersionedStorage* storage = new CollapsedVersionedStorage();

  Key key = bytes("key");
  Value value_one = bytes("value_one");
  Value value_two = bytes("value_two");
  Value* result = storage->ReadObject(key);

  EXPECT_TRUE(storage->PutObject(key, &value_one, 10));
  storage->PrepareForCheckpoint(15);
  EXPECT_TRUE(storage->PutObject(key, &value_two, 12));
  EXPECT_TRUE(storage->PutObject(key, &value_two, 20));
  EXPECT_TRUE(storage->PutObject(key, &value_one, 30));

  EXPECT_EQ(0, storage->ReadObject(key, 10));
  result = storage->ReadObject(key, 12);
  EXPECT_EQ(value_two, *result);
  result = storage->ReadObject(key, 20);
  EXPECT_EQ(value_two, *result);
  result = storage->ReadObject(key, 30);
  EXPECT_EQ(value_one, *result);
  result = storage->ReadObject(key);
  EXPECT_EQ(value_one, *result);

  EXPECT_TRUE(storage->DeleteObject(key, 14));

  EXPECT_EQ(0, storage->ReadObject(key, 12));
  result = storage->ReadObject(key);
  EXPECT_EQ(value_one, *result);

  EXPECT_TRUE(storage->DeleteObject(key, 35));

  delete storage;

  END;
}

TEST(CheckpointingTest) {
  CollapsedVersionedStorage* storage = new CollapsedVersionedStorage();

  Key key = bytes("key");
  Value value_one = bytes("value_one");
  Value value_two = bytes("value_two");
  Value* result;

  EXPECT_TRUE(storage->PutObject(key, &value_one, 10));
  storage->PrepareForCheckpoint(15);
  EXPECT_TRUE(storage->PutObject(key, &value_two, 20));
  storage->Checkpoint();

  sleep(5);

  char checkpoint_path[100];
  snprintf(checkpoint_path, sizeof(checkpoint_path), "%s/15.checkpoint",
           CHKPNTDIR);
  FILE* checkpoint = fopen(checkpoint_path, "r");
  EXPECT_TRUE(checkpoint != NULL);
  fclose(checkpoint);

  EXPECT_EQ(0, storage->ReadObject(key, 10));
  result = storage->ReadObject(key);
  EXPECT_EQ(value_two, *result);

  delete storage;

  END;
}

int main(int argc, char** argv) {
  CollapsedVersionedStorageTest();
  CheckpointingTest();
}


