// Author: Kun Ren (kun.ren@yale.edu)

#include "paxos/paxos.h"

#include <vector>

#include "common/testing.h"
#include "sequencer/sequencer.h"

using std::vector;

// Record the time of the batches submitted.
vector<double> submit_time;
// Record the time of the batches received.
vector<double> receive_time;
// The throuput of get batches from zookeeper.
int throughput;

// Create Paxos object to submit some batches, inserting them into
// a globally consistent batch order.
void* Writer(void *arg) {
  // Create paxos object.
  Paxos writer(ZOOKEEPER_CONF, false);

  string test("test");
  double start, end;
  int write_number = 0;
  // Firstly, submit one batch, sleep for a while (random from 0 to 1 msec),
  // submit another batch, for 70 seconds in all.
  start = GetTime();
  while (1) {
    // Submit the bach.
    writer.SubmitBatch(test);
    end = GetTime();
    submit_time.push_back(end);
    write_number++;
    // The interal is 0 to 1 msec.
    srand(50);
    usleep((rand()%10)*100);
    // Test for 70 seconds.
    if (end - start > 70)
      break;
  }
  sleep(30);
  return NULL;
}

// Create Paxos object to read batches from zookeeper.
void* Reader(void *arg) {
  // Create Paxos object.
  Paxos reader(ZOOKEEPER_CONF, true);

  string batch_data;
  double start, end;
  int read_number = 0;
  // Continued get batches from zookeeper server.
  start = GetTime();
  while (1) {
    bool rc = reader.GetNextBatch(&batch_data);
    if (rc == true) {
      read_number++;
      end = GetTime();
      receive_time.push_back(end);
      // If have received around 200k batches, break the loop,
      // record the running time.
      if (read_number >= 200000) {
        end = GetTime();
        throughput = 200000 / static_cast<int>(end - start);
        break;
      }
    } else {
      end = GetTime();
      // Timeout is 150 sec.
      if (end - start > 150) {
        printf("Writer writes no more than 20k batches.\n");
        break;
      }
    }
  }
  sleep(20);
  return NULL;
}

TEST(PaxosTest) {
  printf("Running zookeeper test.\n");

  pthread_t thread_1;
  pthread_t thread_2;
  pthread_create(&thread_1, NULL, Writer, NULL);
  pthread_create(&thread_2, NULL, Reader, NULL);
  pthread_join(thread_1, NULL);
  pthread_join(thread_2, NULL);

  // Compute the average latency.
  double sum_latency = 0;
  for (unsigned int i = 0; i < receive_time.size(); i++) {
    sum_latency += (receive_time[i] - submit_time[i]);
  }
  double average_latency = sum_latency * 1000 / receive_time.size();

  printf("Throughput: %d txns/sec.\n", throughput);
  printf("Average latency: %lf ms.\n", average_latency);

  printf("Running zookeeper test.......done.\n");
  END;
}

int main(int argc, char** argv) {
#ifdef PAXOS
  // Start zookeeper
  // StartZookeeper(ZOOKEEPER_CONF);
  // printf("Starting zookeeper servers.......done.\n");
  // Run zookeeper test
  PaxosTest();
  // Stop zookeeper
//  printf("Stopping zookeeper servers.\n");
//  StopZookeeper(ZOOKEEPER_CONF);
//  printf("Stopping zookeeper servers.......done.\n");
#endif
}
