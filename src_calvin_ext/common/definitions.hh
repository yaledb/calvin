#pragma once

#define MAX_ACTIVE_TXNS 2000  // default 2000
#define LOCK_BATCH_SIZE 100   // default 100

#define HOT 10

#define COLD_CUTOFF 990000  // default 990000

#define RW_SET_SIZE 1    // MUST BE EVEN, default 10
#define DB_SIZE 1000000  // default 1000000