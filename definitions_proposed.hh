#pragma once

// ============== please don't change ==============
#define COLD_CUTOFF 1000000
#define MAX_LOCK_BATCH_SIZE 2000
#define EPOCH_DURATION 0.01 // 0.01 is 10ms
// =================================================

// ============== 提案手法Calvin用設定 (Sequencer複数化・NUMA最適化版) ==============
// ★ バックグラウンドをNode 1 (奇数)、ワーカーをNode 0 (偶数) 優先に分離 ★
// ==============================================================================

// ============== server setting ==============
#define NUM_CORE 0 // スクリプトで上書き

// --- バックグラウンドスレッドの定義 ---
#define NUM_SEQUENCERS 2           // スクリプトで上書きされる
#define NUM_RO_DISPATCHERS 1       // スクリプトで上書きされる
#define OTHER_BACKGROUND_THREADS 2 // Multiplexer, LockManager
#define NUM_BACKGROUND_THREADS (OTHER_BACKGROUND_THREADS + (NUM_SEQUENCERS * 2) + NUM_RO_DISPATCHERS)

// バックグラウンドスレッドをNode 1 (奇数コア) に順に割り当て
#define MULTIPLEXER_CORE 1
#define LOCK_MANAGER_CORE 3
#define GET_SEQUENCER_WRITER_CORE(i) (5 + ((i) * 4))
#define GET_SEQUENCER_READER_CORE(i) (7 + ((i) * 4))
#define GET_RO_DISPATCHER_CORE(i) (5 + (NUM_SEQUENCERS * 4) + ((i) * 2))

// --- ワーカーの定義 ---
#define NUM_WORKERS_CORE (NUM_CORE - NUM_BACKGROUND_THREADS)
#define NUM_WORKERS (NUM_WORKERS_CORE)

// ヘルパーマクロ
#define NUM_EVEN_CORES ((NUM_CORE + 1) / 2)
#define NUM_ODD_CORES (NUM_CORE / 2)

// ワーカーを「低位偶数優先、その後、高位奇数を逆順」で割り当て
#define GET_WORKER_CORE(thread_id)  \
    (((thread_id) < NUM_EVEN_CORES) \
         ? ((thread_id) * 2)        \
         : ((NUM_ODD_CORES - 1 - ((thread_id) - NUM_EVEN_CORES)) * 2 + 1))
// ==============================================

// ============== database setting ==============
#define DB_SIZE 1000000
#define LOCK_TABLE_SIZE 1000000
// ==============================================

// ============== workload setting ==============
#define RW_SET_SIZE 100
#define SKEW 0.99
#define UNIFORM_KEY_SELECTION_RATIO 0
#define HOT 10
// ==============================================

// ============== used for only calvin ==============
#define MAX_ACTIVE_TXNS 2000
#define LOCK_BATCH_SIZE 100
// ==============================================
