#pragma once

// ============== please don't change ==============
#define COLD_CUTOFF 1000000
#define MAX_LOCK_BATCH_SIZE 2000
#define EPOCH_DURATION 0.01 // 0.01 is 10ms
// =================================================

// ============== 元のCalvin用設定 (NUMA完全分離版) ==============
// ★ バックグラウンドをNode 1 (奇数)、ワーカーをNode 0 (偶数) に分離 ★
// =============================================================

// ============== server setting ==============
#define NUM_CORE 0 // スクリプトで上書き

// --- バックグラウンドスレッドの定義 (4つ) ---
#define NUM_BACKGROUND_THREADS 4
// バックグラウンドをNode 1 (奇数コア) にハードコードで割り当て
#define MULTIPLEXER_CORE 1
#define SEQUENCER_WRITER_CORE 3
#define SEQUENCER_READER_CORE 5
#define LOCK_MANAGER_CORE 7

// --- ワーカーの定義 ---
#define NUM_WORKERS_CORE (NUM_CORE - NUM_BACKGROUND_THREADS)
#define NUM_WORKERS (NUM_WORKERS_CORE)

// -------- [ここから変更] --------

/**
 * @brief ワーカーコアを割り当てる関数
 * 1. まず、偶数コアを昇順 (0, 2, 4, ...) に割り当てる。
 * 2. 偶数コアを使い切ったら (NUM_COREを超えたら)、
 * 利用可能な奇数コアを降順 (NUM_CORE-1, NUM_CORE-3, ...) に割り当てる。
 * 3. その際、バックグラウンドスレッドが使用するコア (1, 3, 5, 7) は除外する。
 */
inline int GetWorkerCore(int thread_id) {
  // 1. 偶数コアを昇順に割り当て
  int even_core = thread_id * 2;
  if (even_core < NUM_CORE) {
    return even_core;
  }

  // 2. 偶数コアを使い切った場合 (スピルオーバー)
  // 利用可能な偶数コアの数を計算
  int num_even_cores = (NUM_CORE + 1) / 2;
  // 自分は何番目のスピルオーバースレッドか (0-indexed)
  int spill_thread_index = thread_id - num_even_cores;

  // 3. 利用可能な奇数コアを降順に探す
  // NUM_CORE-1 が偶数なら、(NUM_CORE-1)-1 = NUM_CORE-2 から開始
  int current_odd_core = (NUM_CORE - 1);
  if (current_odd_core % 2 == 0) {
    current_odd_core--;
  }

  int assigned_spill_count = 0;
  while (current_odd_core >= 0) {
    // バックグラウンドスレッドのコアかチェック
    bool is_background_core =
        (current_odd_core == MULTIPLEXER_CORE) ||
        (current_odd_core == SEQUENCER_WRITER_CORE) ||
        (current_odd_core == SEQUENCER_READER_CORE) ||
        (current_odd_core == LOCK_MANAGER_CORE);

    if (!is_background_core) {
      // 利用可能な奇数コアだった
      if (assigned_spill_count == spill_thread_index) {
        return current_odd_core; // 該当するコアを返す
      }
      assigned_spill_count++;
    }
    current_odd_core -= 2; // 次の奇数コアへ (降順)
  }

  // 割り当て失敗 (NUM_WORKERS の定義が正しければ、ここには来ない)
  return -1; 
}

// ワーカーをNode 0 (偶数コア) に割り当て
// (ただし、使い切ったら降順の奇数コアにスピルオーバー)
#define GET_WORKER_CORE(thread_id) (GetWorkerCore(thread_id))
// -------- [ここまで変更] --------
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