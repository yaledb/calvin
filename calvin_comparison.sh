#!/bin/bash

# ==============================================================================
# Calvin 比較実験用スクリプト (Sequencer/Dispatcher複数化対応・最終版)
# ==============================================================================

# --- 比較対象を設定 ('original' または 'proposed') ---
TARGET="proposed" # ここを 'original' に変えて元カルバンを測定

# --- 引数処理 ---
if [ "$1" == "m" ]; then
    ARGUMENT="m"
elif [ "$1" == "t" ]; then
    ARGUMENT="t"
elif [ "$1" == "y" ]; then
    ARGUMENT="y"
else
    echo "エラー: 引数が無効です。'm', 't', 'y' のいずれかを使用してください。"
    exit 1
fi
echo "🧪 使用するベンチマーク: $ARGUMENT"
echo "🎯 測定対象: $TARGET"

# ========================== 実験パラメータ ==========================
if [ "$TARGET" == "original" ]; then
    # --- 元カルバンの設定 ---
    NUM_SEQUENCERS=5 # ★ ここで元カルバンのSequencer数を設定
    NUM_BACKGROUND=$((2 + NUM_SEQUENCERS * 2)) # (Multiplexer, LockManager) + Sequencers
    DEFINITIONS_FILE="definitions_original.hh"
    SOURCE_DIR="src_calvin"
    OUTPUT_CSV="throughput_summary_${ARGUMENT}_original_s${NUM_SEQUENCERS}.csv"
else
    # --- 提案手法の設定 ---
    NUM_SEQUENCERS=3 # ★ ここで提案手法のSequencer数を設定
    NUM_DISPATCHERS=2 # ★ ここで提案手法のDispatcher数を設定
    NUM_BACKGROUND=$((2 + NUM_SEQUENCERS * 2 + NUM_DISPATCHERS)) # (Multiplexer, LockManager) + Sequencers + Dispatchers
    DEFINITIONS_FILE="definitions_proposed.hh"
    SOURCE_DIR="src_calvin_ext"
    OUTPUT_CSV="throughput_summary_${ARGUMENT}_proposed_s${NUM_SEQUENCERS}_d${NUM_DISPATCHERS}.csv"
fi

THREAD_COUNTS=(1 2 4 8 16 24 32 48 64 72 80)
RUN_DURATION=10
NUM_RUNS=5

# --- 基本設定 ---
SCRIPT_DIR=$(dirname "$0")
cd "$SCRIPT_DIR" || exit 1
CONFIG_FILE="cygnus-run.conf"
BACKUP_CONFIG_FILE="${CONFIG_FILE}.bak"
LOG_FILE="/tmp/calvin_output.log"

# --- 事前準備 ---
rm -f /tmp/failed_log_*.log
cp "$CONFIG_FILE" "$BACKUP_CONFIG_FILE"
echo "✅ 設定ファイルをバックアップ: $BACKUP_CONFIG_FILE"
echo "Threads,Average_Throughput(ops/sec)" > "$OUTPUT_CSV"

# ========================== メインループ ==========================
for THREADS in "${THREAD_COUNTS[@]}"; do
    echo -e "\n======== Computation Cores: $THREADS ========"

    # --- 公平な比較のためのワーカー数調整 ---
    if [ "$TARGET" == "original" ]; then
        NUM_WORKERS=$THREADS
    else
        NUM_WORKERS=$((THREADS - NUM_DISPATCHERS))
        if [ "$NUM_WORKERS" -lt 1 ]; then
            echo "ワーカー数が1未満になるためスキップします。"
            continue
        fi
    fi
    
    NUM_CORE=$((NUM_WORKERS + NUM_BACKGROUND))
    if [ "$NUM_CORE" -gt 96 ]; then
        echo "合計コア数($NUM_CORE)がサーバーの上限(96)を超えるためスキップします。"
        continue
    fi
    
    echo "  - Configuration: Workers=$NUM_WORKERS, Sequencers=$NUM_SEQUENCERS, Dispatchers=$NUM_DISPATCHERS, TotalCores=$NUM_CORE"

    # --- ソースコードと定義ファイルの更新 ---
    echo "  [1/4] ソースコードと定義ファイルを更新中..."
    rm -rf src obj
    cp -r "$SOURCE_DIR"/ src
    cp "$DEFINITIONS_FILE" src/common/definitions.hh

    # sedで各種マクロを置換
    sed -i -E "s/^#define[[:space:]]+NUM_CORE[[:space:]]+.*$/#define NUM_CORE $NUM_CORE/" src/common/definitions.hh
    sed -i -E "s/^#define[[:space:]]+NUM_BACKGROUND_THREADS[[:space:]]+.*$/#define NUM_BACKGROUND_THREADS $NUM_BACKGROUND/" src/common/definitions.hh
    sed -i -E "s/^#define[[:space:]]+NUM_WORKERS[[:space:]]+.*$/#define NUM_WORKERS $NUM_WORKERS/" src/common/definitions.hh
    sed -i -E "s/^#define[[:space:]]+NUM_SEQUENCERS[[:space:]]+.*$/#define NUM_SEQUENCERS $NUM_SEQUENCERS/" src/common/definitions.hh
    if [ "$TARGET" != "original" ]; then
        sed -i -E "s/^#define[[:space:]]+NUM_RO_DISPATCHERS[[:space:]]+.*$/#define NUM_RO_DISPATCHERS $NUM_DISPATCHERS/" src/common/definitions.hh
    fi

    # --- ビルド ---
    echo "  [2/4] ビルドを実行中..."
    cd src
    make clean > /dev/null 2>&1
    if make -j$(nproc); then
        echo "      - ビルド成功。"
    else
        echo "      - ❌ ビルド失敗 (Workers=$NUM_WORKERS)。スキップします。"
        cd ..
        continue
    fi
    cd ..

    # ========================== 測定実行 ==========================
    echo "  [3/4] 測定を実行中 ($NUM_RUNS 回)..."
    THROUGHPUT_ARRAY=()

    for i in $(seq 1 $NUM_RUNS); do
        ./bin/deployment/db 0 "$ARGUMENT" 0 > "$LOG_FILE" 2>&1 &
        APP_PID=$!
        sleep "$RUN_DURATION"
        kill -SIGINT "$APP_PID" 2>/dev/null
        wait "$APP_PID" 2>/dev/null
        sleep 1
        if ps -p $APP_PID > /dev/null; then
           kill -SIGKILL "$APP_PID" 2>/dev/null
        fi

        THROUGHPUT=$(grep -oP 'Completed\s*\K[0-9.]+' "$LOG_FILE" | tail -n 5 | head -n 1 | awk '{print $1}')

        if [ -z "$THROUGHPUT" ]; then
            echo "    - 実行 $i/$NUM_RUNS: ⚠️ スループット抽出失敗"
            FAILED_LOG_NAME="/tmp/failed_log_threads_${THREADS}_run_${i}.log"
            mv "$LOG_FILE" "$FAILED_LOG_NAME"
            echo "      (ログを ${FAILED_LOG_NAME} に保存しました)"
        else
            echo "    - 実行 $i/$NUM_RUNS: ✅ スループット: $THROUGHPUT ops/sec"
            THROUGHPUT_ARRAY+=($THROUGHPUT)
        fi
        rm -f "$LOG_FILE"
    done

    # ========================== 結果集計 ==========================
    echo "  [4/4] 結果を集計中..."
    SUCCESSFUL_RUNS=${#THROUGHPUT_ARRAY[@]}
    if [ "$SUCCESSFUL_RUNS" -gt 1 ]; then
        # 最初の1回をスキップ
        TOTAL_THROUGHPUT=0
        for ((j=1; j<SUCCESSFUL_RUNS; j++)); do
            TOTAL_THROUGHPUT=$(awk "BEGIN {print $TOTAL_THROUGHPUT + ${THROUGHPUT_ARRAY[$j]}}")
        done
        NUM_VALID_RUNS=$((SUCCESSFUL_RUNS - 1))
        AVERAGE_THROUGHPUT=$(awk "BEGIN {printf \"%.2f\", $TOTAL_THROUGHPUT / $NUM_VALID_RUNS}")
        echo "✅ 平均スループット ($THREADS threads): $AVERAGE_THROUGHPUT ops/sec ($NUM_VALID_RUNS 回の有効実行)"
    else
        AVERAGE_THROUGHPUT="N/A"
        echo "⚠️  $THREADS threads の有効な実行が1回以下だったため、平均を計算できませんでした。"
    fi
    echo "$THREADS,$AVERAGE_THROUGHPUT" >> "$OUTPUT_CSV"
done

# ========================== 後始末 ==========================
mv "$BACKUP_CONFIG_FILE" "$CONFIG_FILE"
echo -e "\n🛠️  設定ファイルを元に戻しました。"
echo "🎉 全実験完了。結果は $OUTPUT_CSV に保存されました。"
