// FILE: applications/ycsb.cc

#include "applications/ycsb.h"

#include <atomic>
#include <cmath>
#include <iostream>
#include <set>
#include <string>
#include <vector>

#include "backend/storage.h"
#include "backend/storage_manager.h"
#include "common/configuration.h"
#include "common/definitions.hh"
#include "common/random.hh"
#include "common/utils.h"
#include "proto/txn.pb.h"

enum YCSBTxnType
{
    YCSB_TXN_SP,
    YCSB_TXN_MP,
    YCSB_TXN_INITIALIZE
};

// ---- 補助: Random から [0,1) 一様乱数を作る（Random::Uniform のみで実装）----
static inline double U01(Random &rnd)
{
    // 2^30 で割ることで [0,1) の double を得る
    const uint32_t M = 1u << 30;
    return static_cast<double>(rnd.Uniform(M)) / static_cast<double>(M);
}

// ---- 補助: 簡易 Zipf サンプラー（n が小さいときに使う想定。hot_records_ 用）----
// 返り値は [0, n-1]
static uint64_t ZipfSample(Random &rnd, uint64_t n, double theta)
{
    if (n <= 1 || theta <= 0.0)
        return 0;

    // 定数の前計算（n が小さいので毎回でも軽い）
    double zetan = 0.0;
    for (uint64_t i = 1; i <= n; ++i)
    {
        zetan += 1.0 / std::pow(static_cast<double>(i), theta);
    }
    const double zeta2 = 1.0 + std::pow(2.0, -theta);
    const double alpha = 1.0 / (1.0 - theta);
    const double eta = (1.0 - std::pow(2.0 / static_cast<double>(n), 1.0 - theta)) /
                       (1.0 - zeta2 / zetan);

    const double u = U01(rnd);
    const double uz = u * zetan;

    if (uz < 1.0)
        return 0;
    if (uz < 1.0 + std::pow(0.5, theta))
        return 1;

    // 1-origin を想定した式なので 0-origin に揃える
    const double val = std::floor(
        static_cast<double>(n) * std::pow(eta * u - eta + 1.0, alpha));
    // 範囲ガード
    if (val < 0.0)
        return 0;
    if (val >= static_cast<double>(n))
        return n - 1;
    return static_cast<uint64_t>(val);
}

YCSB::YCSB(double read_ratio, double skew, uint64 db_size, uint64 hot_records)
    : read_ratio_(read_ratio),
      skew_(skew),
      db_size_(db_size),
      hot_records_(hot_records),
      rnd_() {}

void YCSB::InitializeStorage(Storage *storage, Configuration *config) const
{
    for (uint64 i = 0; i < db_size_; i++)
    {
        char key_str[32];
        snprintf(key_str, sizeof(key_str), "k%lu", i);
        if (config->LookupPartition(key_str) == config->this_node_id)
        {
            Value *value = new Value(RandomString(128), 0);
            storage->PutObject(key_str, value);
        }
    }
    // 事前にキー候補を前計算（遅延初期化でも可）
    const_cast<YCSB *>(this)->PrecomputeKeys(config);
}

// 事前計算：各パーティションに属する「非ホット」キーの番号リストを作成
void YCSB::PrecomputeKeys(Configuration *config)
{
    keys_per_partition_.clear();
    keys_per_partition_.resize(config->all_nodes.size());
    for (uint64_t i = hot_records_; i < db_size_; ++i)
    {
        char key_str[32];
        snprintf(key_str, sizeof(key_str), "k%lu", i);
        int partition_id = config->LookupPartition(key_str);
        keys_per_partition_[partition_id].push_back(i);
    }
    keys_ready_.store(true, std::memory_order_release);
    std::cout << "Key precomputation complete." << std::endl;
}

TxnProto *YCSB::NewTxn(int64 txn_id, int /*txn_type*/, std::string /*args*/,
                       Configuration *config) const
{
    // 単一ノードなら常にSP。マルチノード時は 1% を MP に（従来仕様）
    const double mp_ratio = 0.01;
    if (config->all_nodes.size() == 1 || U01(rnd_) >= mp_ratio)
    {
        uint32 part = rnd_.Uniform(static_cast<int>(config->all_nodes.size()));
        return YCSBTxnSP(txn_id, part, config);
    }
    else
    {
        uint32 part1 = rnd_.Uniform(static_cast<int>(config->all_nodes.size()));
        uint32 part2;
        do
        {
            part2 = rnd_.Uniform(static_cast<int>(config->all_nodes.size()));
        } while (part1 == part2);
        return YCSBTxnMP(txn_id, part1, part2, config);
    }
}

int YCSB::Execute(TxnProto * /*txn*/, StorageManager * /*storage*/) const
{
    // Calvin の YCSB は実行ロジックが StorageManager 経由で処理されるためここは空
    return 0;
}

// ランダムキー抽選（前計算リストから重複ナシで取り出し）
void YCSB::GetRandomKeys(std::set<uint64> &keys, int num_keys, uint32 part) const
{
    keys.clear();
    if (!keys_ready_.load(std::memory_order_acquire))
        return;
    const auto &key_candidates = keys_per_partition_[part];
    if (key_candidates.empty())
        return;

    while ((int)keys.size() < num_keys)
    {
        uint64_t index = rnd_.Uniform(static_cast<int>(key_candidates.size()));
        keys.insert(key_candidates[index]);
    }
}

TxnProto *YCSB::YCSBTxnSP(int64 txn_id, uint32 part, Configuration *config) const
{
    if (!keys_ready_.load(std::memory_order_acquire))
        const_cast<YCSB *>(this)->PrecomputeKeys(config);

    TxnProto *txn = new TxnProto();
    txn->set_txn_id(txn_id);
    txn->set_txn_type(YCSB_TXN_SP);

    const bool is_read = (U01(rnd_) < read_ratio_);

    // --- ホットキー 1本（Zipf or 一様）---
    uint64 hot_idx = 0;
    if (skew_ > 0.0)
    {
        hot_idx = ZipfSample(rnd_, static_cast<uint64_t>(hot_records_), skew_);
    }
    else
    {
        hot_idx = rnd_.Uniform(static_cast<int>(hot_records_));
    }
    char hotkey_str[32];
    snprintf(hotkey_str, sizeof(hotkey_str), "k%lu", hot_idx);
    if (is_read)
    {
        txn->add_read_set(hotkey_str);
    }
    else
    {
        txn->add_read_write_set(hotkey_str);
    }

    // --- コールド側（パーティションに属する候補から一様抽選、重複なし）---
    std::set<uint64> keys;
    GetRandomKeys(keys, RW_SET_SIZE - 1, part);
    for (uint64 key : keys)
    {
        char key_str[32];
        snprintf(key_str, sizeof(key_str), "k%lu", key);
        if (is_read)
        {
            txn->add_read_set(key_str);
        }
        else
        {
            txn->add_read_write_set(key_str);
        }
    }
    return txn;
}

TxnProto *YCSB::YCSBTxnMP(int64 txn_id, uint32 part1, uint32 part2,
                          Configuration *config) const
{
    if (!keys_ready_.load(std::memory_order_acquire))
        const_cast<YCSB *>(this)->PrecomputeKeys(config);

    TxnProto *txn = new TxnProto();
    txn->set_txn_id(txn_id);
    txn->set_txn_type(YCSB_TXN_MP);
    txn->set_multipartition(true);

    const bool is_read = (U01(rnd_) < read_ratio_);

    // --- 各パーティションのホットキー（Zipf or 一様）---
    uint64 hot1 = (skew_ > 0.0)
                      ? ZipfSample(rnd_, static_cast<uint64_t>(hot_records_), skew_)
                      : rnd_.Uniform(static_cast<int>(hot_records_));
    uint64 hot2 = (skew_ > 0.0)
                      ? ZipfSample(rnd_, static_cast<uint64_t>(hot_records_), skew_)
                      : rnd_.Uniform(static_cast<int>(hot_records_));

    char hotkey1_str[32], hotkey2_str[32];
    snprintf(hotkey1_str, sizeof(hotkey1_str), "k%lu", hot1);
    snprintf(hotkey2_str, sizeof(hotkey2_str), "k%lu", hot2);
    if (is_read)
    {
        txn->add_read_set(hotkey1_str);
        txn->add_read_set(hotkey2_str);
    }
    else
    {
        txn->add_read_write_set(hotkey1_str);
        txn->add_read_write_set(hotkey2_str);
    }

    // --- コールド側：各パーティションから一様抽選 ---
    std::set<uint64> keys;
    GetRandomKeys(keys, RW_SET_SIZE / 2 - 1, part1);
    for (uint64 key : keys)
    {
        char key_str[32];
        snprintf(key_str, sizeof(key_str), "k%lu", key);
        if (is_read)
        {
            txn->add_read_set(key_str);
        }
        else
        {
            txn->add_read_write_set(key_str);
        }
    }

    keys.clear();
    GetRandomKeys(keys, RW_SET_SIZE / 2 - 1, part2);
    for (uint64 key : keys)
    {
        char key_str[32];
        snprintf(key_str, sizeof(key_str), "k%lu", key);
        if (is_read)
        {
            txn->add_read_set(key_str);
        }
        else
        {
            txn->add_read_write_set(key_str);
        }
    }

    return txn;
}

TxnProto *YCSB::InitializeTxn() const
{
    TxnProto *txn = new TxnProto();
    txn->set_txn_type(YCSB_TXN_INITIALIZE);
    return txn;
}
