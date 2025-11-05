#ifndef _DB_APPLICATIONS_YCSB_H_
#define _DB_APPLICATIONS_YCSB_H_

#include "applications/application.h"
#include "common/random.hh" // ← Random の定義
#include <vector>
#include <set>
#include <atomic>
#include <cstdint>

class YCSB : public Application
{
public:
    YCSB(double read_ratio, double skew, uint64 db_size, uint64 hot_records);

    void InitializeStorage(Storage *storage, Configuration *config) const override;
    TxnProto *NewTxn(int64 txn_id, int txn_type, std::string args, Configuration *config) const override;
    int Execute(TxnProto *txn, StorageManager *storage) const override;

    // Application の宣言と一致しない環境があるため override は付けない
    TxnProto *InitializeTxn() const;

    // 初期化時または遅延でキー候補を前計算
    void PrecomputeKeys(Configuration *config);

private:
    TxnProto *YCSBTxnSP(int64 txn_id, uint32 part, Configuration *config) const;
    TxnProto *YCSBTxnMP(int64 txn_id, uint32 part1, uint32 part2, Configuration *config) const;

    // part に属するランダムキーを重複なしで num_keys 個選ぶ
    void GetRandomKeys(std::set<uint64> &keys, int num_keys, uint32 part) const;

    // パラメータ
    double read_ratio_;
    double skew_; // ← 追加：Zipf 用スキュー (0 なら一様)
    uint64 db_size_;
    uint64 hot_records_;

    // 乱数生成器（common/random.hh）
    mutable Random rnd_;

    // 前計算キャッシュ
    mutable std::atomic<bool> keys_ready_{false};
    std::vector<std::vector<uint64>> keys_per_partition_;
};

#endif // _DB_APPLICATIONS_YCSB_H_
