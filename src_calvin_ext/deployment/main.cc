// Author: Kun Ren (kun.ren@yale.edu)
// Author: Alexander Thomson (thomson@cs.yale.edu)
//
// Main invokation of a single node in the system.

#include <csignal>
#include <cstdio>
#include <cstdlib>
#include <vector>
#include <string>
#include <iostream>

#include "applications/application.h"
#include "applications/microbenchmark.h"
#include "applications/tpcc.h"
#include "applications/ycsb.h"
#include "common/configuration.h"
#include "common/connection.h"
#include "common/definitions.hh"
#include "backend/simple_storage.h"
#include "backend/fetching_storage.h"
#include "backend/collapsed_versioned_storage.h"
#include "scheduler/serial_scheduler.h"
#include "scheduler/deterministic_scheduler.h"
#include "sequencer/sequencer.h"
#include "proto/tpcc_args.pb.h"
#include "proto/txn.pb.h"

// グローバル変数
map<Key, Key> latest_order_id_for_customer;
map<Key, int> latest_order_id_for_district;
map<Key, int> smallest_order_id_for_district;
map<Key, Key> customer_for_order;
unordered_map<Key, int> next_order_id_for_district;
map<Key, int> item_for_order_line;
map<Key, int> order_line_number;
vector<Key> *involed_customers;

pthread_mutex_t mutex_;
pthread_mutex_t mutex_for_item;

// Clientクラス定義 (変更なし)
class MClient : public Client
{
public:
    MClient(Configuration *config, int mp, Application *app)
        : microbenchmark_(static_cast<Microbenchmark *>(app)), config_(config),
          percent_mp_(mp)
    {
    }

    virtual ~MClient() {}
    virtual void GetTxn(TxnProto **txn, int txn_id)
    {
        if (config_->all_nodes.size() > 1 && (rand() % 100) < percent_mp_)
        {
            int other;
            do
            {
                other = rand() % config_->all_nodes.size();
            } while (other == config_->this_node_id);
            *txn = microbenchmark_->MicroTxnMP(txn_id, config_->this_node_id, other);
        }
        else
        {
            *txn = microbenchmark_->MicroTxnSP(txn_id, config_->this_node_id);
        }
    }

private:
    Microbenchmark *microbenchmark_;
    Configuration *config_;
    int percent_mp_;
};

class TClient : public Client
{
public:
    TClient(Configuration *config, int mp, Application *app)
        : config_(config), percent_mp_(mp), tpcc_(static_cast<TPCC *>(app)) {}
    virtual ~TClient() {}
    virtual void GetTxn(TxnProto **txn, int txn_id)
    {
        TPCCArgs args;
        args.set_system_time(GetTime());
        args.set_multipartition((rand() % 100) < percent_mp_);

        string args_string;
        args.SerializeToString(&args_string);

        int random_txn_type = rand() % 100;
        if (random_txn_type < 45)
        {
            *txn = tpcc_->NewTxn(txn_id, TPCC::NEW_ORDER, args_string, config_);
        }
        else if (random_txn_type < 88)
        {
            *txn = tpcc_->NewTxn(txn_id, TPCC::PAYMENT, args_string, config_);
        }
        else if (random_txn_type < 92)
        {
            *txn = tpcc_->NewTxn(txn_id, TPCC::ORDER_STATUS, args_string, config_);
        }
        else if (random_txn_type < 96)
        {
            *txn = tpcc_->NewTxn(txn_id, TPCC::DELIVERY, args_string, config_);
        }
        else
        {
            *txn = tpcc_->NewTxn(txn_id, TPCC::STOCK_LEVEL, args_string, config_);
        }
    }

private:
    Configuration *config_;
    int percent_mp_;
    TPCC *tpcc_;
};

class YClient : public Client
{
public:
    YClient(Configuration *config, int mp, Application *ycsb_app)
        : config_(config), percent_mp_(mp), ycsb_app_(ycsb_app) {}
    virtual ~YClient() {}
    virtual void GetTxn(TxnProto **txn, int txn_id)
    {
        string args_string = "";
        *txn = ycsb_app_->NewTxn(txn_id, 0, args_string, config_);
    }

private:
    Configuration *config_;
    int percent_mp_;
    Application *ycsb_app_;
};

// シグナルハンドラ
void stop(int sig)
{
    exit(sig);
}

int main(int argc, char **argv)
{
    // 引数チェックとシグナルハンドラ設定
    if (argc < 4)
    {
        fprintf(stderr, "Usage: %s <node-id> <m[icro]|t[pcc]|y[csb]> <percent_mp> [f for fetching]\n",
                argv[0]);
        exit(1);
    }
    bool useFetching = (argc > 4 && argv[4][0] == 'f');
    signal(SIGINT, &stop);
    signal(SIGTERM, &stop);

    // 設定とマルチプレクサの構築
    Configuration config(StringToInt(argv[1]), "deploy-run.conf");
    ConnectionMultiplexer multiplexer(&config);

    // --- 1. ApplicationとStorageは全コンポーネントで共有 ---
    Application *application;
    if (argv[2][0] == 'm')
    {
        application = new Microbenchmark(config.all_nodes.size(), HOT);
    }
    else if (argv[2][0] == 't')
    {
        application = new TPCC();
    }
    else if (argv[2][0] == 'y')
    {
        const double read_ratio = 1.0;
        const double skew = 0.99;
        const uint64 db_size = DB_SIZE;
        const uint64 hot_records = 10;
        application = new YCSB(read_ratio, skew, db_size, hot_records);
    }
    else
    {
        fprintf(stderr, "Unknown benchmark type: %s\n", argv[2]);
        exit(1);
    }

    pthread_mutex_init(&mutex_, NULL);
    pthread_mutex_init(&mutex_for_item, NULL);
    involed_customers = new vector<Key>;
    Storage *storage;
    if (!useFetching)
    {
        storage = new SimpleStorage();
    }
    else
    {
        storage = FetchingStorage::BuildStorage();
    }
    storage->Initmutex();
    application->InitializeStorage(storage, &config);

    // --- 2. Connectionも全コンポーネントで共有 ---
    Connection *rw_connection = multiplexer.NewConnection("scheduler_");
    vector<Connection *> *ro_connections = new vector<Connection *>();
    for (int i = 0; i < NUM_RO_DISPATCHERS; i++)
    {
        string channel_name = "ro_scheduler_" + IntToString(i);
        ro_connections->push_back(multiplexer.NewConnection(channel_name));
    }

    // ----------- ★★★ ここからが修正の核心 ★★★ -----------
    // --- 3. 複数のClientとSequencerを生成 ---
    vector<Client *> clients;
    vector<Sequencer *> sequencers;

    // definitions.hhで定義されたNUM_SEQUENCERSの数だけループ
    for (int i = 0; i < NUM_SEQUENCERS; i++)
    {
        // Sequencerごとに独立したClientインスタンスを作成
        Client *client;
        if (argv[2][0] == 'm')
        {
            client = new MClient(&config, atoi(argv[3]), application);
        }
        else if (argv[2][0] == 't')
        {
            client = new TClient(&config, atoi(argv[3]), application);
        }
        else
        {
            client = new YClient(&config, atoi(argv[3]), application);
        }
        clients.push_back(client);

        // Sequencerを生成し、ClientとConnection、そして自身のID(i)を渡す
        // 注: この変更には、sequencer.hとsequencer.ccの修正が前提となります
        Sequencer *sequencer = new Sequencer(&config, rw_connection, ro_connections, client, storage, i);
        sequencers.push_back(sequencer);
        std::cout << "Sequencer " << i << " created and started." << std::endl;
    }

    // --- 4. Schedulerは一つだけ生成 ---
    // 全てのSequencerが、この一つのSchedulerにトランザクションを供給する
    DeterministicScheduler scheduler(&config,
                                     rw_connection,
                                     ro_connections,
                                     storage,
                                     application);
    // -----------------------------------------

    // 180秒間実行
    Spin(180);

    // メモリ解放
    for (Client *c : clients)
    {
        delete c;
    }
    for (Sequencer *s : sequencers)
    {
        delete s;
    }
    delete application;
    delete storage;
    delete involed_customers;

    for (Connection *conn : *ro_connections)
    {
        delete conn;
    }
    delete ro_connections;
    delete rw_connection;

    return 0;
}