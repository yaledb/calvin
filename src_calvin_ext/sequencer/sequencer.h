#ifndef _DB_SEQUENCER_SEQUENCER_H_
#define _DB_SEQUENCER_SEQUENCER_H_

#include <set>
#include <string>
#include <queue>
#include <vector>

#include "common/definitions.hh"

using std::queue;
using std::set;
using std::string;
using std::vector;

class Configuration;
class Connection;
class Storage;
class TxnProto;

class Client
{
public:
    virtual ~Client() {}
    virtual void GetTxn(TxnProto **txn, int txn_id) = 0;
};

class Sequencer
{
public:
    // ★ コンストラクタに sequencer_id を追加
    Sequencer(Configuration *conf,
              Connection *rw_connection,
              vector<Connection *> *ro_connections,
              Client *client,
              Storage *storage,
              int sequencer_id); // ★ sequencer_id を追加

    ~Sequencer();

private:
    void RunWriter();
    void RunReader();
    static void *RunSequencerWriter(void *arg);
    static void *RunSequencerReader(void *arg);
    void FindParticipatingNodes(const TxnProto &txn, set<int> *nodes);

    double epoch_duration_;
    Configuration *configuration_;
    Connection *rw_connection_;
    vector<Connection *> *ro_connections_;
    Client *client_;
    Storage *storage_;

    int sequencer_id_; // ★ sequencer_id を保持するメンバー変数を追加

    pthread_t writer_thread_;
    pthread_t reader_thread_;
    bool deconstructor_invoked_;
    queue<string> batch_queue_;
    pthread_mutex_t mutex_;
};
#endif // _DB_SEQUENCER_SEQUENCER_H_