// Author: Alexander Thomson (thomson@cs.yale.edu)
//
// SerialScheduler is a trivial scheduler that executes transactions serially
// as they come in, without locking.

#ifndef _DB_SCHEDULER_SERIAL_SCHEDULER_H_
#define _DB_SCHEDULER_SERIAL_SCHEDULER_H_

#include "scheduler/scheduler.h"

class Configuration;
class Connection;
class Storage;

class SerialScheduler : public Scheduler {
 public:
  SerialScheduler(Configuration* conf, Connection* connection,
                  Storage* storage, bool checkpointing);
  virtual ~SerialScheduler();
  virtual void Run(const Application& application);

 private:
  // Configuration specifying node & system settings.
  Configuration* configuration_;

  // Connection for sending and receiving protocol messages.
  Connection* connection_;

  // Storage layer used in application execution.
  Storage* storage_;

  // Should we checkpoint?
  bool checkpointing_;
};
#endif  // _DB_SCHEDULER_SERIAL_SCHEDULER_H_
