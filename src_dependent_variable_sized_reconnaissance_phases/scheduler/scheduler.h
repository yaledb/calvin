// Author: Alexander Thomson (thomson@cs.yale.edu)
//
// A database node's Scheduler determines what transactions should be run when
// at that node. It is responsible for communicating with other nodes when
// necessary to determine whether a transaction can be scheduled. It also
// forwards messages on to the backend that are sent from other nodes
// participating in distributed transactions.

#ifndef _DB_SCHEDULER_SCHEDULER_H_
#define _DB_SCHEDULER_SCHEDULER_H_

class Application;

class Scheduler {
 public:
  virtual ~Scheduler() {}

};
#endif  // _DB_SCHEDULER_SCHEDULER_H_
