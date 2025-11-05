#pragma once

#include <string>
#include <iostream>
#include <sched.h>  // これを追加

static void PrintCpu(std::string worker_name, int id) {
  std::cout << worker_name << " is on cpu " << sched_getcpu() << std::endl;
}