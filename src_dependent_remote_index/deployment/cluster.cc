// Author: Shu-chun Weng (scweng@cs.yale.edu)
// Author: Kun Ren (kun.ren@yale.edu)
// Author: Alexander Thomson (thomson@cs.yale.edu)

#include <sys/types.h>
#include <sys/select.h>
#include <sys/stat.h>
#include <unistd.h>

#include <cassert>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <cerrno>
#include <csignal>
#include <ctime>

#include <map>
#include <vector>
#include <utility>

#include "common/configuration.h"

using std::map;
using std::vector;

void ConstructDBArgs(int argc, char* argv[], int arg_begin);
bool CheckExecutable(const char* exec);
int UpdatePorts(int port_begin, Configuration* config);
void Deploy(const Configuration& config, const char* exec);

const char default_input_config_filename[] = "deployment/test.conf";
const char default_port_filename[]         = "deployment/portfile";
const char default_executable_filename[]   = "../obj/deployment/db";

const char default_run_config_filename[] = "deploy-run.conf";
const char remote_exec[] = "ssh";

// Command templates.

// Redirects stdin from /dev/null (actually, prevents reading from stdin).
// This must be used when ssh is run in the background.
const char remote_opt1[] = "-nT";
// remote_opt2 = address
const char remote_opt3_fmt[] = "cd %s; %s %d %s";
const char remote_quite_opt3_fmt[] = "cd %s; %s %d %s > /dev/null 2>&1";
const char remote_valgrind_opt3_fmt[] = "cd %s; valgrind %s %d %s";
// sprintf(remote_opt3, remote_opt3_fmt,
//         cwd, per-exec, node-id, db-args (joined with spaces))


// TODO(scw): make deployer class; these should be class objs
char* cwd;
char* db_args;

bool do_valgrind;
bool do_quite;

// TODO(scw): move to deplayer class; should avoid non-POD global variable
// Type: fd -> nodeID
map<int, int> children_pipes;
vector<int> children_pids;
volatile bool end_cluster;

int main(int argc, char* argv[]) {
  int arg_begin;

  const char* config_file = default_input_config_filename;
  const char* port_file   = default_port_filename;
  const char* exec        = default_executable_filename;

  for (arg_begin = 1; arg_begin < argc; ++arg_begin) {
    if (strcmp(argv[arg_begin], "-h") == 0) {
      printf("Usage: %s [-v|-q] [-c config-file] [-p port-file]\n"
             "          [-d db-exec] [db-args..]\n"
             "  -c config-file   default: %s\n"
             "  -p port-file     default: %s\n"
             "  -d db-exec       default: %s\n",
             argv[0],
             default_input_config_filename,
             default_port_filename,
             default_executable_filename);
      return 0;
    } else if (strcmp(argv[arg_begin], "-c") == 0) {
      config_file = argv[++arg_begin];
      assert(arg_begin < argc);
    } else if (strcmp(argv[arg_begin], "-d") == 0) {
      exec = argv[++arg_begin];
      assert(arg_begin < argc);
    } else if (strcmp(argv[arg_begin], "-p") == 0) {
      port_file = argv[++arg_begin];
      assert(arg_begin < argc);
    } else if (strcmp(argv[arg_begin], "-q") == 0) {
      do_quite = true;
    } else if (strcmp(argv[arg_begin], "-v") == 0) {
      do_valgrind = true;
    } else {
      break;
    }
  }

  // filling in db_args from argv
  ConstructDBArgs(argc, argv, arg_begin);

  // get a config obj
  Configuration config(-1, config_file);
  cwd = getcwd(NULL, 0);

  FILE* port_fp = fopen(port_file, "r");
  int port_begin;
  if (port_fp == NULL) {
    printf("Cannot read port file '%s': %s\n", port_file, strerror(errno));
    return -1;
  } else if (fscanf(port_fp, "%d", &port_begin) != 1) {
    printf("port-file should contain a number\n");
    fclose(port_fp);
    return -1;
  }
  fclose(port_fp);

  if (CheckExecutable(exec)) {
    printf("Executable's problem\n");
    return -1;
  }

  int next_port = UpdatePorts(port_begin, &config);

  port_fp = fopen(port_file, "w");
  if (port_fp == NULL) {
    printf("Cannot write port file '%s': %s\n", port_file, strerror(errno));
    return -1;
  }
  fprintf(port_fp, "%d\n", next_port);
  fclose(port_fp);

//  for (map<int, Node*>::iterator it = config.all_nodes.begin();
//       it != config.all_nodes.end(); ++it) {
//    char copy_config[1024];
//    snprintf(copy_config, sizeof(copy_config),
//             "scp -rp deploy-run.conf %s:db3/deploy-run.conf",
//             it->second->host.c_str());
//    system(copy_config);
//  }

  Deploy(config, exec);

  delete[] db_args;
  return 0;
}

void ConstructDBArgs(int argc, char* argv[], int arg_begin) {
  int len = 0;
  for (int i = arg_begin; i < argc; ++i)
    len += strlen(argv[i]) + 1;
  db_args = new char[len + 1];

  char* p = db_args;
  for (int i = arg_begin; i < argc; ++i)
    p += sprintf(p, " %s", argv[i]);
}

bool CheckExecutable(const char* exec) {
  struct stat buf;
  if (stat(exec, &buf) < 0) {
    printf("Cannot access %s: %s\n", exec, strerror(errno));
    return true;
  }
  if (!S_ISREG(buf.st_mode) || !(buf.st_mode & S_IXUSR)) {
    printf("Cannot execute %s\n", exec);
    return true;
  }
  return false;
}

int UpdatePorts(int port_begin, Configuration* config) {
  map<string, int> next_port_map;
  for (map<int, Node*>::const_iterator it = config->all_nodes.begin();
       it != config->all_nodes.end(); ++it) {
    Node* node = it->second;

    // Insert <node->host, port_begin> if the host has not appeared.
    // Otherwise, use the existing host-port pair.
    map<string, int>::iterator port_it =
      next_port_map.insert(std::make_pair(node->host, port_begin))
                   .first;

    node->port = port_it->second;
    port_it->second = port_it->second + 1;
  }

  int max_next_port = -1;
  for (map<string, int>::const_iterator it = next_port_map.begin();
       it != next_port_map.end(); ++it)
    if (it->second > max_next_port)
      max_next_port = it->second;

  return max_next_port;
}

// deploy Node node with specified nodeid and args
void DeployOne(int nodeID,
               const Node* node,
               const char* exec,
               const char* config_file) {
  const char* remote_opt2 = node->host.c_str();

  char copy_config[1024];
  snprintf(copy_config, sizeof(copy_config),
           "scp -rp deploy-run.conf %s:db3/deploy-run.conf",
           node->host.c_str());
  system(copy_config);

  char remote_opt3[1024];
  if (do_valgrind)
    snprintf(remote_opt3, sizeof(remote_opt3), remote_valgrind_opt3_fmt,
             cwd, exec, nodeID, db_args);
  else if (do_quite)
    snprintf(remote_opt3, sizeof(remote_opt3), remote_quite_opt3_fmt,
             cwd, exec, nodeID, db_args);
  else
    snprintf(remote_opt3, sizeof(remote_opt3), remote_opt3_fmt,
             cwd, exec, nodeID, db_args);

  // Black magic, don't touch (bug scw if this breaks).
  int pipefd[2];
  pipe(pipefd);
  int pid = fork();
  if (pid == 0) {
    setsid();
    close(pipefd[0]);
    dup2(pipefd[1], 1);
    dup2(pipefd[1], 2);
    close(pipefd[1]);
    execlp("ssh", "ssh", remote_opt1, remote_opt2, remote_opt3, NULL);
    printf("Node %d spawning failed\n", nodeID);
    exit(-1);
  } else if (pid < 0) {
    printf("Node %d forking failed\n", nodeID);
  } else {
    children_pids.push_back(pid);
    children_pipes.insert(std::pair<int, int>(pipefd[0], nodeID));
    close(pipefd[1]);
  }

  timespec to_sleep = { 0, 100000000 };  // 0.1 sec
  nanosleep(&to_sleep, NULL);

  (void) config_file;
}

void TerminatingChildren(int sig);
void KillRemote(const Configuration& config, const char* exec, bool client_int);
void KillLocal();

// deploy all nodes specified in config
void Deploy(const Configuration& config, const char* exec) {
  if (!config.WriteToFile(default_run_config_filename)) {
    printf("Unable to create temporary config file '%s'\n",
           default_run_config_filename);
    return;
  }

  // use DeployOne to span components
  for (map<int, Node*>::const_iterator it = config.all_nodes.begin();
       it != config.all_nodes.end(); ++it)
    DeployOne(it->first, it->second, exec, default_run_config_filename);

  // BLOCK A: Grab messages from all components, prepend node number, print.
  end_cluster = false;
  signal(SIGINT, &TerminatingChildren);
  signal(SIGTERM, &TerminatingChildren);
  signal(SIGPIPE, &TerminatingChildren);

  int num_fd = children_pipes.size();
  int max_fd = 0;
  fd_set readset, fds;
  FD_ZERO(&fds);
  for (map<int, int>::const_iterator it = children_pipes.begin();
       it != children_pipes.end(); ++it) {
    if (it->first > max_fd)
      max_fd = it->first;
    FD_SET(it->first, &fds);
  }
  ++max_fd;

  char buf[4096];
  while (num_fd > 0) {  // while there are still any components alive
    if (end_cluster) {
      KillRemote(config, exec, false);
      end_cluster = false;
    }

    readset = fds;
    int actions = select(max_fd, &readset, NULL, NULL, NULL);
    if (actions == -1) {
      if (errno == EINTR)
        continue;
      break;
    }

    vector<int> erasing;
    for (map<int, int>::const_iterator it = children_pipes.begin();
         it != children_pipes.end(); ++it) {
      if (FD_ISSET(it->first, &readset)) {
        int n;
        if ((n = read(it->first, buf, sizeof(buf))) <= 0) {
          erasing.push_back(it->first);
        } else {
          buf[n] = 0;

          char* save_p;
          char* p = strtok_r(buf, "\n", &save_p);
          do {
            printf("%02d: %s\n", it->second, p);
            p = strtok_r(NULL, "\n", &save_p);
          } while (p);
        }
      }
    }
    if (erasing.size() > 0) {
      for (vector<int>::const_iterator it = erasing.begin();
           it != erasing.end(); ++it) {
        children_pipes.erase(*it);
        FD_CLR(*it, &fds);
      }
      num_fd -= erasing.size();
    }
  }
  // at the end of this while statement, either there was a user interrupt or
  // no components remain alive

  // END BLOCK A

  // kill all active components & all ssh procs
  KillRemote(config, exec, false);

  timespec to_sleep = { 1, 0 };  // 1 sec
  nanosleep(&to_sleep, NULL);
  KillLocal();
}

// Handles CTRL-C and other terminating signals and dies gracefully.
void TerminatingChildren(int sig) {
  end_cluster = true;
}

// try to kill all remote processes spawned
void KillRemote(const Configuration& config,
                const char* exec, bool client_int) {
  const char* sig_arg;
  if (client_int)
    sig_arg = "-INT";
  else
    sig_arg = "-TERM";

  char exec_fullpath[1024];
  snprintf(exec_fullpath, sizeof(exec_fullpath), "%s/%s",
           cwd, exec);

  for (map<int, Node*>::const_iterator it = config.all_nodes.begin() ;
    it != config.all_nodes.end(); ++it) {
    Node* node = it->second;

    int pid = fork();
    if (pid == 0) {
      execlp("ssh", "ssh", node->host.c_str(),
             "killall", sig_arg, exec_fullpath, NULL);
      exit(-1);
    }
  }
}

// kill all processes forked on local machine
void KillLocal() {
  for (vector<int>::const_iterator it = children_pids.begin();
       it != children_pids.end(); ++it)
    kill(*it, SIGTERM);
}
