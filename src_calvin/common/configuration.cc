// Author: Shu-chun Weng (scweng@cs.yale.edu)
// Author: Alexander Thomson (thomson@cs.yale.edu)
// Author: Kun Ren (kun@cs.yale.edu)

#include "common/configuration.h"

#include <netdb.h>
#include <netinet/in.h>

#include <cctype>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <string>

#include "common/utils.h"

using std::string;

Configuration::Configuration(int node_id, const string& filename)
    : this_node_id(node_id) {
  if (ReadFromFile(filename))  // Reading from file failed.
    exit(0);
}

// TODO(alex): Implement better (application-specific?) partitioning.
int Configuration::LookupPartition(const Key& key) const {
  if (key.find("w") == 0)  // TPCC
    return OffsetStringToInt(key, 1) % static_cast<int>(all_nodes.size());
  else
    return StringToInt(key) % static_cast<int>(all_nodes.size());
}

bool Configuration::WriteToFile(const string& filename) const {
  FILE* fp = fopen(filename.c_str(), "w");
  if (fp == NULL)
      return false;
  for (map<int, Node*>::const_iterator it = all_nodes.begin();
       it != all_nodes.end(); ++it) {
    Node* node = it->second;
    fprintf(fp, "node%d=%d:%d:%d:%s:%d\n",
            it->first,
            node->replica_id,
            node->partition_id,
            node->cores,
            node->host.c_str(),
            node->port);
  }
  fclose(fp);
  return true;
}

int Configuration::ReadFromFile(const string& filename) {
  char buf[1024];
  FILE* fp = fopen(filename.c_str(), "r");
  if (fp == NULL) {
    printf("Cannot open config file %s\n", filename.c_str());
    return -1;
  }
  char* tok;
  // Loop through all lines in the file.
  while (fgets(buf, sizeof(buf), fp)) {
    // Seek to the first non-whitespace character in the line.
    char* p = buf;
    while (isspace(*p))
      ++p;
    // Skip comments & blank lines.
    if (*p == '#' || *p == '\0')
      continue;
    // Process the rest of the line, which has the format "<key>=<value>".
    char* key = strtok_r(p, "=\n", &tok);
    char* value = strtok_r(NULL, "=\n", &tok);
    ProcessConfigLine(key, value);
  }
  fclose(fp);
  return 0;
}

void Configuration::ProcessConfigLine(char key[], char value[]) {
  if (strncmp(key, "node", 4) != 0) {
#if VERBOSE
    printf("Unknown key in config file: %s\n", key);
#endif
  } else {
    Node* node = new Node();
    // Parse node id.
    node->node_id = atoi(key + 4);

    // Parse additional node addributes.
    char* tok;
    node->replica_id   = atoi(strtok_r(value, ":", &tok));
    node->partition_id = atoi(strtok_r(NULL, ":", &tok));
    node->cores        = atoi(strtok_r(NULL, ":", &tok));
    const char* host   =      strtok_r(NULL, ":", &tok);
    node->port         = atoi(strtok_r(NULL, ":", &tok));

    // Translate hostnames to IP addresses.
    string ip;
    {
      struct hostent* ent = gethostbyname(host);
      if (ent == NULL) {
        ip = host;
      } else {
        uint32_t n;
        char buf[32];
        memmove(&n, ent->h_addr_list[0], ent->h_length);
        n = ntohl(n);
        snprintf(buf, sizeof(buf), "%u.%u.%u.%u",
            n >> 24, (n >> 16) & 0xff,
            (n >> 8) & 0xff, n & 0xff);
        ip = buf;
      }
    }
    node->host = ip;

    all_nodes[node->node_id] = node;
  }
}

