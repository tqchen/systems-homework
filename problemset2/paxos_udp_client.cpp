#include <sys/select.h>
#include <sys/time.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <signal.h>
#include "./utils/utils.h"
#include "./lock_client-inl.hpp"
#include "./post_udp-inl.hpp"

using namespace consencus;

int main(int argc, char *argv[]) {
  if (argc < 4) {
    printf("Usage: <Node-Address-List> <nserver> <current-node-id>\n");
    return 0;           
  }
  int nserver = atoi(argv[2]);
  int node_id = atoi(argv[3]);
  std::vector<sockaddr_in> addrs;
  PostOfficeUDP::LoadAddrs(argv[1], &addrs);
    
  PostOfficeUDP post(addrs, node_id);
  {
    utils::Check(node_id >= nserver && node_id < (int)post.WorldSize(),
                 "client node id should be between nserver and nnode");
    LockClient locker(&post, nserver);
    char *scmd;
    size_t n = 0;
    while (getline(&scmd, &n, stdin) != -1) {
      // remove \n
      scmd[strlen(scmd) - 1] = '\0';
      n = 0;  
      unsigned lock_index;
      if (sscanf(scmd, "lock(%u)", &lock_index) == 1) {
        utils::LogPrintf("[%d] start exec lock(%u)\n", node_id, lock_index);
        locker.Lock(lock_index);
        utils::LogPrintf("[%d] finish exec lock(%u)\n", node_id, lock_index);
        continue;
      }
      if (sscanf(scmd, "unlock(%u)", &lock_index) == 1) {
        utils::LogPrintf("[%d] start exec unlock(%u)\n", node_id, lock_index);
        locker.UnLock(lock_index);
        utils::LogPrintf("[%d] finish exec unlock(%u)\n", node_id, lock_index);
        continue;
      }
      printf("unknown command \"%s\"", scmd);
    }
  }  
  return 0;
}
