#include <sys/select.h>
#include <sys/time.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <signal.h>
#include "./utils/utils.h"
#include "./lock_server-inl.hpp"
#include "./post_udp-inl.hpp"

using namespace consencus;

LockServer *server = NULL;
PostOfficeUDP *post = NULL;

void shutdown_handler(int sig) {
  if (server != NULL) { 
    server->Shutdown();
  }
}

int main(int argc, char *argv[]) {
  if (argc < 4) {
    printf("Usage: <Node-Address-List> <nserver> <current-node-id>\n");
    return 0;           
  }
  int nserver = atoi(argv[2]);
  int node_id = atoi(argv[3]);
  std::vector<sockaddr_in> addrs;
  PostOfficeUDP::LoadAddrs(argv[1], &addrs);
  
  struct sigaction act;
  act.sa_handler = shutdown_handler;
  sigemptyset (&act.sa_mask);
  act.sa_flags = 0; 
  sigaction(SIGTERM, &act, NULL);
  sigaction(SIGINT, &act, NULL);
  post = new PostOfficeUDP(addrs, node_id);
  server = new LockServer(post, nserver);
  utils::LogPrintf("[%d] start lock server...\n", node_id);
  server->RunServer();

  delete server;
  delete post;

  return 0;
}
