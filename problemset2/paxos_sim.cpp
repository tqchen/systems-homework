#include <queue>

#include "./lock_server-inl.hpp"
#include "./lock_client-inl.hpp"
#include "./post_pipe-inl.hpp"

using namespace consencus;

// use to simulate client 
class ClientThread {
 public:
  ClientThread(IPostOffice *post, unsigned num_server, int nodeid)
      : locker(post, num_server), nodeid(nodeid) {
    task_counter.Init(0);
    queue_lock.Init();
    destroy_signal = false;
    worker_thread.Start(ThreadEntry, this);
  }
  ~ClientThread(void) {
    while(queue.size() != 0) {
      sleep(10);
    }
    destroy_signal = true;
    task_counter.Post();
    worker_thread.Join();
    queue_lock.Destroy();
    task_counter.Destroy();
    printf("[%d] client thread shutdown\n", nodeid);
  }
  inline void RunCmd(int lock, unsigned lock_index) {
    queue_lock.Lock();
    queue.push(std::make_pair(lock, lock_index));    
    queue_lock.Unlock();
    task_counter.Post();
  }  

 private:
  /*!\brief entry point of loader thread */
  inline static THREAD_PREFIX ThreadEntry(void *pthread) {
    static_cast<ClientThread*>(pthread)->RunWorkerThread();
    utils::ThreadExit(NULL);
    return NULL;
  }
  inline void RunWorkerThread(void) {
    while (!destroy_signal) {
      task_counter.Wait();
      if (destroy_signal) break;
      queue_lock.Lock();
      std::pair<int, unsigned> cmd = queue.front(); queue.pop();
      queue_lock.Unlock();
      if (cmd.first == 0) {
        utils::LogPrintf("[%d] !!!start exec lock(%d)\n", nodeid, cmd.second);
        locker.Lock(cmd.second);
        utils::LogPrintf("[%d] !!!finish exec lock(%d)\n", nodeid, cmd.second);
      } else {
        utils::LogPrintf("[%d] !!!start exec unlock(%d)\n", nodeid, cmd.second);
        locker.UnLock(cmd.second);
        utils::LogPrintf("[%d] !!!finish exec unlock(%d)\n", nodeid, cmd.second);
      }
    }
  }  
  // the lock interface
  LockClient locker;
  // node id
  int nodeid;
  // destroy
  bool destroy_signal;
  // lock for accessing the queue
  utils::Mutex queue_lock;  
  std::queue< std::pair<int, unsigned> > queue;
  // number of tasks in the quque
  utils::Semaphore task_counter;  
  // worker thread
  utils::Thread worker_thread;  
};

class ServerThread {
 public:
  ServerThread(IPostOffice *post, unsigned num_server) 
      : server(post, num_server) {
    worker_thread.Start(ThreadEntry, this);
  }
  ~ServerThread(void) {
    server.Shutdown();
    worker_thread.Join();
  }
 private:
  /*!\brief entry point of loader thread */
  inline static THREAD_PREFIX ThreadEntry(void *pthread) {
    static_cast<ServerThread*>(pthread)->server.RunServer();
    utils::ThreadExit(NULL);
    return NULL;
  }
  LockServer server;
  // worker thread
  utils::Thread worker_thread;  
};

int main(int argc, char *argv[]) {
  if (argc < 1) { 
    printf("Usage: stdin/script\n");
    printf("First two lines of input must be num_server=number\n num_nodes=number\n");
    printf("0 to num_server - 1 will be lock server, num_server to num_nodes - 1 will be client\n");
    printf("the input from stdin can contain a sequence of command\n");
    printf("possible commands are in format param=value, no space in param and value since it is not a good parser:)\n");
    printf("Commands:\n");
    printf("\t  drop[node-id]=droprate: set drop rate of node, set droprate=1 means we isolate the node from rest of the group\n");
    printf("\t  exec[client-id]=lock[lock-id]\n");
    printf("\t  exec[client-id]=unlock[lock-id]\n");
    return 0;
  }
  int nserver, nnodes;
  FILE *fi = stdin;
  if (strcmp(argv[1], "stdin")) {
    fi = fopen(argv[1], "r");
    utils::Check(fi != NULL, "fail to open \"%s\"", argv[1]);
  }
  utils::Check(fscanf(fi, "num_server=%d\n", &nserver) == 1, "first line must be num_server=number");
  utils::Check(fscanf(fi, "num_node=%d", &nnodes) == 1, "second line must be num_node=number");
  utils::Check(nserver < nnodes, "num_server must be smaller than num_nodes");
  
  PostOfficePipe post(nnodes);
  {
    std::vector<ClientThread*> clients;
    std::vector<ServerThread*> servers;
    for (int i = 0; i < nserver; ++i) {
      servers.push_back(new ServerThread(post.GetPoster(nserver-i-1), nserver));
    }
    for (int i = nserver; i < nnodes; ++i) {
      clients.push_back(new ClientThread(post.GetPoster(i), nserver, i));
    }
    printf("Start working with %d servers %d nodes\n", nserver, nnodes);
    char *scmd;
    size_t n = 0;
    while (getline(&scmd, &n, fi) != -1) {
      // remove \n
      scmd[strlen(scmd) - 1] = '\0';
      n = 0;  
      char name[256], val[256];
      if (sscanf(scmd, "%[^=]=%[^\n]\n", name, val) != 2) continue;
      if (!strncmp(name, "exec[", 5)) {
        int nid, lockid;
        utils::Check(sscanf(name, "exec[%d]", &nid) == 1, "invalid command");
        utils::Check(nid >= nserver && nid < nnodes, "client id");        
        if (sscanf(val, "lock[%d]", &lockid) == 1) {
          clients[nid - nserver]->RunCmd(0, lockid);
        } else {
          utils::Check(sscanf(val, "unlock[%d]", &lockid) == 1, "exec must be lock or unlock");
          clients[nid - nserver]->RunCmd(1, lockid);
        }
      }
      if (!strncmp(name, "drop[", 5)) {
        int nid, nid2;
        if (sscanf(name, "drop[%d-%d]", &nid, &nid2) == 2) {
          for (int i = nid; i <= nid2; ++i) {
            post.SetDropRate(i, atof(val));
          }
          continue;
        }
        if (sscanf(name, "drop[%d]", &nid) == 1) {
          post.SetDropRate(nid, atof(val));
          continue;
        }
      }
      if (!strncmp(name, "raise[",6)) {
        int nid = atoi(val);
        if (sscanf(name, "raise[%d]", &nid) != 1) continue;
        utils::Message msg;
        unsigned from = 0, inc = atoi(val);
        msg.WriteT(from);
        msg.WriteT(LockServer::kBecomeLeader);
        msg.WriteT(inc);
        post.Send(-1, nid, msg);
        continue;
      }
      if (!strcmp(name, "sleep")) {
        sleep(atoi(val)); continue;
      }      
      sleep(1);
    }
    if (fi != stdin) fclose(fi);
    for (size_t i = 0; i < clients.size(); ++i) {      
      delete clients[i];
    }
    for (size_t i = 0; i < servers.size(); ++i) {
      delete servers[i];
    }
  }
  return 0;
}
