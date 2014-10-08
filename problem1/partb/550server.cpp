#include <cstdio>
#include <cstdlib>
#include <unistd.h>
#include <sys/poll.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <cstring>
#include "utils/utils.h"
#include "utils/thread_pool.h"

// server that handles requests using async I/O
class Server {
 public:  
  // using 4 threads
  Server(void) {
    nthread = 4;
    thread_pool = NULL;
  }
  ~Server(void) {
    delete thread_pool;
  }
  inline void Start(void) {
    while (true) {
      std::vector<pollfd> fds;
      this->CreatePoll(fds);
      utils::Check(fds.size() != 0, "BUG, fds is always not empty");
      int ret = poll(&fds[0], fds.size(), 3000);
      utils::Check(ret != -1, "poll error");
      if (ret == 0) continue;
      for (size_t i = 0; i < states.size(); ++i) {
        if (fds[i].revents & fds[i].events) {
          
          // handle event happened
          switch (states[i]->type) {
            case State::kGetName: this->HandleGetName(states[i]); break;
            default: utils::Error("BUG, find unexpected pollfd");
          }
        }
      }
    }
  }
  inline void SetParam(const char *name, const char *val) {
    if (!strcmp(name, "nthread")) nthread = atoi(val);    
  }
 private:
  // a state in a request
  struct State {
    // state of type we will be in 
    enum Type {
      kGetName,
      kLoadData,
      kFailLoad,
      kSendBack,
      kClosed
    };
    // type of state we will be in
    Type type;
    // descriptor for self-pipe
    int pipefd[2];
    // socket file descriptor
    int sockfd;
    // file name to fetch
    std::string file_name;
    // data blob, storing the content of data
    std::string data_blob;
    // the end of sending buffer
    size_t send_end;
  };
  inline void HandleGetName(State *s) {
    size_t len = recv(s->sockfd, recvbuffer, 256, 0);
    size_t n = s->file_name.length();
    s->file_name.resize(n + len);
    memcpy(&s->file_name[n], recvbuffer, len);
    if (s->file_name[n+len-1] == '\n') {
      // change state here
      s->file_name.resize(n+len-1);
      pipe(s->pipefd);
      thread_pool->AddJob(FileLoader, s);
    }    
  }
  // create a list of fds we want to poll on 
  inline void CreatePoll(std::vector<pollfd> &fds) {
    fds.clear();
    for (size_t i = 0; i < states.size(); ++i) {
      pollfd pfd;   
      switch (states[i]->type) {
        case State::kGetName: pfd.fd = states[i]->sockfd; pfd.events = POLLIN; break;
        case State::kLoadData: pfd.fd = states[i]->pipefd[0]; pfd.events = POLLIN; break;
        case State::kSendBack: pfd.fd = states[i]->sockfd; pfd.events = POLLOUT; break;
        default: utils::Error("BUG, find unexpected pollfd");
      }
      fds.push_back(pfd);
    }
    {// push back listen socket
      pollfd pfd;
      pfd.fd = sock_listen;
      pfd.events = POLLIN;
      fds.push_back(pfd);
    }
  }
  // file loader function
  inline static void FileLoader(void *arg) {
    char sig = '\0';
    State *s = static_cast<State*>(arg);
    FILE *fi = fopen(s->file_name.c_str(), "rb");
    // fail to load file
    if (fi == NULL) {
      s->type = State::kFailLoad;
      write(s->pipefd[1], &sig, sizeof(sig));
      return;
    }
    fseek(fi, 0, SEEK_END);
    s->data_blob.resize(ftell(fi));
    fseek(fi, 0, SEEK_SET);
    if (s->data_blob.length() != 0) {
      fread(&s->data_blob[0], s->data_blob.length(), 1, fi);
    }
    // succesfully load file
    s->type = State::kSendBack;
    write(s->pipefd[1], &sig, sizeof(sig));
  }
  // receive buffer
  char recvbuffer[256];
  // threadpool
  utils::ThreadPool *thread_pool;
  // the states
  std::vector<State*> states;
  // number of threads we are using
  int nthread;
  // the listening socket
  int sock_listen;
};

int main(int argc, char *argv[]) {
  return 0;
}
