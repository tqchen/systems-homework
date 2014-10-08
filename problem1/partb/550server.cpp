#include <cstdio>
#include <cstdlib>
#include <fcntl.h>
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
            case State::kLoadData: this->HandleLoadData(states[i]); break;
            case State::kSendBack: this->HandleSendBack(states[i]); break;
            default: utils::Error("BUG, find unexpected pollfd");
          }
        }
      }
      this->CleanClosedStates();
      if (fds.back().revents & fds.back().events) {
        this->HandleListen();
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
    size_t sent_bytes;
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
      // set pipe to non blocking
      fcntl(s->pipefd[0], fcntl(s->pipefd[0], F_GETFL) | O_NONBLOCK);
      // switch to load data
      s->type = State::kLoadData;
      thread_pool->AddJob(FileLoader, s);
    }
  }
  inline void HandleLoadData(State *s) {
    char sig;
    if (read(s->pipefd[0], &sig, sizeof(sig)) == 0) return;
    utils::Check(sig == '\0', "BUG");
    // close the necessary pipe
    close(s->pipefd[0]);
    close(s->pipefd[1]);
    
    if (s->type == State::kLoadData) {
      // finish loading
      s->sent_bytes = 0;
      s->type = State::kSendBack;    
    } else {
      utils::Check(s->type == State::kFailLoad, "type must be loading or fail to load");
      close(s->sockfd);
      // fail to load
      s->type = State::kClosed;
    }
  }
  inline void HandleSendBack(State *s) {
    size_t &sent_bytes = s->sent_bytes;
    const std::string &data_blob = s->data_blob;
    size_t len = send(s->sockfd, data_blob.c_str() + sent_bytes, data_blob.length() - sent_bytes, 0);
    sent_bytes += len;
    if (sent_bytes == data_blob.length()) {
      close(s->sockfd);
      // fail to load
      s->type = State::kClosed;
    }
  }
  inline void CleanClosedStates(void) {
    size_t top = 0;
    for (size_t j = 0; j < states.size(); ++j) {
      if (states[j]->type == State::kClosed) {
        delete states[j];
      } else {
        states[top++] = states[j];        
      }
    }
    states.resize(top);
  }
  inline void HandleListen(void) {
    State *s = new State();
    s->sockfd = accept(sock_listen, NULL, NULL);
    s->type = State::kGetName;
    states.push_back(s);
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
