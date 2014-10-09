#include <cstdio>
#include <cstdlib>
#include <fcntl.h>
#include <unistd.h>
#include <signal.h>
#include <sys/poll.h>
#include <sys/types.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <netdb.h>
#include <netdb.h>
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
    s_addr = "127.0.0.1";
    s_port = "9000";
    this->shutdown_signal = false;
  }
  ~Server(void) {    
  }
  inline void Shutdown(void) {
    this->shutdown_signal = true;
  }
  inline void Start(void) {
    thread_pool = new utils::ThreadPool(nthread);
    this->StartListen();

    while (!shutdown_signal) {
      std::vector<pollfd> fds;
      this->CreatePoll(fds);
      utils::Check(fds.size() != 0, "BUG, fds is always not empty");
      int ret = poll(&fds[0], fds.size(), -1);
      if (ret == -1) {
        utils::Check(shutdown_signal, "poll error");
      }
      if (ret == 0) continue;
      
      for (size_t i = 0; i < states.size(); ++i) {
        if (fds[i].revents & fds[i].events) {          
          // handle event happened
          switch (states[i]->type) {
            case State::kGetName: this->HandleGetName(states[i]); break;
            case State::kFailLoad:
            case State::kLoadData: this->HandleLoadData(states[i]); break;
            case State::kSendBack: this->HandleSendBack(states[i]); break;              
            default: utils::Error("BUG, find unexpected pollfd");
          }
        }
      }
      this->CleanClosedStates();
      utils::Check(fds.back().fd == sock_listen, "BUG");
      if (fds.back().revents & POLLIN) {
        this->HandleListen();
      }
    }  
    // wait all jobs to finish
    thread_pool->WaitAllJobs();    
    // shutdown server
    close(sock_listen);
    for (size_t  i = 0; i < states.size(); ++i) {
      states[i]->Shutdown();
      delete states[i];
    }
    delete thread_pool;    
    utils::LogPrintf("shutdown server..\n");
  }
  inline void SetParam(const char *name, const char *val) {
    if (!strcmp(name, "nthread")) nthread = atoi(val);    
    if (!strcmp(name, "addr")) s_addr = val;
    if (!strcmp(name, "port")) s_port = val;
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
    // shutdown
    inline void Shutdown(void) {
      switch(type) {
        case kFailLoad: 
        case kLoadData: close(pipefd[0]); close(pipefd[1]); // no break
        case kGetName:
        case kSendBack: close(sockfd); break;
        case kClosed: break;
      }
    }
  };

  inline void StartListen(void) {
    int status;
    addrinfo hints;
    addrinfo *res;  // will point to the results
    memset(&hints, 0, sizeof(hints)); // make sure the struct is empty
    hints.ai_family = AF_UNSPEC;     // don't care IPv4 or IPv6
    hints.ai_socktype = SOCK_STREAM; // TCP stream sockets
    hints.ai_flags = AI_PASSIVE;     // fill in my IP for me

    if ((status = getaddrinfo(s_addr.c_str(), s_port.c_str(), &hints, &res)) != 0) {
      fprintf(stderr, "getaddrinfo error: %s\n", gai_strerror(status));
      exit(1);
    }
    sock_listen = socket(res->ai_family, res->ai_socktype, res->ai_protocol);
    fcntl(sock_listen, fcntl(sock_listen, F_GETFL) | O_NONBLOCK);
    utils::Check(bind(sock_listen, res->ai_addr, res->ai_addrlen) != -1,
                 "unable to bind to %s:%s\n", s_addr.c_str(), s_port.c_str());
    listen(sock_listen, 16);
    utils::LogPrintf("start server on %s:%s ...\n", s_addr.c_str(), s_port.c_str());
  }
  inline void HandleGetName(State *s) {
    size_t len = recv(s->sockfd, recvbuffer, 256, 0);
    size_t n = s->file_name.length();
    s->file_name.resize(n + len);
    memcpy(&s->file_name[n], recvbuffer, len);
    const char *ptr = strchr(s->file_name.c_str(), '\n');
    if (ptr != NULL) {      
      // change state here
      s->file_name.resize(ptr - s->file_name.c_str());
      // print ok
      utils::LogPrintf("GetName Finish: \"%s\"\n", s->file_name.c_str());
      utils::Check(pipe(s->pipefd) != -1, "fail to create pipe");
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
    fcntl(s->sockfd, fcntl(s->sockfd, F_GETFL) | O_NONBLOCK);    
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
        default: utils::Error("BUGX, find unexpected pollfd");
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
      utils::Check(write(s->pipefd[1], &sig, sizeof(sig)) != -1, "fail to write pipe");
      return;
    }
    fseek(fi, 0, SEEK_END);
    s->data_blob.resize(ftell(fi));
    fseek(fi, 0, SEEK_SET);
    if (s->data_blob.length() != 0) {
      utils::Check(fread(&s->data_blob[0], s->data_blob.length(), 1, fi) != 0, "Bug");
    }
    // succesfully load file
    utils::Check(write(s->pipefd[1], &sig, sizeof(sig)) != -1, "fail to write pipe");
  }
  // receive buffer
  char recvbuffer[256];
  // threadpool
  utils::ThreadPool *thread_pool;
  // the states
  std::vector<State*> states;
  // port and address of server
  std::string s_port, s_addr;
  // number of threads we are using
  int nthread;
  // the listening socket
  int sock_listen;
  // shutdown signal
  bool shutdown_signal;
};

Server server;

void shutdown_handler(int sig) {
  server.Shutdown();
}
void pipe_handler(int sig) {
}

int main(int argc, char *argv[]) {
  if (argc < 3) {
    printf("Usage: [IP] [port]\n"); 
    printf("proceed with default value\n");
  } 
  if (argc > 1) server.SetParam("addr", argv[1]);
  if (argc > 2) server.SetParam("port", argv[2]);
  
  struct sigaction act, pact;
  act.sa_handler = shutdown_handler;
  pact.sa_handler = pipe_handler;
  sigemptyset (&act.sa_mask);
  sigemptyset (&pact.sa_mask);
  act.sa_flags = 0; pact.sa_flags = 0;
  sigaction(SIGTERM, &act, NULL);
  sigaction(SIGINT, &act, NULL);
  sigaction(SIGPIPE, &pact, NULL);
  server.Start();
  return 0;
}
