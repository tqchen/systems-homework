#ifndef POST_PIPE_INL_HPP_
#define POST_PIPE_INL_HPP_

#include <unistd.h>
#include <sys/select.h>
#include <sys/time.h>
#include <cstdlib>
#include <algorithm>
#include "./post_office.h"
#include "./utils/utils.h"
#include "./utils/thread.h"
#include "./utils/message.h"

namespace consencus {

class PostOfficePipe {
 public:
  // a poster is one instance of post office
  class Poster : public IPostOffice {
   public:
    virtual unsigned WorldSize(void) const {
      return parent->WorldSize();
    }
    virtual unsigned GetRank(void) const {
      return node;
    }
    virtual bool RecvFrom(utils::Message *msg) {
      return parent->RecvFrom(node, msg);
    }
    virtual void SendTo(unsigned nid, const utils::Message &msg) {
      parent->Send(node, nid, msg);
    }    
   protected:
    friend class PostOfficePipe;
    Poster(unsigned node, PostOfficePipe *parent)
        : node(node), parent(parent) {}
    unsigned node;
    PostOfficePipe *parent;
  };
  // the constructor
  PostOfficePipe(int num_nodes) {
    nodes.resize(num_nodes);
    for (int i = 0; i < num_nodes; ++i) {
      utils::Check(pipe(nodes[i].pipefd) != -1, "fail to create pipe");
      posters.push_back(Poster(i, this));
    }
    queue_lock.Init();
    task_counter.Init(0);
    destroy_signal = false;
    worker_thread.Start(ThreadEntry, this);
  }
  ~PostOfficePipe(void) {
    destroy_signal = true;
    task_counter.Post();
    worker_thread.Join();
    queue_lock.Destroy();
    task_counter.Destroy();    
    for (size_t i = 0; i < nodes.size(); ++i) {
      close(nodes[i].pipefd[0]);
      close(nodes[i].pipefd[1]);
    }    
  }
  // get poster of i-th node
  IPostOffice *GetPoster(unsigned nid) {
    return &posters[nid];
  }
  virtual bool RecvFrom(unsigned nid, utils::Message *msg) {
    timeval timeout;
    // use 1 sec timeout
    timeout.tv_sec = 0;
    timeout.tv_usec = 100000;
    fd_set rfds;
    FD_ZERO(&rfds);
    FD_SET(nodes[nid].pipefd[0], &rfds);
    int ret = select(FD_SETSIZE, &rfds, NULL, NULL, &timeout);
    // timeout
    if (ret == 0) return false;
    utils::Check(ret >= 0, "pipe failure");    
    utils::Assert(read(nodes[nid].pipefd[0], &msg->message_size, sizeof(msg->message_size)) != 0, "cannot read");
    utils::Assert(read(nodes[nid].pipefd[0], msg->data, msg->message_size) != 0, "postoffice, cannot read");
    return true;
  }
  virtual void Send(int from, int to, const utils::Message &msg) {
    float drop_rate = 0.0f;
    if (from != to && from != -1) { 
      drop_rate = std::max(nodes[from].drop_rate, nodes[to].drop_rate);
    }
    // drop the message, simulate network failure
    if (utils::Uniform() < drop_rate) {
      return;
    }
    std::string tmp; 
    tmp.resize(msg.message_size);
    memcpy(&tmp[0], msg.data, msg.message_size);
    queue_lock.Lock();
    queue.push(std::make_pair(tmp, to));
    queue_lock.Unlock();
    task_counter.Post();
  }
  virtual unsigned WorldSize(void) const {
    return nodes.size();
  }
  // set drop rate of certain node
  virtual void SetDropRate(unsigned nid, float p) {
    nodes[nid].drop_rate = p;
    utils::LogPrintf("[%u] !!! DropRate set to %g\n", nid, p);
  }
 private:
  /*!\brief entry point of loader thread */
  inline static THREAD_PREFIX ThreadEntry(void *pthread) {
    static_cast<PostOfficePipe*>(pthread)->RunWorkerThread();
    utils::ThreadExit(NULL);
    return NULL;
  }
  inline void RunWorkerThread(void) {
    while (!destroy_signal) {
      task_counter.Wait();
      if (destroy_signal) break;
      queue_lock.Lock();
      std::pair<std::string, unsigned> cmd = queue.front(); queue.pop();
      queue_lock.Unlock();
      unsigned nid = cmd.second;
      size_t size = cmd.first.length();
      utils::Check(write(nodes[nid].pipefd[1], &size, sizeof(size)) != -1, "pipe write fail");
      utils::Check(write(nodes[nid].pipefd[1], &cmd.first[0], size) != -1, "pipe write fail");      
    }
  }
  // the information about each node
  struct NodeInfo {
    int pipefd[2];
    // the probability of packet being dropped
    float drop_rate;
    NodeInfo(void) {
      drop_rate = 0;
    }
  };
  // destroy signal
  bool destroy_signal;
  // message queue
  std::queue< std::pair<std::string, unsigned> > queue;
  // loack for accessing message queue
  utils::Mutex queue_lock;
  // number of undelivered message
  utils::Semaphore task_counter;
  // worker thread
  utils::Thread worker_thread;
  std::vector<Poster> posters;
  std::vector<NodeInfo> nodes;
};
}  // namespace consencus
#endif
