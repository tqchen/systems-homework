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
    virtual int WorldSize(void) const {
      return parent->WorldSize();
    }
    virtual int GetRank(void) const {
      return node;
    }
    virtual bool RecvFrom(utils::Message *msg) {
      return parent->RecvFrom(node, msg);
    }
    virtual void SendTo(int nid, const utils::Message &msg) {
      parent->Send(node, nid, msg);
    }    
   protected:
    friend class PostOfficePipe;
    Poster(int node, PostOfficePipe *parent)
        : node(node), parent(parent) {}
    int node;
    PostOfficePipe *parent;
  };
  // the constructor
  PostOfficePipe(int num_nodes) {
    nodes.resize(num_nodes);
    for (int i = 0; i < num_nodes; ++i) {
      utils::Check(pipe(nodes[i].pipefd) != -1, "fail to create pipe");
      nodes[i].lock.Init();
      posters.push_back(Poster(i, this));
    }
  }
  ~PostOfficePipe(void) {
    for (size_t i = 0; i < nodes.size(); ++i) {
      close(nodes[i].pipefd[0]);
      close(nodes[i].pipefd[1]);
    }
  }
  // get poster of i-th node
  IPostOffice *GetPoster(int nid) {
    return &posters[nid];
  }
  virtual bool RecvFrom(int nid, utils::Message *msg) {
    timeval timeout;
    // use 1 sec timeout
    timeout.tv_sec = 1;
    timeout.tv_usec = 0;
    fd_set rfds;
    FD_ZERO(&rfds);
    FD_SET(nodes[nid].pipefd[0], &rfds);
    int ret = select(1, &rfds, NULL, NULL, &timeout);
    // timeout
    if (ret == 0) return false;
    utils::Check(ret >= 0, "pipe failure");    
    utils::Assert(read(nodes[nid].pipefd[0], &msg->message_size, sizeof(msg->message_size)) != 0, "cannot read");
    utils::Assert(read(nodes[nid].pipefd[0], &msg->data, msg->message_size) != 0, "postoffice, cannot read");
    return true;
  }
  virtual void Send(int from, int to, const utils::Message &msg) {
    float drop_rate = 1.0f;
    if (from != to) { 
      drop_rate = std::min(nodes[from].drop_rate, nodes[to].drop_rate);
    }
    // drop the message, simulate network failure
    if (utils::Uniform() < drop_rate) return;
    nodes[to].lock.Lock();
    // pipe can block, but it is ok here, since we eagerly read from the pipe
    utils::Check(write(nodes[to].pipefd[1], &msg.message_size, sizeof(msg.message_size)) != -1, "pipe write fail");
    utils::Check(write(nodes[to].pipefd[1], &msg.data, msg.message_size) != -1, "pipe write fail");
    nodes[to].lock.Unlock();
  } 
  virtual int WorldSize(void) const {
    return nodes.size();
  }
  // set drop rate of certain node
  virtual void SetDropRate(int nid, float p) {
    nodes[nid].drop_rate = p;
  }
 private:
  // the information about each node
  struct NodeInfo {
    int pipefd[2];
    utils::Mutex lock;
    // the probability of packet being dropped
    float drop_rate;
    NodeInfo(void) {
      drop_rate = 0;
    }
  };
  std::vector<Poster> posters;
  std::vector<NodeInfo> nodes;
};
}  // namespace consencus
#endif
