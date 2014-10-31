#ifndef LOCK_SERVER_INL_H_
#define LOCK_SERVER_INL_H_

#include <queue>
#include <deque>
#include <list>
#include <utility>

#include "./utils/utils.h"
#include "./utils/message.h"
#include "./post_office.h"
#include "./utils/thread.h"
namespace consencus {

struct LockMessage {
  // type of messages passed in lock signal
  enum Type {
    kNull,
    // client message to server
    kLockRequest,
    kUnlockRequest,
    kClientLockGrantedAck,
    // server message to client 
    kServerLockGranted,
    kServerAck
  };
  /*! \brief type of the message */
  Type type;
  /*! \brief the lock index that the client would like to aqquire */
  unsigned lock_index;
  /*! \brief the unique counter of the message */
  unsigned counter;
  /*! \brief node index of the client */
  unsigned node_client;
  // check equivalence of two lock message
  inline bool operator==(const LockMessage &b) const {
    return type == b.type && 
        lock_index == b.lock_index &&
        counter == b.counter &&
        node_client == b.node_client;
  }
};

class LockServer : public MultiPaxos<LockMessage> {
 public:
  LockServer(IPostOffice *post, int num_server) 
      : MultiPaxos<LockMessage>(post, num_server) {
    latest_chosen.resize(post->WorldSize());
    latest_scan = 0;
    cmd_ptr = 0;
  }
 protected:
  typedef MultiPaxos<LockMessage> Parent;
  
  virtual void HandleClientRequest(utils::IStream &in, unsigned sender) {
    // skip slave mode
    if (server_state == kSlave) return;
    // note: message maybe duplicated
    // need a duplicate detection scheme
    LockMessage msg;
    in.Read(&msg, sizeof(msg));
    utils::Assert(msg.node_client == sender, "invalid client message");
    if (msg.type == LockMessage::kClientLockGrantedAck) {
      this->HandleLockGrantedAck(msg); return;
    }
    if (!CheckChosen(msg)) {
      // avoid add duplicated elements into queue
      for (std::deque<LockMessage>::iterator it = queue.begin(); 
           it != queue.end(); ++it) {
        if (*it == msg) return;
      }
      this->queue.push_back(msg);
    } else {
      // acknowledge that this message have been chosen
      // (i.e. it is remembered by DSM)
      this->SendServerAck(msg);
    }
  }
  virtual bool GetNewValue(LockMessage *p_value) {
    utils::Assert(server_state == kLeaderAccept, "invalid server state");
    while (queue.size() != 0) {
      LockMessage cmd = queue.front(); queue.pop_front();
      if (!CheckChosen(cmd)) {
        *p_value = cmd; return true;
      } else {
        this->SendServerAck(cmd);
      }
    }
    return false;
  }
  virtual void HandleChosenEvent(unsigned inst_index) {
    const LockMessage &cmd = server_rec[inst_index].value;
    // acknowledge that this command have been chosen
    this->SendServerAck(cmd);
    
    while (cmd_ptr <= inst_index) {
      const ProposeState &c = server_rec[cmd_ptr];
      utils::Assert(c.type == ProposeState::kChosen, "invalid server rec, chosen");
      if (lock_state.size() <= c.value.lock_index) {
        lock_state.resize(c.value.lock_index);
      } 
      lock_state[c.value.lock_index].Exec(c.value);
      // if these cmd are skiped by HandleChosenEvent
      // these cmd are being proposed and chosen by another leader
      // the other leader will only send out the notification when holder is acknowledgeed
      ++cmd_ptr;
    }
    utils::Assert(cmd_ptr == inst_index + 1, "BUG");
    // if this instance does not result in new holder
    // broadcast the chosen information now
    if (!lock_state[cmd.lock_index].new_holder) {
      this->SendChosenNotify(inst_index);
    } else {
      const LockMessage &h = lock_state[cmd.lock_index].holder;
      holder_notify.push_back(std::make_pair(inst_index, h));
      this->SendHolderNotification(h);
    }
  }
  inline void HandleLockGrantedAck(const LockMessage &msg) {
    for (std::list< std::pair<unsigned, LockMessage> >::iterator 
             it = holder_notify.begin(); it != holder_notify.end(); ++it) {
      if (it->second == msg) {
        // get ack back from holder, now it is safe to broadcast 
        // the chosen message
        this->SendChosenNotify(it->first);
        // delete from notification queue
        holder_notify.erase(it);
        break;
      }
    }
  }
  virtual void HandleTimeOut(void) {
    // re-transmit lock grant message
    for (std::list< std::pair<unsigned, LockMessage> >::iterator it = holder_notify.begin();
         it != holder_notify.end(); ++it) {
      this->SendHolderNotification(it->second);
    }
    Parent::HandleTimeOut();
  }
 private:
  // information about a lock
  struct LockState {
    // whether the holder have changed to a new holder in last execution
    bool new_holder; 
    // the current holder of lock
    LockMessage holder;
    // the wait queue of the lock
    std::queue<LockMessage> wait_queue;
    LockState(void) {
      holder.type = LockMessage::kNull;
      new_holder = false;
    }
    // return whether this lock is hold by nobody
    inline bool no_holder(void) const {
      return holder.type == LockMessage::kNull;
    }
    // update the state by executing cmd
    inline bool Exec(const LockMessage &cmd) {
      new_holder = false;
      if (cmd.type == LockMessage::kLockRequest) {
        if (this->no_holder()) {
          holder = cmd; new_holder = true;
        } else {
          wait_queue.push(cmd);
        }
      } else {
        utils::Assert(cmd.type == LockMessage::kUnlockRequest, "invalid lock command");
        // different ppl cannot unlock this lock, this is ignored
        if (cmd == holder) {
          if (wait_queue.size() != 0) {
            holder = wait_queue.front();
            wait_queue.pop(); new_holder = true;
          } else {
            holder.type = LockMessage::kNull;
          }
        } else {
          return false;
        }
      }
      return true;
    }
  };
  // send notification to holder that the lock has been granted
  inline void SendHolderNotification(LockMessage msg) {
    out_msg.Clear();
    out_msg.WriteT(node_id);
    out_msg.WriteT(LockMessage::kServerLockGranted);
    out_msg.WriteT(msg);
    post->SendTo(msg.node_client, out_msg);    
  }
  // send server ack to the client to acknowledge a message has been chosen
  inline void SendServerAck(LockMessage msg) {
    out_msg.Clear();
    out_msg.WriteT(node_id);
    out_msg.WriteT(LockMessage::kServerAck);
    out_msg.WriteT(msg);
    post->SendTo(msg.node_client, out_msg);
  }
  // update latest chosen command from each node
  inline bool CheckChosen(const LockMessage &msg) {
    // update the latest command
    while (latest_scan < server_rec.size()) {
      ProposeState &s = server_rec[latest_scan];
      if (s.type != ProposeState::kChosen) break;
      LockMessage &c = latest_chosen[s.value.node_client];
      utils::Assert(c.type == LockMessage::kNull ||
                    c.counter < s.value.counter,
                    "client must send command in incremental counter");
      c = s.value;
      ++latest_scan;
    }
    LockMessage &c = latest_chosen[msg.node_client];
    if (c.type == LockMessage::kNull ||
        c.counter < msg.counter) {
      return false;
    } else {
      utils::Assert(c == msg, "invalid client message");
      return true;
    }
  }
  // last executed command
  unsigned cmd_ptr;
  // lock state of each lock
  std::vector<LockState> lock_state;
  // the scan counter used to update the lastest information
  size_t latest_scan;
  // latest known value in queue from each node
  std::vector<LockMessage> latest_chosen;
  // propose queue, store value that have not yet been proposed
  std::deque<LockMessage> queue;
  // the list of granted holder of the lock that not yet ack back to the server
  std::list< std::pair<unsigned, LockMessage> > holder_notify;
};
} // namespace
#endif
