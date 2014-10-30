#ifndef PAXOS_INL_H_
#define PAXOS_INL_H_

#include <queue>
#include "./utils/utils.h"
#include "./utils/message.h"
#include "./post_office.h"
#include "./utils/thread.h"


namespace consencus {
/*! 
 * \brief multi instance paxos 
 *  this is a template class, so that we can handle various value type 
 *  by specifyig the TValue
 * \tparam TValue, type of value used in the protocol
 */
template<typename TValue>
class MultiPaxos {
 public:
  MultiPaxos(IPostOffice *post) : post(post) {
    this->node_id = post->GetRank();
    timeout_counter = 0;
    timeout_limit = 20;
    majority_size = post->WorldSize() / 2 + 1;
    leader.node_id = 0; leader.counter = 0;
    current_instance = 0;
    queue_lock.Init();
    post_lock.Init();
  }
  virtual ~MultiPaxos(void) {
    queue_lock.Destroy();
    post_lock.Destroy();
  }
  // start running
  inline void Run(void) {
    // every start wantin to be a leader 
    this->ChangeServerState(kLeaderPrepare);
    utils::Message msg;
    while (true) {
      bool ret = post->RecvFrom(&msg);
      if (!ret) {
        this->HandleTimeOut(); continue;
      }
      msg.Seek(0);
      int sender;
      MessageType type;
      // first two are always sender and type
      utils::Check(msg.Read(&sender, sizeof(sender)) != 0, "invalid message");
      utils::Check(msg.Read(&type, sizeof(type)) != 0, "invalid message");
      if (type == kTimeout) {
        this->HandleTimeOut(); continue;
      }
      // when message means terminate
      if (type == kTerminate) break;
      // always clear and stamp current node id in out_msg
      out_msg.Clear(); out_msg.WriteT(node_id);
      switch(type) {
        case kTerminate: break;
        case kAcceptRequest: {
          this->HandleAcceptReq(msg, out_msg); 
          this->SendTo(sender, out_msg);
          break;
        }
        case kPrepareRequest: {
          this->HandlePrepareReq(msg, out_msg); 
          this->SendTo(sender, out_msg);
          break;
        }
        case kPrepareReturn: this->HandlePrepareReturn(msg, sender); break; 
        case kAcceptReturn: this->HandleAcceptReturn(msg, sender); break; 
        case kChosenNotify: this->HandleChosenNotify(msg, sender); break;
        default: utils::Error("unknown message type");
      }
    }
  }
 protected:
  // the current state of server
  enum ServerState {
    kSlave,
    kLeaderIdle,
    kLeaderPrepare,
    kLeaderAccept
  };
  /*! 
   * \brief proposal ID, consists of counter and node id of proposer   
   */
  struct ProposalID {
    /*! \brief counter */
    unsigned counter;
    /*! \brief node id */
    unsigned node_id;    
    // comparator proposal 
    inline bool operator<(const ProposalID &b) const {
      if (counter < b.counter) return true;
      return node_id < b.node_id;
    }
    inline bool operator==(const ProposalID &b) const {
      return counter == b.counter && node_id == b.node_id;
    }
    inline bool operator<=(const ProposalID &b) const {
      if (counter <= b.counter) return true;
      return node_id <= b.node_id;
    }
  };
  /*! 
   * \brief accept record, contains a indicator that it is null(no record so far)
   *        or is_null == false and pid is the most recent proposal ID
   */
  struct ProposeState {
    enum State {
      // this no proposal set yet
      kNull,
      // the proposal is accepted
      kAccepted,
      // the proposal is chosen and learned
      kChosen
    };
    /*! \brief the proposal number */
    ProposalID pid;
    /*! \brief type of the state */
    State type;
    /*! \brief value of the proposal, if any */
    TValue value;
    // default constructor
    ProposeState(void) : type(kNull) {}
    inline bool is_null(void) {
      return type == kNull;
    }
  };
  /*! \brief the state of the server */
  ServerState server_state;
  /*! \brief the propose id used by the lastest leader it known */
  ProposalID leader;
  /*! \brief server record of set of states to be proposed */
  std::vector<ProposeState> server_rec;
  /*! \brief current instance being proposed */
  unsigned current_instance;
  /*! \brief node id of current node */
  int node_id;
  
  // notify that the queue is empty
  virtual void OnQueueEmpty(void) {}
  virtual void SendTo(int nid, const utils::Message &msg) {
    post_lock.Lock();
    post->SendTo(nid, msg);
    post_lock.Unlock();
  }
  virtual void AddCommand(TValue value) {
    queue_lock.Lock();
    queue.push(value);
    queue_lock.Unlock();
  }
 private:
  /*! \brief type of message that can be send to the paxos */
  enum MessageType {
    kTerminate = 0,
    // simple acknowledge that leader is alive
    kLeaderAck = 1,
    // timeout message, can be used to simulate timeout
    kTimeout = 2,
    // the request message to prepare
    kPrepareRequest = 3,
    // the request message to prepare
    kPrepareReturn = 4,
    // request for accept
    kAcceptRequest = 5,
    // return message from accept
    kAcceptReturn = 6,
    // notify that some value is chosen
    kChosenNotify = 7
  };
  // the record to be returned to the proposer
  struct AcceptRecord {
    // proposal id
    ProposalID pid;
    // instance index
    unsigned inst_index;
    // the proposed value
    TValue value;                 
  };
  // ---- Server data structure -----
  /*! \brief size of majority set */
  int majority_size;
  // timeout counter, used to record timeout since last state change
  int timeout_counter, timeout_limit;
  /*! \brief number of promise we received so far */
  int promise_counter;  
  /*! \brief leader state, whether promise is replied */
  std::vector<bool> promise_replied;
  /*! \brief number of accept we received so far */
  int accept_counter;
  /*! \brief leader state, whether accept is replied */
  std::vector<bool> accept_replied;
  //---- Accepter data structure ---
  /*! \brief the promise of the accepter, not to accept things before */
  ProposeState promise;
  /*! \brief record of accepted proposal in each of instance */
  std::vector<ProposeState> accepted_rec;
  //--- gobal structure ---
  /*! \brief post office that handles message passing*/
  IPostOffice *post;
  // temporal out message
  utils::Message out_msg;
  // propose queue, store value that have not yet been proposed
  std::queue<TValue> queue;
  // command lock 
  utils::Mutex queue_lock, post_lock;
  
  // change server state to state
  inline void ChangeServerState(ServerState state) {
    server_state = state;
    timeout_counter = 0;
    if (server_state == kLeaderPrepare || server_state == kLeaderAccept) {
      // advance current instance to latest not decided value
      while (current_instance < server_rec.size() &&
             server_rec[current_instance].type == ProposeState::kChosen) {
        current_instance += 1;
      }      
    }    
    // change to leader state, need to prepare the necessary data structures
    if (server_state == kLeaderPrepare) {
      promise_counter = 0;
      promise_replied.resize(post->WorldSize());
      std::fill(promise_replied.begin(), promise_replied.end(), false);
      leader.node_id = this->node_id;
      leader.counter += 1;      
      this->SendPrepareReq();      
      return;
    }
    if (server_state == kLeaderAccept) {
      // we are running out of history sequence
      if (current_instance == server_rec.size()) {
        if (queue.size() == 0) {
          // switch to idle state, no new command
          this->server_state = kLeaderIdle;
          // notify observer
          this->OnQueueEmpty();
          return;
        } else {
          // push the command in the queue to the proposal
          ProposeState s;
          s.pid = leader;
          s.type = ProposeState::kAccepted;
          queue_lock.Lock();
          s.value = queue.front();
          queue.pop();
          queue_lock.Unlock();
          server_rec.push_back(s);
        }
      }
      accept_counter = 0;
      accept_replied.resize(post->WorldSize());
      std::fill(accept_replied.begin(), accept_replied.end(), false);
      ProposeState &s = server_rec[current_instance];
      s.pid = leader;
      utils::Assert(!s.is_null(), "this code does not allow lag command yet");
      // in future, send no-op when there is lag
      this->SendAcceptReq();
      return;
    }
  }
  // handle time out event
  inline void HandleTimeOut(void) {
    timeout_counter += 1;
    if (server_state == kLeaderIdle) {
      // try to switch to accept state, if possible
      this->ChangeServerState(kLeaderAccept);
      // still in idle, send ack to all server to tell them leader is alive
      if (server_state == kLeaderIdle) {
        this->SendLeaderAck();
      }
    }
    if (timeout_counter > timeout_limit) {
      // this is abnormal timeout, maybe some server is down
      // try to switch to new leader state
      this->ChangeServerState(kLeaderPrepare);
      return;
    }
    // normal timeout, try to re-transmit unfinished request
    switch (server_state) {
      case kLeaderPrepare: this->SendPrepareReq(); return;
      case kLeaderAccept: this->SendAcceptReq(); return;
      case kLeaderIdle:
      case kSlave: return; // do nothing
    }   
  }
  // send message that current instance is chosen
  inline void SendChosenNotify(unsigned inst_index) {
    AcceptRecord r;
    r.pid = leader; r.inst_index = inst_index;
    r.value = server_rec[inst_index].value;
    out_msg.Clear();
    out_msg.WriteT(node_id);
    out_msg.WriteT(kChosenNotify);
    out_msg.WriteT(r);
  }
  // send ack to each node to tell them leader is still alive
  inline void SendLeaderAck(void) {
    utils::Assert(server_state == kLeaderIdle, "wrong state to send prepare");
    utils::Assert(leader.node_id == this->node_id, "leader node id inconsistent");
    int ninst = post->WorldSize();
    out_msg.Clear();
    out_msg.WriteT(node_id);
    out_msg.WriteT(kLeaderAck);
    out_msg.WriteT(leader);
    for (int i = 0; i < ninst; ++i) {
      this->SendTo(i, out_msg);
    }
  }
  // send prepare request to every node that has not replied yet
  inline void SendPrepareReq(void) {
    utils::Assert(server_state == kLeaderPrepare, "wrong state to send prepare");
    utils::Assert(leader.node_id == this->node_id, "leader node id inconsistent");
    int ninst = post->WorldSize();
    out_msg.Clear();
    out_msg.WriteT(node_id);
    out_msg.WriteT(kPrepareRequest);
    out_msg.WriteT(leader);
    out_msg.WriteT(current_instance);
    for (int i = 0; i < ninst; ++i) {
      if (promise_replied[i]) continue;
      this->SendTo(i, out_msg);
    }
  }
  // send prepare request to every node that has not replied yet
  inline void SendAcceptReq(void) {
    utils::Assert(server_state == kLeaderAccept, "wrong state to send prepare");
    utils::Assert(leader.node_id == this->node_id, "leader node id inconsistent");
    int ninst = post->WorldSize();
    utils::Assert(server_rec[current_instance].pid == leader,
                  "send accepted req bug");
    out_msg.Clear();
    out_msg.WriteT(node_id);
    out_msg.WriteT(kAcceptRequest);      
    out_msg.WriteT(leader);
    out_msg.WriteT(server_rec[current_instance].value);
    out_msg.WriteT(current_instance);
    for (int i = 0; i < ninst; ++i) {
      if (!promise_replied[i]) continue;
      if (accept_replied[i]) continue;
      this->SendTo(i, out_msg);
    }
  }
  // handling the leader ack message
  inline void HandleLeaderAck(utils::IStream &in, int sender) {
    ProposalID pid;
    utils::Check(in.Read(&pid, sizeof(pid)) != 0, "invalid message");
    // simply ignore this one, this is invalid leader
    if (pid < leader)  return;
    // cleanup timeout counter
    timeout_counter = 0;
    // if we get a ack with larger pid
    // this means we must change to slave state, if we are in LeaderState
    if (leader < pid) {
      leader = pid; this->ChangeServerState(kSlave);
    }
  }
  // handling the return value of prepare
  inline void HandleChosenNotify(utils::IStream &in, int sender) {
    if (server_state != kSlave) return;
    // every broadcast message of chosen must be correct
    AcceptRecord r;    
    utils::Check(in.Read(&r, sizeof(r)) != 0, "invalid message");
    if (server_rec.size() <= r.inst_index) {
      server_rec.resize(r.inst_index + 1);
    }
    server_rec[r.inst_index].type = ProposeState::kChosen;
    server_rec[r.inst_index].pid = r.pid;
    server_rec[r.inst_index].value = r.value;
    // update leader record, if needed
    if (leader <= r.pid) {
      leader = r.pid;
      // valid message, reset timeout counter
      timeout_counter = 0;
    }
  }
  // handling the return value of prepare
  inline void HandlePrepareReturn(utils::IStream &in, int sender) {
    // valid message, reset timeout counter
    timeout_counter = 0;
    // we already switch to slave state, ignore the message
    if (server_state != kLeaderPrepare) return;
    ProposalID pid;
    utils::Check(in.Read(&pid, sizeof(pid)) != 0, "invalid message");
    // message matchs current proposal
    if (pid == leader) {
      utils::Assert(leader.node_id == this->node_id, "leader state bug");
      // if already get reply, this is duplicated message, ignore it
      if (promise_replied[sender]) return;
      // update the states
      promise_replied[sender] = true;
      // get list of already accepted record
      std::vector<AcceptRecord> rec;
      in.Read(&rec);
      for (size_t i = 0; i < rec.size(); ++i) {
        if (server_rec.size() <= rec[i].inst_index) {
          server_rec.resize(rec[i].inst_index + 1);
        }
        ProposeState &s = server_rec[rec[i].inst_index];
        utils::Assert(s.type != ProposeState::kChosen,
                      "accepted state already being chosen");        
        if (s.is_null() || s.pid < rec[i].pid) {
          s.type = ProposeState::kAccepted;
          s.pid = rec[i].pid;
          s.value = rec[i].value;
          utils::Assert(rec[i].pid <= leader, "break protocol");
        }
      }
      promise_counter += 1;
      if (promise_counter >= majority_size) {
        this->ChangeServerState(kLeaderAccept);
      }
    } else {
      if (leader < pid) {
        // new leader must be another machine, switch to slave mode
        utils::Assert(leader.node_id != this->node_id, "new leader bug");
        leader = pid;
        this->ChangeServerState(kSlave);
      } else {
        // out-dated message, ignore it
        return;
      }
    }
  }
  // handling the return value of prepare
  inline void HandleAcceptReturn(utils::IStream &in, int sender) {
    // valid message, reset timeout counter
    timeout_counter = 0;
    // we already switch to different state, ignore the message
    if (server_state != kLeaderAccept) return;
    ProposalID pid;
    utils::Check(in.Read(&pid, sizeof(pid)) != 0, "invalid message");
    if (pid == leader) {
      utils::Assert(leader.node_id == this->node_id, "leader state bug");
      // if already get reply, this is duplicated message, ignore it
      if (accept_replied[sender]) return;
      // update the states
      accept_replied[sender] = true;
      // the request is accepted!
      accept_counter += 1;
      if (accept_counter >= majority_size) {
        // mark current instance as chosen!!
        server_rec[current_instance].type = ProposeState::kChosen;        
        this->ChangeServerState(kLeaderAccept);
      }
    } else {
      if (leader < pid) {
        // new leader must be another machine, switch to slave mode
        utils::Assert(leader.node_id != this->node_id, "new leader bug");
        leader = pid;
        this->ChangeServerState(kSlave);
      } else {
        // out-dated message, ignore it
        return;
      }
    }
  }
  // accepter: handle prepare
  inline void HandlePrepareReq(utils::IStream &in, utils::IStream &out) {
    // the proposal to be handled
    ProposalID pid;
    // starting instance the sender is interested in
    unsigned start_inst;
    utils::Check(in.Read(&pid, sizeof(pid)) != 0, "invalid message");
    utils::Check(in.Read(&start_inst, sizeof(start_inst)) != 0, "invalid message");    
    // success
    out.WriteT(kPrepareReturn);
    if (promise.is_null() || promise.pid <= pid) {
      promise.pid = pid;
      promise.type = ProposeState::kAccepted;
      // attach the promise id to the data anyway
      out.WriteT(promise.pid);
      // return the accepted instance which are 
      std::vector<AcceptRecord> rec;
      for (unsigned i = start_inst; i < accepted_rec.size(); ++i) {
        if (!accepted_rec[i].is_null()) {
          utils::Assert(accepted_rec[i].pid <= pid, "BUG");
          AcceptRecord r;
          r.pid = accepted_rec[i].pid;
          r.value = accepted_rec[i].value;
          r.inst_index = i;
          rec.push_back(r);
        }
      }
      // write all the instance id, value pairs
      out.Write(rec);
    } else {
      // failure, return the promised id 
      // so that the node know who is the leader the accepter thinks      
      out.WriteT(promise.pid);
    }
  }
  /*!
   * \brief handles accept request 
   * \return a proposal ID, if it matchs pid, it means the proposal is acceped
   *  otherwise, the proposal is rejected, and the accepter thinks current leader is pid
   */
  inline void HandleAcceptReq(utils::ISeekStream &in, utils::ISeekStream &out) {
    // the proposal to be handled
    ProposalID pid; 
    // the proposed value
    TValue value;
    // the proposed instance index
    unsigned inst_index;
    utils::Check(in.Read(&pid, sizeof(pid)) != 0, "invalid message");
    utils::Check(in.Read(&value, sizeof(value)) != 0, "invalid message");    
    utils::Check(in.Read(&inst_index, sizeof(inst_index)) != 0, "invalid message");    
    utils::Check(!promise.is_null(), "must have a promised value before accept");
    // send the message back
    out.WriteT(kAcceptReturn);
    // there is the handler
    if (promise.pid == pid) {
      // accept the instance if proposal id matchs promise
      if (accepted_rec.size() <= inst_index) { 
        accepted_rec.resize(inst_index + 1);
      }
      if (!accepted_rec[inst_index].is_null()) {
        utils::Assert(accepted_rec[inst_index].pid == pid, "can only accept same proposal");
      }
      accepted_rec[inst_index].type = ProposeState::kAccepted;
      accepted_rec[inst_index].pid = pid;
      accepted_rec[inst_index].value = value;
      // return pid
      out.WriteT(pid);
    } else {
      utils::Check(pid < promise.pid, "promise have not occurred before");
      // return pid
      out.WriteT(promise.pid);
    }
  }
};
}  // namespace consencus
#endif
