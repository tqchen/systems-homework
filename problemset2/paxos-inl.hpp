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
      if (counter < b.counter) return true;
      if (counter == b.counter) {
        return node_id <= b.node_id;
      } else {
        return false;
      }
    }
  };
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
    kChosenNotify = 7,
    // client request, trap into client request handler
    kClientRequest = 8,
    // this message is used for testing, encourage the node to become leader
    kBecomeLeader = 9
  };
  // get message type name
  inline static const char *GetName(MessageType t) {
    switch (t) {
      case kTerminate: return "kTerminate";
      case kLeaderAck: return "kLeaderAck";
      case kTimeout: return "kTimeout";
      case kPrepareRequest: return "kPrepareRequest";
      case kPrepareReturn: return "kPrepareReturn";
      case kAcceptRequest: return "kAcceptRequest";
      case kAcceptReturn: return "kAcceptReturn";
      case kChosenNotify: return "kChosenNotify";
      case kClientRequest: return "kClientRequest";
      case kBecomeLeader: return "kBecomeLeader";
    }
    return "";
  }    
  MultiPaxos(IPostOffice *post, int num_server) : post(post) {
    this->node_id = post->GetRank();
    this->num_server = num_server;
    utils::Check(num_server <=  post->WorldSize(),
                 "number of server exceed limit");
    timeout_counter = 0;
    timeout_limit = 100;
    majority_size = num_server / 2 + 1;
    leader.node_id = 0; 
    leader.counter = 0;
    current_instance = 0;
    this->shutdown = true;
    this->startserver = false;
    shutdown_finish.Init(0);
  }
  virtual ~MultiPaxos(void) {
    this->Shutdown();
    if (startserver) shutdown_finish.Wait();
    shutdown_finish.Destroy();
  }
  inline void Shutdown(void){
    if (shutdown) return;
    this->shutdown = true;
    out_msg.Clear();
    out_msg.WriteT(node_id);
    out_msg.WriteT(kTerminate);
    post->SendTo(node_id, out_msg);
  }
  // running a paxos server, doing MultiPaxos
  inline void RunServer(void) {
    this->startserver = true;
    this->shutdown = false;
    // every start wantin to be a leader 
    this->ChangeServerState(kLeaderPrepare);
    utils::Message msg;
    while (!this->shutdown) {
      bool ret = post->RecvFrom(&msg);
      
      if (this->shutdown) break;
      if (!ret) {
        this->HandleTimeOut(); continue;
      }
      msg.Seek(0);
      unsigned sender;
      MessageType type;
      // first two are always sender and type
      utils::Check(msg.Read(&sender, sizeof(sender)) != 0, "invalid message");
      utils::Check(msg.Read(&type, sizeof(type)) != 0, "invalid message");
      if (type != kLeaderAck) {
        //utils::LogPrintf("[%u] recv %s from %u\n", node_id, GetName(type), sender);
      }
      if (type == kTimeout) {
        this->HandleTimeOut(); continue;
      }
      // when message means terminate
      if (type == kTerminate) {
        server_state = kTerminated; break;
      }
      // always clear and stamp current node id in out_msg
      out_msg.Clear(); out_msg.WriteT(node_id);
      switch(type) {
        case kTerminate: break;
        case kAcceptRequest: {
          this->HandleAcceptReq(msg, out_msg); 
          post->SendTo(sender, out_msg);
          break;
        }
        case kPrepareRequest: {
          this->HandlePrepareReq(msg, out_msg); 
          post->SendTo(sender, out_msg);
          break;
        }
        case kClientRequest: this->HandleClientRequest(msg, sender); break;
        case kPrepareReturn: this->HandlePrepareReturn(msg, sender); break; 
        case kLeaderAck: this->HandleLeaderAck(msg, sender); break;
        case kAcceptReturn: this->HandleAcceptReturn(msg, sender); break; 
        case kChosenNotify: this->HandleChosenNotify(msg, sender); break;
        case kBecomeLeader: {
          unsigned inc;
          utils::Check(msg.Read(&inc, sizeof(inc))!=0, "finish read inc");
          leader.counter += inc;
          utils::LogPrintf("[%d] raise counter by %u\n", node_id, inc);
          this->ChangeServerState(kLeaderPrepare);
          break;
        }
        case kTimeout: utils::Error("unexpected message timeout");
      }
    }
    utils::LogPrintf("[%u] server shutdown\n", node_id);
    shutdown_finish.Post();
  }
 protected:
  // the current state of server
  enum ServerState {
    kSlave,
    kLeaderIdle,
    kLeaderPrepare,
    kLeaderAccept,
    kTerminated
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
  //--- gobal structure ---
  // signal to shutdown server
  bool shutdown, startserver;
  // shutdown finish signal
  utils::Semaphore shutdown_finish;
  /*! \brief post office that handles message passing*/
  IPostOffice *post;
  // temporal out message
  utils::Message out_msg;
  /*! \brief the state of the server */
  ServerState server_state;
  /*! \brief the propose id used by the lastest leader it known */
  ProposalID leader;
  /*! \brief server record of set of states to be proposed */
  std::vector<ProposeState> server_rec;
  /*! \brief current instance being proposed */
  unsigned current_instance;
  /*! \brief node id of current node */
  unsigned node_id;
  // ---- following two functions are left to be implemented by subclass----
  /*! \brief to be implemented by the subclass, handle client specific request */
  virtual void HandleClientRequest(utils::IStream &in, unsigned sender) = 0;
  /*! 
   * \brief get next new value
   * \param p_value address to put the new value into
   * \return true if there is new value, false if no new value exist
   */
  virtual bool GetNewValue(TValue *p_value) = 0;
  /*! \brief to be implemented by the subclass, handle the event that inst_index is being chosen */
  virtual void HandleChosenEvent(unsigned inst_index) = 0;
  // handle time out event
  virtual void HandleTimeOut(void) {
    timeout_counter += 1;
    // handle timeout
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
      if (server_state != kLeaderPrepare) {
        this->ChangeServerState(kLeaderPrepare);
      }
      return;
    }    
    // normal timeout, try to re-transmit unfinished request
    switch (server_state) {
      case kLeaderPrepare: this->SendPrepareReq(); return;
      case kLeaderAccept: this->SendAcceptReq(); return;
      case kLeaderIdle: return;
      case kSlave: return; // do nothing
      case kTerminated: utils::Error("invalid server state");
    }   
  }
 private:
  // the record to be returned to the proposer
  struct AcceptRecord {
    // proposal id
    ProposalID pid;
    // instance index
    unsigned inst_index;
    // the proposed value
    TValue value;                 
  };
  ////////////////////////////////
  // ---- Server data structure -----
  /*! \brief total number of servers in paxos group */
  int num_server;
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
  /////////////////////////////////
  //---- Accepter data structure ---
  /*! \brief the promise of the accepter, not to accept things before */
  ProposeState promise;
  /*! \brief record of accepted proposal in each of instance */
  std::vector<ProposeState> accepted_rec;
  /////////////////////
  //---- Server Logic------
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
      if (server_state == kLeaderPrepare) { 
        // set rest server rec that are not chosen to be unknown
        // this is important, since server_rec records the 
        // largest (pid, value) of accepted response if the state is not Chosen
        // previous trace need to be cleanedup, so the accept response max only contains current run
        for (unsigned i = current_instance; i < server_rec.size(); ++i) {
          if (server_rec[i].type != ProposeState::kChosen) {
            server_rec[i].type = ProposeState::kNull;
          }
        }
      }
    }
    if (server_state == kSlave) {
      utils::LogPrintf("[%d] Change to Slave\n", node_id);
    }
    // change to leader state, need to prepare the necessary data structures
    if (server_state == kLeaderPrepare) {
      promise_counter = 0;
      promise_replied.resize(num_server);
      std::fill(promise_replied.begin(), promise_replied.end(), false);
      leader.node_id = this->node_id;
      leader.counter += 1;
      utils::LogPrintf("[%d] LeaderPrepare\n", node_id); 
      this->SendPrepareReq();      
      return;
    }
    if (server_state == kLeaderAccept) {
      // we are running out of history sequence
      if (current_instance == server_rec.size()) {
        TValue value;
        if (this->GetNewValue(&value)) {
          // get new value to be proposed
          // push the command in the queue to the proposal
          ProposeState s;
          s.pid = leader;
          s.type = ProposeState::kAccepted;
          s.value = value;
          server_rec.push_back(s);
          //utils::LogPrintf("[%d] LeaderAccept\n", node_id);
        } else {
          // switch to idle state, no new value so far
          server_state = kLeaderIdle; return;
        }
      }
      accept_counter = 0;
      accept_replied.resize(num_server);
      std::fill(accept_replied.begin(), accept_replied.end(), false);
      ProposeState &s = server_rec[current_instance];
      s.pid = leader;
      utils::Assert(!s.is_null(), "this code does not allow lag command yet");
      // in future, send no-op when there is lag
      this->SendAcceptReq();
      return;
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
    for (int i = 0; i < num_server; ++i) {
      if (i != node_id) {
        post->SendTo(i, out_msg);
      }
    }
  }
  // send ack to each node to tell them leader is still alive
  inline void SendLeaderAck(void) {
    utils::Assert(server_state == kLeaderIdle, "wrong state to send prepare");
    utils::Assert(leader.node_id == this->node_id, "leader node id inconsistent");
    out_msg.Clear();
    out_msg.WriteT(node_id);
    out_msg.WriteT(kLeaderAck);
    out_msg.WriteT(leader);
    for (int i = 0; i < num_server; ++i) {
      post->SendTo(i, out_msg);
    }
  }
  // send prepare request to every node that has not replied yet
  inline void SendPrepareReq(void) {
    utils::Assert(server_state == kLeaderPrepare, "wrong state to send prepare");
    utils::Assert(leader.node_id == this->node_id, "leader node id inconsistent");
    out_msg.Clear();
    out_msg.WriteT(node_id);
    out_msg.WriteT(kPrepareRequest);
    out_msg.WriteT(leader);
    
    out_msg.WriteT(current_instance);
    for (int i = 0; i < num_server; ++i) {
      if (promise_replied[i]) continue;
      post->SendTo(i, out_msg);
    }
  }
  // send prepare request to every node that has not replied yet
  inline void SendAcceptReq(void) {
    utils::Assert(server_state == kLeaderAccept, "wrong state to send prepare");
    utils::Assert(leader.node_id == this->node_id, "leader node id inconsistent");
    utils::Assert(server_rec[current_instance].pid == leader,
                  "send accepted req bug");
    out_msg.Clear();
    out_msg.WriteT(node_id);
    out_msg.WriteT(kAcceptRequest);      
    out_msg.WriteT(leader);
    out_msg.WriteT(server_rec[current_instance].value);
    out_msg.WriteT(current_instance);
    for (int i = 0; i < num_server; ++i) {
      if (!promise_replied[i]) continue;
      if (accept_replied[i]) continue;
      post->SendTo(i, out_msg);
    }
  }
  // handling the leader ack message
  inline void HandleLeaderAck(utils::IStream &in, unsigned sender) {
    ProposalID pid;
    utils::Check(in.Read(&pid, sizeof(pid)) != 0, "HandleLeaderAck: invalid message");
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
  inline void HandleChosenNotify(utils::IStream &in, unsigned sender) {
    if (server_state != kSlave) return;
    // every broadcast message of chosen must be correct
    AcceptRecord r;
    utils::Check(in.Read(&r, sizeof(r)) != 0, "HandleChosenNotify: invalid message");
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
  inline void HandlePrepareReturn(utils::IStream &in, unsigned sender) {
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
        // if this instance is already chosen, no need to mark it, simply skip it
        if (s.type == ProposeState::kChosen) continue;
        // otherwise mark it
        if (s.is_null() || s.pid < rec[i].pid) {
          s.type = ProposeState::kAccepted;
          s.pid = rec[i].pid;
          s.value = rec[i].value;
          utils::Assert(rec[i].pid <= leader, "break protocol");
        }
      }
      promise_counter += 1;
      if (promise_counter >= majority_size) {
        utils::LogPrintf("[%u] Leader change to Accept mode\n", node_id);
        this->ChangeServerState(kLeaderAccept);
      }
    } else {
      if (leader < pid) {
        leader = pid;
        // new leader must be another machine, switch to slave mode
        utils::Assert(leader.node_id != this->node_id, "new leader bug");
        this->ChangeServerState(kSlave);
      } else {
        // out-dated message, ignore it
        return;
      }
    }
  }
  // handling the return value of prepare
  inline void HandleAcceptReturn(utils::IStream &in, unsigned sender) {
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
        this->HandleChosenEvent(current_instance);
        this->SendChosenNotify(current_instance);
        this->ChangeServerState(kLeaderAccept);
      }
    } else {
      if (leader < pid) {
        leader = pid;
        // new leader must be another machine, switch to slave mode
        utils::Assert(leader.node_id != this->node_id, "new leader bug");
        this->ChangeServerState(kSlave);
      } else {
        // out-dated message, ignore it
        return;
      }
    }
  }
  ///////////////////////////
  // --- Accepter Logic-----
  //////////////////////////
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
      //printf("[%u] promise (%u, %u)\n", node_id, pid.node_id, pid.counter);
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
      //printf("[%u] rej (%u, %u)\n", node_id, pid.node_id, pid.counter);
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
  inline void HandleAcceptReq(utils::IStream &in, utils::IStream &out) {
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
