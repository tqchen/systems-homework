#ifndef PAXOS_INL_H_
#define PAXOS_INL_H_

#include "./utils/utils.h"
#include "./utils/message.h"
#include "./post_office.h"

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
  }
  /*! \brief type of message that can be send to the paxos */
  enum MessageType {
    kTerminate,
    // the request message to prepare
    kPrepareRequest,
    // the request message to prepare
    kPrepareSuccess,
    kPrepareFailure,
    kAcceptRequest,
    kAcceptReturn
  };

  // start running multi
  inline void Run(void) {
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
        default: utils::Error("unknown message type");
      }
    }
  }
 private:
  // handle time out event
  inline void HandleTimeOut(void) {
  }
  /*! \brief post office that handles message passing*/
  IPostOffice *post;
  /*! \brief node id of current node */
  int node_id;
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
  ///!!! Accepter Event Handling!!!!
  /*! 
   * \brief accept record, contains a indicator that it is null(no record so far)
   *        or is_null == false and pid is the most recent proposal ID
   */
  struct AcceptRecordID {
    /*! \brief whetehr */
    bool is_null;
    /*! \brief the proposal number */
    ProposalID pid;
    // default constructor
    AcceptRecordID(void) : is_null(true) {}
    // set the record to correct proposal ID
    inline void SetProposalID(ProposalID pid) {
      this->is_null = false; this->pid = pid;
    }
  };
  /*! \brief the promise of the accepter, not to accept things before */
  AcceptRecordID promise;
  /*! \brief record of accepted proposal in each of instance */
  std::vector<AcceptRecordID> accepted_id;
  /*! \brief record of accepted value in each instance */  
  std::vector<TValue> accepted_value;
  // temporal out message
  utils::Message out_msg;
  /*!
   * \brief prepare instance from start_inst, if succeed, return 
   */
  inline void HandlePrepareReq(utils::IStream &in, utils::IStream &out) {
    // the proposal to be handled
    ProposalID pid;
    // starting instance the sender is interested in
    unsigned start_inst;
    utils::Check(in.Read(&pid, sizeof(pid)) != 0, "invalid message");
    utils::Check(in.Read(&start_inst, sizeof(start_inst)) != 0, "invalid message");    
    if (promise.is_null || promise.pid <= pid) {
      promise.SetProposalID(pid);
      // success
      out.WriteT(kPrepareSuccess);      
      // return the accepted instance which are 
      std::vector< std::pair<int, TValue> > rec;
      for (unsigned i = start_inst; i < accepted_id.size(); ++i) {
        if (!accepted_id[i].is_null) {
          utils::Assert(accepted_id[i].pid <= pid, "BUG");
          rec.push_back(std::make_pair(i, accepted_value[i]));
        }
      }
      // write all the instance id, value pairs
      out.Write(rec);
    } else {
      // failure, return the promised id 
      // so that the node know who is the leader the accepter thinks      
      out.WriteT(kPrepareFailure);
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
    utils::Check(!promise.is_null, "must have a promised value before accept");
    // send the message back
    out.WriteT(kAcceptReturn);
    // there is the handler
    if (promise.pid == pid) {
      // accept the instance if proposal id matchs promise
      if (inst_index <= accepted_id.size()) { 
        accepted_id.resize(inst_index + 1);
        accepted_value.resize(inst_index+1);
      }
      if (!accepted_id[inst_index].is_null) {
        utils::Assert(accepted_id[inst_index].pid == pid, "can only accept same proposal");
      } 
      accepted_id[inst_index].SetProposalID(pid);
      accepted_value[inst_index] = value;
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

