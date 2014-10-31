#ifndef LOCK_CLIENT_INL_HPP_
#define LOCK_CLIENT_INL_HPP_

#include "./utils/utils.h"
#include "./utils/message.h"
#include "./post_office.h"
#include "./lock_server-inl.hpp"

namespace consencus {
class LockClient {
 public:
  LockClient(IPostOffice *post, unsigned num_server) 
      : post(post), num_server(num_server) {
    req.type = LockMessage::kNull;
    req.node_client = post->GetRank();
    req.counter = 0;
  }
  // lock a certain lock index
  inline void Lock(unsigned lock_index) {
    // states of a lock
    const int kLockRequestSent = 0;
    const int kLockRequestAcked = 1;
    const int kLockGranted = 2;
    const int kLockGrantedAcked = 3;
    req.type = LockMessage::kLockRequest;
    req.lock_index = lock_index;
    ++req.counter;
    this->SendMessage(req);    
    int state = kLockRequestSent;
    while (state != kLockGrantedAcked) {
      bool ret = post->RecvFrom(&in_msg);
      if (!ret) {
        // time out and we are in state of waiting ack
        if (state != kLockRequestAcked) this->SendMessage(req);
        continue;
      }
      unsigned sender;
      LockMessage::Type type;
      LockMessage lock_msg;
      in_msg.Seek(0);
      // first two are always sender and type
      utils::Check(in_msg.Read(&sender, sizeof(sender)) != 0, "Lock-1: invalid message");
      utils::Check(in_msg.Read(&type, sizeof(type)) != 0, "Lock-2: invalid message");
      utils::Check(in_msg.Read(&lock_msg, sizeof(lock_msg)) != 0, "Lock-3:invalid message");
      // process different type of data
      switch (type) {
        case LockMessage::kServerAck: {
          // only if ack is to previous request, proceed to next step
          if (lock_msg == req) state |= 1;
          break;
        }
        case LockMessage::kServerLockGranted: {
          // outdated message
          if (lock_msg.counter < req.counter) break;
          if (state < kLockGranted) {
            utils::Assert(lock_msg == req, "invalid granted lock");
            state = kLockGranted;
            req.type = LockMessage::kLockGrantedAck;
            ++req.counter;
          }
          // whenever recv the message send req back
          this->SendMessage(req);
          break;
        }
        default: utils::Error("invalid lock message type to client");
      }
    }
    // push to lock_index
    aqquired_lock.push_back(lock_index);
  }
  inline void UnLock(unsigned lock_index) {
    bool aqquired = false;
    for (std::list<unsigned>::iterator it = aqquired_lock.begin();
         it != aqquired_lock.end(); ++it) {
      if (*it == lock_index) {
        aqquired = true;
        aqquired_lock.erase(it); break;
      }
    }
    utils::Check(aqquired, "cannot unlock a lock that is not aqquired");
    req.type = LockMessage::kUnlockRequest;
    req.lock_index = lock_index;    
    ++req.counter;
    this->SendMessage(req);
    // start sending message to server
    while (true) {
      bool ret = post->RecvFrom(&in_msg);
      if (!ret) {
        // timeout, retransmit request
        this->SendMessage(req); continue;
      }
      in_msg.Seek(0);
      // sender 
      unsigned sender;
      LockMessage::Type type;
      LockMessage lock_msg;
      // first two are always sender and type
      utils::Check(in_msg.Read(&sender, sizeof(sender)) != 0, "Unlock-1: invalid message");
      utils::Check(in_msg.Read(&type, sizeof(type)) != 0, "Unlock-2: invalid message");
      utils::Check(in_msg.Read(&lock_msg, sizeof(lock_msg)) != 0, "Unlock-3: invalid message");
      // get ack from server, can break out
      if (lock_msg == req && type == LockMessage::kServerAck) break;
    }
  }
  
 private:
  // for simplicity send to all known lock server
  inline void SendMessage(LockMessage msg) {
    out_msg.Clear();
    out_msg.WriteT(msg.node_client);
    out_msg.WriteT(LockServer::kClientRequest);
    out_msg.WriteT(msg);
    for (unsigned i = 0; i < num_server; ++i) {
      post->SendTo(i, out_msg);
    }
  }
  // the temporal output message
  utils::Message in_msg, out_msg;
  // list of aqquired lock
  std::list<unsigned> aqquired_lock;
  // common request message
  LockMessage req;
  // post office
  IPostOffice *post;
  // number of server
  unsigned num_server;
  // this node id
  unsigned node_client;
};
}  // namespace consencus
#endif
