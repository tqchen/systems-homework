#ifndef POST_UDP_INL_HPP_
#define POST_UDP_INL_HPP_

#include <cstring>
#include <netdb.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <cstdlib>
#include <algorithm>
#include "./post_office.h"
#include "./utils/utils.h"
#include "./utils/thread.h"
#include "./utils/message.h"

namespace consencus {

// use UDP to implement message passing
class PostOfficeUDP : public IPostOffice {
 public:
  PostOfficeUDP(const std::vector<sockaddr_in> &machines, unsigned node_id)
      : addrs(machines), node_id(node_id) {
    utils::Check((sockfd = socket(AF_INET, SOCK_DGRAM, 0)) != -1, "failed to created UDP socket");
    utils::Check(bind(sockfd, (sockaddr*)&addrs[node_id], sizeof(addrs[node_id])) != -1, "fail to bind");
  }
  /*! \brief number of nodes in the group */
  virtual unsigned WorldSize(void) const {
    return addrs.size();
  }
  /*! 
   * \brief get the unique id of current node in the group 
   * this is ensured to be from 0 - n - 1
   */
  virtual unsigned GetRank(void) const {
    return node_id;
  }
  /*! 
   * \brief recieve the message from any body in the world
   *  the message sender is not identified, and need to be encoded in the message it self if needed 
   * \return 
   *  true if a message is successfully recieved
   *  false if timeout happens
   */
  virtual bool RecvFrom(utils::Message *msg) {
    timeval timeout;
    // use 1 sec timeout
    timeout.tv_sec = 0;
    timeout.tv_usec = 100000;
    fd_set rfds;
    FD_ZERO(&rfds);
    FD_SET(sockfd, &rfds);
    int ret = select(FD_SETSIZE, &rfds, NULL, NULL, &timeout);
    // timeout
    if (ret == 0) return false;
    msg->message_size = recvfrom(sockfd, msg->data, utils::Message::kMaxSize, 0, NULL, NULL);
    return true;
  }
  /*! 
   * \brief send mesage of target node nid 
   * \param nid node id 
   * \param msg the message 
   */  
  virtual void SendTo(unsigned nid, const utils::Message &msg) {
    sendto(sockfd, msg.data, msg.message_size, 0, (sockaddr*)&addrs[nid], sizeof(addrs[nid]));
  }
  // load address from file list
  inline static void LoadAddrs(const char *fname, std::vector<sockaddr_in> *p_out) {
    FILE *fi = fopen(fname, "r");
    utils::Check(fi != NULL, "cannot open \"%s\"", fname);
    char url[256]; 
    int port;
    
    while (fscanf(fi, "%s%d", url, &port) == 2) {
      hostent *hp = gethostbyname(url);
      utils::Check(hp != NULL, "cannot obtain address of %s", url);
      sockaddr_in addr;
      memset(&addr, 0, sizeof(addr));
      addr.sin_family = AF_INET;
      addr.sin_port = htons(port);
      memcpy(&addr.sin_addr, hp->h_addr_list[0], hp->h_length);
      p_out->push_back(addr);
    }
    fclose(fi);
  }
 private:
  // socket
  int sockfd;
  // address list of all  nodes
  std::vector<sockaddr_in> addrs;
  // current node id
  unsigned node_id;
};
}  // namespace consencus
#endif
