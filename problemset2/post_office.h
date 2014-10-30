#ifndef POST_OFFICE_H_
#define POST_OFFICE_H_

#include "./utils/utils.h"
#include "./utils/message.h"

namespace consencus {
/*! 
 * \brief interface of post office that handles message passing
 * this only defines the interface, the implementation can be based on different protocols: e.g. in process pipe, UDP
 */
class IPostOffice {
 public:
  virtual ~IPostOffice(){}
  /*! \brief number of nodes in the group */
  virtual int WorldSize(void) const = 0;
  /*! 
   * \brief get the unique id of current node in the group 
   * this is ensured to be from 0 - n - 1
   */
  virtual int GetRank(void) const = 0;
  /*! 
   * \brief recieve the message from any body in the world
   *  the message sender is not identified, and need to be encoded in the message it self if needed 
   * \return 
   *  true if a message is successfully recieved
   *  false if timeout happens
   */
  virtual bool RecvFrom(utils::Message *msg) = 0;
  /*! 
   * \brief send mesage of target node nid 
   * \param nid node id 
   * \param msg the message 
   */  
  virtual void SendTo(int nid, const utils::Message &msg) = 0;
};
}  // namespace consencus
#endif
