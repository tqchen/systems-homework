#ifndef UTILS_MESSAGE_H_
#define UTILS_MESSAGE_H_
/*!
 * \file message.h
 * \brief simple data structure to handle packed messages
 * \author Tianqi Chen
 */
#include <cstring>
#include "./io.h"

namespace utils {
/*! 
 * \brief a data structure used to pack and unpack messages
 * the message can be used to exchange information via UDP
 */
struct Message : public ISeekStream {
 public:
  // size of the message
  size_t message_size;
  // maximum size of the buffer
  const static int kMaxSize = 4 << 20;
  // data content 
  char *data;
  // constructor
  Message(void) : message_size(0), curr_ptr_(0) {
    data = new char[kMaxSize];
  }
  ~Message(void) {
    delete [] data;
  }
  virtual size_t Read(void *ptr, size_t size) {
    utils::Assert(curr_ptr_ <= message_size,
                  "read can not have position excceed buffer length");
    size_t nread = std::min(message_size - curr_ptr_, size);
    if (nread != 0) memcpy(ptr, data + curr_ptr_, nread);
    curr_ptr_ += nread;
    return nread;
  } 
  virtual void Write(const void *ptr, size_t size) {
    if (size == 0) return;
    if (curr_ptr_ + size > message_size) {
      utils::Assert(curr_ptr_ + size <= kMaxSize,
                    "exceed maximum limit of message buffer");
      message_size = curr_ptr_ + size;
    }
    memcpy(data + curr_ptr_, ptr, size); 
    curr_ptr_ += size;
  }
  
  /*! \brief seek to certain position of the message */
  virtual void Seek(size_t pos) {
    curr_ptr_ = pos;
  }
  /*! \brief tell the position of the file pointer in message */
  virtual size_t Tell(void) {
    return curr_ptr_;
  }
  /*! \brief clear the message */  
  inline void Clear(void) {
    this->message_size = 0;
    this->curr_ptr_ = 0;
  }
  
 private:
  // used to set file pointer 
  size_t curr_ptr_;
};

}
#endif
