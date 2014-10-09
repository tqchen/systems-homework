#ifndef UTILS_UTILS_H_
#define UTILS_UTILS_H_
/*!
 * \file utils.h
 * \brief some utils function for error handling
 * \author Tianqi Chen
 */
#include <cstdio>
#include <string>
#include <cstdlib>
#include <vector>
#include <cstdarg>

namespace utils {
/*! \brief error message buffer length */
const int kPrintBuffer = 1 << 12;

inline void HandleCheckError(const char *msg) {
  fprintf(stderr, "%s\n", msg);
  exit(-1);
}
/*!\brief same as assert, but this is intended to be used as message for user*/
inline void Check(bool exp, const char *fmt, ...) {
  if (!exp) {
    std::string msg(kPrintBuffer, '\0');
    va_list args;
    va_start(args, fmt);
    vsnprintf(&msg[0], kPrintBuffer, fmt, args);
    va_end(args);
    HandleCheckError(msg.c_str());
  }
}

/*! \brief report error message, same as check */
inline void Error(const char *fmt, ...) {
  {
    std::string msg(kPrintBuffer, '\0');
    va_list args;
    va_start(args, fmt);
    vsnprintf(&msg[0], kPrintBuffer, fmt, args);
    va_end(args);
    HandleCheckError(msg.c_str());
  }
}

/*! \brief printf, print message to the console */
inline void LogPrintf(const char *fmt, ...) {
  va_list args;
  va_start(args, fmt);
  vprintf(fmt, args);
  va_end(args);
}
} // namespace utils
#endif

