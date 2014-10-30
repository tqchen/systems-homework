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

#ifdef _MSC_VER
typedef unsigned char uint8_t;
typedef unsigned short int uint16_t;
typedef unsigned int uint32_t;
typedef unsigned long uint64_t;
typedef long int64_t;
#else
#include <inttypes.h>
#endif

namespace utils {
/*! \brief error message buffer length */
const int kPrintBuffer = 1 << 12;

inline void HandleCheckError(const char *msg) {
  fprintf(stderr, "%s\n", msg);
  exit(-1);
}
inline void HandleAssertError(const char *msg) {
  fprintf(stderr, "%s\n", msg);
  exit(-1);
}

/*! \brief assert an condition is true, use this to handle debug information */
inline void Assert(bool exp, const char *fmt, ...) {
  if (!exp) {
    std::string msg(kPrintBuffer, '\0');
    va_list args;
    va_start(args, fmt);
    vsnprintf(&msg[0], kPrintBuffer, fmt, args);
    va_end(args);
    HandleAssertError(msg.c_str());
  }
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

/*! \brief seed the PRNG */
inline void Seed(unsigned seed) {
  srand(seed);
}
/*! \brief basic function, uniform */
inline double Uniform(void) {
  return static_cast<double>(rand()) / (static_cast<double>(RAND_MAX)+1.0);
}

} // namespace utils
#endif

