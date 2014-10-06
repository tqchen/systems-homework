#include <cstdio>
#include <vector>
#include <cstring>
#include <string>
#include <sys/wait.h>
#include <stdlib.h>
#include <unistd.h>
#include "./utils.h"

int main(int argc, char *argv[]) {
  char *test = "ls";
  std::vector<char *> args;
  args.push_back(test);
  args.push_back(NULL);

  execvp("ls", &args[0]);
  return 0;
}

