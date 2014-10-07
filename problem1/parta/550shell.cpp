#include <cstdio>
#include <vector>
#include <cstring>
#include <string>
#include <sys/wait.h>
#include <stdlib.h>
#include <unistd.h>
#include "./utils.h"

const int kBuffer = 2 << 20;

struct pipe_t {
  int fd[2];
};

int main(int argc, char *argv[]) {  
  char *scmd;
  size_t n = 0;
  while (getline(&scmd, &n, stdin) != -1) {
    // remove \n
    scmd[strlen(scmd) - 1] = '\0';
    n = 0;
    std::vector<char*> cmds; 
    {
      char *ptr = strtok(scmd, "|");
      while (ptr != NULL) {
        cmds.push_back(ptr);
        ptr = strtok(NULL, "|");
      }
    }
    
    std::vector<pid_t> childs;
    std::vector<pipe_t> pipes;

    for (size_t i = 0; i+1 < cmds.size(); ++i) {
      pipe_t p;
      Check(pipe(p.fd) != -1, "cannot create pipe");
      pipes.push_back(p);
    }

    for (size_t i = 0; i < cmds.size(); ++i) {
      pid_t pid = fork();
      if (pid == 0) {
        // child
        if (i != 0) {
          close(pipes[i-1].fd[1]);
          // child
          dup2(pipes[i-1].fd[0], STDIN_FILENO);
        }
        if (i + 1 != cmds.size()) {
          close(pipes[i].fd[0]);
          dup2(pipes[i].fd[1], STDOUT_FILENO);        
        }        
        // execute command   
        std::vector<char*> args;
        char * ptr = strtok(cmds[i], " \t");
        while (ptr != NULL) {
          args.push_back(ptr);
          ptr = strtok(NULL, " \t");        
        }
        Check(args.size() > 0, "invalid command format");
        args.push_back(NULL);
        // run execute
        Check(execvp(args[0], &args[0]) != -1, "error when executing %s", args[0]);
      } else {
        childs.push_back(pid);
      }
    }
    
    for (size_t i = 0; i < childs.size(); ++i) {
      int status;
      waitpid(childs[i], &status, 0);
    }
    free(scmd);
  }
  return 0;
}
