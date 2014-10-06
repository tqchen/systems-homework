#include <cstdio>
#include <vector>
#include <cstring>
#include <string>
#include <sys/wait.h>
#include <stdlib.h>
#include <unistd.h>
#include "./utils.h"

const int kBuffer = 2 << 20;

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
    
    pid_t run_pid = fork();
    if (run_pid == 0) { 
      for (size_t i = 0; i < cmds.size(); ++i) {
        std::vector<char*> args;
        char * ptr = strtok(cmds[i], " \t");
        while (ptr != NULL) {
          args.push_back(ptr);
          ptr = strtok(NULL, " \t");        
        }
        Check(args.size() > 0, "invalid command format");
        args.push_back(NULL);
        // run command
        int pipefd[2];
        
        if (i + 1 != cmds.size()) {
          Check(pipe(pipefd) != -1, "cannot create pipe");
          pid_t pid = fork();
          if (pid == 0) {
            // child
            close(pipefd[1]);          
            dup2(pipefd[0], STDIN_FILENO);
          } else {
            // parent
            close(pipefd[0]);
            dup2(pipefd[1], STDOUT_FILENO);
            execvp(args[0], &args[0]);
            break;
          }
        } else {
          execvp(args[0], &args[0]);
          break;
        }
      }
    } else {
      int status;
      waitpid(run_pid, &status, 0);
    }
    free(scmd);
  }
  return 0;
}
