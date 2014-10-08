#include "utils/utils.h"
#include "utils/thread_pool.h"

int a = 0;
void print(void *arg) {
  int *pa = (int*)arg;
  pa[0]+=1;
  printf("hello world %d\n", *pa);
}
int main(int argc, char *argv[]) {
  utils::ThreadPool pool(5);
  pool.AddJob(print, &a);
  pool.AddJob(print, &a);
  pool.AddJob(print, &a);
  pool.WaitAllJobs();
  return 0;
}
