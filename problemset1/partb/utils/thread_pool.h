#ifndef UTILS_THREAD_POOL_H_
#define UTILS_THREAD_POOL_H_
/*!
 * \file thread_pool.h
 * \brief implementation of simple thread pool
 */
#include "./utils.h"
#include "./thread.h"
#include <vector>
#include <queue>

namespace utils {
class ThreadPool {
 public:
  ThreadPool(int nthread) {
    this->InitPool(nthread);
  }
  // destructor, stops all threads, release the resources
  ~ThreadPool(void) {
    destroy_signal = true;
    for (size_t i = 0; i < workers.size(); ++i) {
      task_counter.Post();
    }
    for (size_t i = 0; i < workers.size(); ++i) {
      workers[i].Join();
    }
    queue_lock.Destroy();
    task_counter.Destroy();
  }
  inline void AddJob(void (*func)(void*), void *arg) {
    Task t; t.func = func; t.argument = arg;
    queue_lock.Lock();
    queue.push(t);
    queue_lock.Unlock();
    task_counter.Post();
  }
  // wait for all jobs in the queue to finish
  inline void WaitAllJobs(void) {
   // note this is not enough, this only indicate all jobs finish launch
    while(queue.size() != 0) {
      queue_empty.Wait();
    }
  }

 private:
  inline void InitPool(int nthread) {
    this->destroy_signal = false;
    queue_lock.Init();
    task_counter.Init(0);
    workers.resize(nthread);
    for (size_t i = 0; i < workers.size(); ++i) {
      workers[i].Start(ThreadEntry, this);
    }
  }
  // this is the routine worker thread run
  inline void RunWorkerThread(void) {
    while (!destroy_signal) {
      task_counter.Wait();
      if (destroy_signal) break;
      queue_lock.Lock();
      Task tsk = queue.front();
      queue.pop();
      queue_lock.Unlock();
      // run the functions
      tsk.func(tsk.argument);  
      // finish the job
      if (queue.size() == 0) queue_empty.Post();
    }
  }
  /*!\brief entry point of loader thread */
  inline static THREAD_PREFIX ThreadEntry(void *pthread) {
    static_cast<ThreadPool*>(pthread)->RunWorkerThread();
    ThreadExit(NULL);
    return NULL;
  }
  /*! \brief the interface of tasks that can be run */
  struct Task {
    /*! \brief the function pointer to the function we want to run */
    void (*func)(void *arg);
    /*! 
     * \brief the pointer to the argument, 
     *   the argument should be kept live during until the function is called
     */
    void *argument;
  };  
  // the thread pool is destructing
  bool destroy_signal;
  // task queue
  std::queue<Task> queue;
  // workers in the thread pool
  std::vector<Thread> workers;
  // lock for accessing the queue
  Mutex queue_lock;
  // number of tasks in the quque
  Semaphore task_counter, queue_empty;
};
}  // namespace utils
#endif

