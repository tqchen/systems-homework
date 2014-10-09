Teammember:

Tianqi Chen, 1323348, tqchen@cs.washington.edu
Tianyi Zhou, 1323375, tianzh@cs.washington.edu

Part A: Piping Shell

We firstly build a parser to separate different commands. Then for each command, we creat a pipe and use fork() to excute it. The child programs close pipes and replace their read end, write end with current states, and are excuted in parallel. We wait for all the child programs to terminate before starting next one. 

Part B: AMTED Server

Our submission fulfills all "MUST". "SHOULD" and the first requirement in "CAN". The main thread handles asynchronous network I/O, uses listen socket receiving (multiple) requests, and uses threadpool to manage multple worker threads. 