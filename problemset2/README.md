Team
===

Tianqi Chen, 1323348, tqchen@cs.washington.edu
Tianyi Zhou, 1323375, tianzh@cs.washington.edu

Simulator
===
Simulator is a tool that executes the commands and simulates the drop of message sending
* You can use exec[nodeid]=lock[lock-id] to ask nodeid to execute lock(lock-id)
* You can use drop[nodid]=0.8 to set the droprate of messages to nodeid to 0.8
* sleep=number is used to insert lags
* raise[nodeid]=10 will enforce nodeid to execute prepare phase by increasing its proposal counter by 10
* Example: ```cd test;../paxos_sim sim2.txt```
* Example: ```cd test;../paxos_sim sim_unstable.txt```  this example drops packets between all connections by probability at least 0.8
* Typical tests: set drop[nodeid]=1 to remove the node from group, then set drop[noidid]=0 to get it back.

UDP Server
===
UDP server will starts a lock server client by reading a hardcoded address list.
* To start server 0 ```cd test;../paxos_udp_server nodelist.txt 3 0```
* To start server 1 ```cd test;../paxos_udp_server nodelist.txt 3 1```
* To start server 2 ```cd test;../paxos_udp_server nodelist.txt 3 2```
* To start client 3 ```cd test;../paxos_udp_client nodelist.txt 3 3```
* To start client 4 ```cd test;../paxos_udp_client nodelist.txt 3 4``
This will start 3 paxos lock server, and 2 lock client. We can then type lock(lock-id) or unlock(lock-id) in the client.

