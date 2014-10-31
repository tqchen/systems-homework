export CC  = gcc
export CXX = g++
export LDFLAGS= -pthread -lm 

export CFLAGS = -Wall -O3  -pedantic 

# specify tensor path
BIN = paxos_sim paxos_udp_server paxos_udp_client
OBJ = 

.PHONY: clean all

all: $(BIN) $(OBJ) $(SLIB) 

paxos_sim: paxos_sim.cpp utils/*.h *.hpp *.h 
paxos_udp_server: paxos_udp_server.cpp utils/*.h *.hpp *.h 
paxos_udp_client: paxos_udp_client.cpp utils/*.h *.hpp *.h 

$(BIN) : 
	$(CXX) $(CFLAGS) $(LDFLAGS) -o $@ $(filter %.cpp %.o %.c, $^)

$(OBJ) : 
	$(CXX) -c $(CFLAGS) -o $@ $(firstword $(filter %.cpp %.c, $^) )

clean:
	rm -rf $(BIN) $(OBJ) *~ */*~
