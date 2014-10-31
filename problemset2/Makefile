export CC  = gcc
export CXX = g++
export LDFLAGS= -pthread -lm 

export CFLAGS = -Wall -O3  -pedantic 

# specify tensor path
BIN = paxos_sim
OBJ = 

paxos_sim: paxos_sim.cpp utils/*.h *.hpp *.h 

.PHONY: clean all

all: $(BIN) $(OBJ) $(SLIB) 


$(BIN) : 
	$(CXX) $(CFLAGS) $(LDFLAGS) -o $@ $(filter %.cpp %.o %.c, $^)

$(OBJ) : 
	$(CXX) -c $(CFLAGS) -o $@ $(firstword $(filter %.cpp %.c, $^) )

clean:
	rm -rf $(BIN) $(OBJ) *~ */*~
