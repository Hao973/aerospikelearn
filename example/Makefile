#Makefile
OBJECT= ./src/aeroclient.o ./src/aeromain.o 
		
CPP=g++ -g -std=c++11

INDEBUG=-I./src -I/usr/include/aerospike
LIBFLAG=-lssl -lcrypto -lpthread -ldl -lz
LIBAEROSPIKE=/usr/lib/libaerospike.a 

all: aeromain

$(OBJECT):%.o:%.cpp
	$(CPP) ${INDEBUG} ${LIBFLAG} -c $< -o $@

aeromain:$(OBJECT)
	$(CPP) -o aeromain ${OBJECT} ${INDEBUG} ${LIBAEROSPIKE} ${LIBFLAG} 
clean:
	$(RM) *.o ./src/*.o  aeromain
