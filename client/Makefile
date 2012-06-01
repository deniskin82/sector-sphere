include ../Makefile.common

CCFLAGS += -I../udt -I../gmp -I../common -I../security

OBJS = client_conf.o fscache.o client.o fsclient.o dcclient.o clientmgmt.o

all: libclient.so libclient.a

test: fscache_unittest

%.o: %.cpp
	$(C++) -fPIC $(CCFLAGS) $< -c

libclient.so: $(OBJS)
	$(C++) -shared -o $@ $^ $(LDFLAGS) -ludt -lrpc -lcommon

libclient.a: $(OBJS)
	ar -rcs $@ $^

fscache_unittest: fscache_unittest.cpp fscache.h fscache.cpp all 
	$(C++) $(CCFLAGS) fscache_unittest.cpp -o $@ $(LDFLAGS) -ludt -lrpc -lclient -lcommon

clean:
	rm -f *.o *.so *.a

install:
	mv libclient.so libclient.a ../lib
