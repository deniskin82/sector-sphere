include ../Makefile.common

CCFLAGS += -I../gmp -I../security -I../common

ifeq ($(STATIC), 1)
   LDFLAGS += libmaster.a ../lib/librpc.a ../lib/libsecurity.a ../lib/libcommon.a ../lib/libudt.a
else
   LDFLAGS += -L../lib -lmaster -lrpc -lsecurity -lcommon -ludt
endif

OBJS = master_conf.o slavemgmt.o user.o replica.o master.o

all: libmaster.so libmaster.a start_master start_all stop_all
test: config_unittest replica_unittest

%.o: %.cpp
	$(C++) -fPIC $(CCFLAGS) $< -c

libmaster.so: $(OBJS)
	$(C++) -shared -o $@ $^

libmaster.a: $(OBJS)
	ar -rcs $@ $^

start_master: start_master.cpp libmaster.a libmaster.so
	$(C++) start_master.cpp -o start_master $(CCFLAGS) $(LDFLAGS)

start_all: start_all.cpp
	$(C++) start_all.cpp -o start_all $(CCFLAGS) $(LDFLAGS)

stop_all: stop_all.cpp
	$(C++) stop_all.cpp -o stop_all $(CCFLAGS) $(LDFLAGS)

config_unittest: config_unittest.cpp master_conf.cpp
	$(C++) config_unittest.cpp -o $@ $(CCFLAGS) $(LDFLAGS)

replica_unittest: replica_unittest.cpp replica.h replica.cpp
	$(C++) replica_unittest.cpp -o $@ $(CCFLAGS) $(LDFLAGS)

clean:
	rm -f *.o *.so *.a start_master start_all stop_all

install:
	mv libmaster.so libmaster.a ../lib
	../security/ssl_cert_gen master
