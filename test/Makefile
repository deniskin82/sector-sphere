include ../Makefile.common

CCFLAGS += -I../client -I../gmp -I../udt -I../common

ifeq ($(STATIC), 1)
   LDFLAGS += ../lib/libclient.a ../lib/libsecurity.a ../lib/librpc.a ../lib/libcommon.a ../lib/libudt.a
else
   LDFLAGS += -L../lib -lsecurity -lrpc -ludt -lcommon -lclient
endif

EXE = iotest send_dbg_cmd

all: $(EXE)

%.o: %.cpp
	$(C++) -fPIC $(CCFLAGS) $< -c

iotest: iotest.cpp
	$(C++) $^ -o $@ $(CCFLAGS) $(LDFLAGS)

send_dbg_cmd: send_dbg_cmd.cpp
	$(C++) $^ -o $@ $(CCFLAGS) $(LDFLAGS)

clean:
	rm -f *.o $(EXE)

install:
	
