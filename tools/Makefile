include ../Makefile.common

ifeq ($(STATIC), 1)
   LDFLAGS += ../lib/libclient.a ../lib/libsecurity.a ../lib/librpc.a ../lib/libcommon.a ../lib/libudt.a
else
   LDFLAGS += -L../lib -lsecurity -lrpc -ludt -lcommon -lclient
endif

EXE = $(addprefix sector_, download upload ls rm mkdir stat sysinfo mv cp pipe shutdown replicate) \
       sphere_stream 

all: $(EXE)

%.o: %.cpp
	$(C++) -fPIC $(CCFLAGS) $< -c

sector_%: %.cpp
	$(C++) $^ -o $@ $(CCFLAGS) $(LDFLAGS)

sphere_stream: stream.cpp probot.o
	$(C++) $^ -o $@ $(CCFLAGS) $(LDFLAGS)

clean:
	rm -f *.o $(EXE)

install:
	
