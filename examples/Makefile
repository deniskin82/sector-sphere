include ../Makefile.common

ifeq ($(STATIC), 1)
   LDFLAGS += ../lib/libclient.a ../lib/libsecurity.a ../lib/librpc.a ../lib/libcommon.a ../lib/libudt.a 
else
   LDFLAGS += -L../lib -lsecurity -lrpc -ludt -lcommon -lclient
endif

APP = testfs testdc wordcount text.idx mrsort mrword

all: $(APP) funcs

%: %.cpp
	$(C++) $^ -o $@ $(CCFLAGS) $(LDFLAGS)

funcs: _always_check_
	cd ./funcs; make; cd ../

_always_check_:
	true

clean:
	rm -f *.o *.so *.a $(APP) ./funcs/*.so

install:
	
