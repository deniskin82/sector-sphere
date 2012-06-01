include ../Makefile.common

ifeq ($(AMD64), 1)
   CCFLAGS += -D AMD64
else
   CCFLAGS += -D IA32
endif

ifeq ($(OSX), 1)
   CCFLAGS += -D OSX
else
   CCFLAGS += -D LINUX
endif

LDFLAGS += 

OBJS = md5.o common.o window.o list.o buffer.o packet.o channel.o queue.o core.o cache.o epoll.o api.o ccc.o

all: libudt.so libudt.a

%.o: %.cpp %.h
	$(C++) -fPIC $(CCFLAGS) $< -c

libudt.so: $(OBJS)
	$(C++) -shared -o $@ $^ $(LDFLAGS)

libudt.a: $(OBJS)
	ar -rcs $@ $^

clean:
	rm -f *.o *.so *.a

install:
	mv libudt.so libudt.a ../lib
