C++ = g++
CCFLAGS = -I../../include -I../../udt -shared -fPIC -O3 -g
LDFLAGS = -lstdc++ ../../lib/libcommon.a ../../lib/libudt.a

all: randwriter.so sort.so sorthash.so wordbucket.so gen_idx.so mr_sort.so mr_word.so

%.so: %.cpp
	$(C++) $(CCFLAGS) -o $@ $^ $(LDFLAGS)

clean:
	rm -f *.so
