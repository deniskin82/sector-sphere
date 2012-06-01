/*****************************************************************************
Copyright 2011 VeryCloud LLC

Licensed under the Apache License, Version 2.0 (the "License"); you may not
use this file except in compliance with the License. You may obtain a copy of
the License at

   http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
License for the specific language governing permissions and limitations under
the License.
*****************************************************************************/

/*****************************************************************************
written by
   bdl62, last updated 05/21/2011
*****************************************************************************/

#include <cassert>
#include <iostream>
#include <string>

#include "fscache.h"

using namespace std;

// Test file metadata cache
int test1()
{
   Cache cache;

   const string filename = "testfile";
   const int64_t size = 100;
   const int64_t timestamp = 101;

   cache.update(filename, timestamp, size);
   SNode s;
   int result = cache.stat(filename, s);
   assert(result == 0);

   const int64_t update_size = 102;
   const int64_t update_timestamp = 103;
   cache.update(filename, update_timestamp, update_size);
   result = cache.stat(filename, s);
   assert(result == 1);
   assert((s.m_llSize == update_size) && (s.m_llTimeStamp == update_timestamp));

   cache.remove(filename);
   result = cache.stat(filename, s);
   assert(result < 0);

   cout << "Metadata cache testing passed.\n";
   return 0;
}

// Test IO data cache
int test2()
{
   Cache cache;

   cache.setCacheBlockSize(200);

   const string filename = "testfile";
   const int64_t size = 100;
   const int64_t timestamp = 101;
   cache.update(filename, timestamp, size);

   int64_t offset = 200;
   int64_t block = 201;
   char* buf = new char[block];
   *buf = 'z';
   cache.insert(buf, filename, offset, block);

   char data[block];
   int64_t result = cache.read(filename, data, offset, block);
   assert((result == block) && (*data == 'z'));

   result = cache.read(filename, data, offset, block + 1);
   assert(result == block);

   // Test large block that cover multiple cache units.
   int64_t off2 = 900;
   int64_t block2 = 901;
   char* large_buf = new char[block2];
   cache.insert(large_buf, filename, off2, block2);

   char data2[block2];
   result = cache.read(filename, data2, off2, block2);
   assert(result == block2);

   // Read partial data.
   result = cache.read(filename, data2, off2 + 50, block2 - 100);
   assert(result == block2 - 100);

   // Test maximu cache size.
   cache.setMaxCacheSize(1000);

   // Insert overlapping buffer block.
   large_buf = new char[block2];
   cache.insert(large_buf, filename, off2, block2);
   large_buf = new char[block2];
   cache.insert(large_buf, filename, off2, block2);

   // Cache should constain most recent block only,
   assert(cache.getCacheSize() == 901);

   cout << "IO cache check passed.\n";
   return 0;
}

// performance and memory leak testing.
int test3(const int64_t& max, int unit, int block)
{
   Cache cache;

   cache.setMaxCacheSize(max);
   cache.setCacheBlockSize(unit);

   const string filename = "testfile";
   const int64_t size = unit * 100;
   const int64_t timestamp = 101;
   cache.update(filename, timestamp, size);

   for (int i = 0; i < 100; ++ i)
   {
      char* buf = new char[block];
      cache.insert(buf, filename, block * i, block);
   }

   assert(cache.getCacheSize() <= max);

   char* buf = new char[unit];
   for (int i = 0; i < 100; ++ i)
   {
      cache.read(filename, buf, block * i, block);
   }

   cout << "performance and memory leak testing passed.\n";
   return 0;
}


// Consistency check.
int test4()
{
   Cache cache;

   const string filename = "testfile";
   const int64_t size = 100;
   const int64_t timestamp = 101;
   cache.update(filename, timestamp, size);

   int* val = new int(1);
   cache.insert((char*)val, filename, 0, sizeof(int));
   *val = 2;
   cache.insert((char*)val, filename, 0, sizeof(int));

   int readval = 0;
   cache.read(filename, (char*)&readval, 0, sizeof(int));
   assert(readval == *val);

   *val = 3;
   cache.insert((char*)val, filename, 0, sizeof(int));
   cache.read(filename, (char*)&readval, 0, sizeof(int));
   assert(readval == *val);

   cout << "consistency testing passed.\n";
   return 0;
}

int main()
{
   test1();
   test2();
   test3(100000, 100, 100);
   test3(100000, 200, 100);
   test3(100000, 100, 200);
   test4();

   return 0;
}
