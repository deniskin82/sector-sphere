/*****************************************************************************
Copyright 2005 - 2010 The Board of Trustees of the University of Illinois.

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
   Yunhong Gu, last updated 04/23/2010
*****************************************************************************/

#ifndef __SECTOR_FS_CACHE_H__
#define __SECTOR_FS_CACHE_H__

#include <list>
#include <map>
#ifndef WIN32
   #include <pthread.h>
#endif
#include <string>

#include <index.h>


struct InfoBlock
{
   int m_iCount;		// number of reference
   int m_bChange;		// if the file has been changed
   int64_t m_llTimeStamp;	// time stamp
   int64_t m_llSize;		// file size
   int64_t m_llLastAccessTime;	// last access time
};

struct CacheBlock
{
   CacheBlock();

   int64_t m_llBlockID;			// unique block ID, as blocks may have same filename, offset, size, etc.
   std::string m_strFile;               // file name 
   int64_t m_llOffset;                  // cache block offset
   int64_t m_llSize;                    // cache size
   int64_t m_llCreateTime;              // cache creation time
   int64_t m_llLastAccessTime;          // cache last access time
   int m_iAccessCount;                  // number of accesses
   char* m_pcBlock;                     // cache data
   bool m_bWrite;			// if this block is being written

   static int64_t s_llBlockIDSeed;	// a seed number to generate unique block ID.
};

class Cache
{
// File metadata cache, for keep tracking file changes that have not been
// updated to the master yet.
typedef std::map<std::string, InfoBlock> InfoBlockMap;

// Store the physical cache block. The physical blocks are on a single list, shared by all open files.
typedef std::list<CacheBlock*> CacheBlockList;
typedef CacheBlockList::iterator CacheBlockIter;

// Each index tree is lined by fix-sized slots. Each slot may contain a list of blocks. 
// Blocks may overlap or even cover the same file region. This is especially true to write cache.
typedef std::list<CacheBlockIter> BlockIndex;
typedef std::map<int64_t, BlockIndex> BlockIndexMap;

// Each file has its own block tree.
typedef std::map<std::string, BlockIndexMap> FileCacheMap;

public:
   Cache();
   ~Cache();

   int setCacheBlockSize(const int size);
   int setMaxCacheSize(const int64_t& ms);
   int setMaxCacheTime(const int64_t& mt);
   int setMaxCacheBlocks(const int num);

   int64_t getCacheSize() {return m_llCacheSize;}

public: // operations for file metadata cache
   void update(const std::string& path, const int64_t& ts, const int64_t& size, bool first = false, bool doEvict = true);
   void remove(const std::string& path);
   int stat(const std::string& path, SNode& attr);

public: // operations for file data cache
   int insert(char* block, const std::string& path, const int64_t& offset, const int64_t& size, const bool& write = false);

      // Functionality:
      //    Read certain amount of data from cache.
      // Parameters:
      //    0) [in] path: file name.
      //    1) [out] buf: buffer to receive data.
      //    2) [in] offset: from where to read data.
      //    3) [in] size: total data to read.
      // Returned value:
      //    Actual size of data read. This may return 0 but never return negative value.

   int64_t read(const std::string& path, char* buf, const int64_t& offset, const int64_t& size);
   char* retrieve(const std::string& path, const int64_t& offset, const int64_t& size, const int64_t& id);
   int clearWrite(const std::string& path, const int64_t& offset, const int64_t& size, const int64_t& id);

private:
   void shrink();
   void parseIndexOffset(const int64_t& offset, const int64_t& size, int64_t& index_off, int64_t& block_num);
   void releaseBlock(CacheBlock* cb);
   void evict(const std::string& path);

private:
   InfoBlockMap m_mOpenedFiles;

   // The cache blocks are stored in a double linked list with oldest block as the head node.
   // Every file keeps an index to the blocks. In order to retrieve cache block first, the index
   // use fixed sized block offset.
   std::list<CacheBlock*> m_lCacheBlocks;
   FileCacheMap m_mFileCache;

   int64_t m_llCacheSize;               // total size of cache
   int m_iBlockNum;                     // total number of cache blocks
   int64_t m_llMaxCacheSize;            // maximum size of cache allowed
   int64_t m_llMaxCacheTime;            // maximum time to stay in the cache without IO
   int m_iMaxCacheBlocks;               // maximum number of cache blocks
   int m_iBlockUnitSize;                // the index block size

   pthread_mutex_t m_Lock;
};


#endif
