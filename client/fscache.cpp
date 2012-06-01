/*****************************************************************************
Copyright 2005 - 2011 The Board of Trustees of the University of Illinois.

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
   Yunhong Gu, last updated 03/17/2011
*****************************************************************************/

#include <assert.h>
#include <string.h>
#include <iostream>
#include "common.h"
#include "fscache.h"
#include <unistd.h>

using namespace std;


int64_t CacheBlock::s_llBlockIDSeed = 0;

CacheBlock::CacheBlock()
{
   m_llBlockID = s_llBlockIDSeed ++;
   if (s_llBlockIDSeed < 0)
      s_llBlockIDSeed = 0;
}

Cache::Cache():
m_llCacheSize(0),
m_iBlockNum(0),
m_llMaxCacheSize(10000000),
m_llMaxCacheTime(10000000),
m_iMaxCacheBlocks(4096),
m_iBlockUnitSize(1000000)
{
   CGuard::createMutex(m_Lock);
}

Cache::~Cache()
{
   CGuard::releaseMutex(m_Lock);
}

int Cache::setCacheBlockSize(const int size)
{
   CGuard sg(m_Lock);

   // Cannot change block size if there is already data in the cache.
   if (m_llCacheSize > 0)
      return -1;

   m_iBlockUnitSize = size;
   return 0;
}

int Cache::setMaxCacheSize(const int64_t& ms)
{
   CGuard sg(m_Lock);
   m_llMaxCacheSize = ms;
   return 0;
}

int Cache::setMaxCacheTime(const int64_t& mt)
{
   CGuard sg(m_Lock);
   m_llMaxCacheTime = mt;
   return 0;
}

int Cache::setMaxCacheBlocks(const int num)
{
   CGuard sg(m_Lock);
   m_iMaxCacheBlocks = num;
   return 0;
}

void Cache::update(const string& path, const int64_t& ts, const int64_t& size, bool first, bool doEvict)
{
   CGuard sg(m_Lock);

   InfoBlockMap::iterator s = m_mOpenedFiles.find(path);

   if (s == m_mOpenedFiles.end())
   {
      InfoBlock r;
      r.m_iCount = 1;
      r.m_bChange = false;
      r.m_llTimeStamp = ts;
      r.m_llSize = size;
      r.m_llLastAccessTime = CTimer::getTime() / 1000000;
      m_mOpenedFiles[path] = r;
      return;
   }

   if ((s->second.m_llTimeStamp != ts) || (s->second.m_llSize != size))
   {
      s->second.m_bChange = true;
      s->second.m_llTimeStamp = ts;
      s->second.m_llSize = size;
      s->second.m_llLastAccessTime = CTimer::getTime() / 1000000;
      if( doEvict )
          evict(path);
   }

   // Increase reference count.
   if (first)
      s->second.m_iCount ++;
}

// Note: must be called with m_Lock held.
void Cache::evict(const string& path)
{
   m_mFileCache.erase( path );

   for( CacheBlockIter blk = m_lCacheBlocks.begin(); blk != m_lCacheBlocks.end(); )
   {
      if( (*blk)->m_strFile == path )
      {
         CacheBlockIter tmp = blk++;
         m_llCacheSize -= (*tmp)->m_llSize;
         --m_iBlockNum;
         m_lCacheBlocks.erase( tmp );
      }
      else
         ++blk;
   }
}

void Cache::remove(const string& path)
{
   CGuard sg(m_Lock);

   map<string, InfoBlock>::iterator s = m_mOpenedFiles.find(path);
   if (s == m_mOpenedFiles.end())
      return;

   // Remove the file information when its reference count becomes 0.
   if (-- s->second.m_iCount == 0)
      m_mOpenedFiles.erase(s);

   // Note that we do not remove the data cache even if the file is closed,
   // in case it may be opened again in the near future.
}

int Cache::stat(const string& path, SNode& attr)
{
   CGuard sg(m_Lock);

   map<string, InfoBlock>::iterator s = m_mOpenedFiles.find(path);

   if (s == m_mOpenedFiles.end())
      return -1;

   if (!s->second.m_bChange)
      return 0;

   attr.m_llTimeStamp = s->second.m_llTimeStamp;
   attr.m_llSize = s->second.m_llSize;
   return 1;
}

int Cache::insert(char* block, const string& path, const int64_t& offset, const int64_t& size, const bool& write)
{
   CGuard sg(m_Lock);

   InfoBlockMap::iterator s = m_mOpenedFiles.find(path);
   if (s == m_mOpenedFiles.end())
      return -1;
   s->second.m_llLastAccessTime = CTimer::getTime() / 1000000;

   int64_t first_block;
   int64_t block_num;
   parseIndexOffset(offset, size, first_block, block_num);

   FileCacheMap::iterator c = m_mFileCache.find(path);
   if (c == m_mFileCache.end())
   {
      // This is the first cache block for this file.
      m_mFileCache[path].clear();
      c = m_mFileCache.find(path);
   }

   CacheBlock* cb = new CacheBlock;
   cb->m_strFile = path;
   cb->m_llOffset = offset;
   cb->m_llSize = size;
   cb->m_llCreateTime = s->second.m_llLastAccessTime;
   cb->m_llLastAccessTime = s->second.m_llLastAccessTime;
   cb->m_pcBlock = block;
   cb->m_bWrite = write;

   // insert at the head of the list, newest block
   CacheBlockIter it = m_lCacheBlocks.insert(m_lCacheBlocks.begin(), cb);

   // Update per-file index. Update each offset index slot, and insert
   // the new block at the front. During read, the first block that covers
   // the requested area will be served, since it contains more recent data.
   for (int64_t i = first_block, n = first_block + block_num; i < n; ++ i)
      c->second[i].push_front(it);

   m_llCacheSize += cb->m_llSize;
   ++ m_iBlockNum;

   // check and remove old caches to limit memory usage
   shrink();

   return cb->m_llBlockID;;
}

int64_t Cache::read(const string& path, char* buf, const int64_t& offset, const int64_t& size)
{
   CGuard sg(m_Lock);

   InfoBlockMap::iterator s = m_mOpenedFiles.find(path);
   assert(s != m_mOpenedFiles.end());

   FileCacheMap::iterator c = m_mFileCache.find(path);
   if (c == m_mFileCache.end())
      return 0;

   int64_t first_block;
   int64_t block_num;
   parseIndexOffset(offset, size, first_block, block_num);

   BlockIndexMap::iterator slot = c->second.find(first_block);
   if (slot == c->second.end())
      return 0;

   // Scan every cache entry in this block
   CacheBlock* cb = NULL;
   CacheBlockIter it;
   for (BlockIndex::const_iterator block = slot->second.begin(); block != slot->second.end(); ++ block)
   {
      if (((**block)->m_llOffset <= offset) && (offset < (**block)->m_llOffset + (**block)->m_llSize))
      {
         // Once we find a block, the search is stopped. If the block does not have all the requested data,
         // partial data may be returned. We let the app to handle this situation (e.g., issue another
         // read with updated paramters).
         cb = **block;
         it = *block;
         break;
      }
   }

   if (NULL == cb)
      return 0;

   // calculate read size.
   int64_t readsize = size;
   if (cb->m_llOffset + cb->m_llSize < offset + size)
      readsize = cb->m_llOffset + cb->m_llSize - offset;

   // cache miss if cache content is smaller than read size
   if (readsize < size)
      return 0;

   memcpy(buf, cb->m_pcBlock + offset - cb->m_llOffset, readsize);

   // Update last access time.
   s->second.m_llLastAccessTime = CTimer::getTime();
   cb->m_llLastAccessTime = CTimer::getTime();

   // Update the block by moving it to the head of the cache list
   if (m_lCacheBlocks.size() > 1)
   {
      // Add the block to the head of list, since it is just accessed.
      CacheBlockIter new_it = m_lCacheBlocks.insert(m_lCacheBlocks.begin(), cb);

      // update per-file index first, otherwise the iterator will be changed.
      parseIndexOffset(cb->m_llOffset, cb->m_llSize, first_block, block_num);
      for (int i = first_block, n = first_block + block_num; i < n; ++ i)
      {
         for (BlockIndex::iterator block = c->second[i].begin(); block != c->second[i].end(); ++ block)
         {
            if (*block == it)
            {
               c->second[i].erase(block);
               c->second[i].push_front(new_it);
               break;
            }
         }
      }

      // Now remove the old position.
      m_lCacheBlocks.erase(it);
   }

   return readsize;
}

char* Cache::retrieve(const std::string& path, const int64_t& offset, const int64_t& size, const int64_t& id)
{
   CGuard sg(m_Lock);

   InfoBlockMap::const_iterator s = m_mOpenedFiles.find(path);
   if (s == m_mOpenedFiles.end())
      return NULL;

   FileCacheMap::iterator c = m_mFileCache.find(path);
   if (c == m_mFileCache.end())
      return NULL;

   int64_t first_block;
   int64_t block_num;
   parseIndexOffset(offset, size, first_block, block_num);

   // Retrieve cache line index.
   BlockIndexMap::iterator slot = c->second.find(first_block);
   if (slot == c->second.end())
      return NULL;

   for (BlockIndex::const_iterator block = slot->second.begin(); slot != c->second.end(); ++ block)
   {
      if ((**block)->m_llBlockID == id)
         return (**block)->m_pcBlock;
   }

   return NULL;
}

void Cache::shrink()
{
   while ((m_llCacheSize > m_llMaxCacheSize) || (m_iBlockNum > m_iMaxCacheBlocks))
   {
      if (m_lCacheBlocks.empty())
         return;

      // choose the last/oldest block to delete.
      CacheBlock* cb = m_lCacheBlocks.back();

      // cannot release write cache before it is flushed.
      if (cb->m_bWrite)
         return;

      releaseBlock(cb);
      m_lCacheBlocks.pop_back();
   }
}

int Cache::clearWrite(const string& path, const int64_t& offset, const int64_t& size, const int64_t& id)
{
   CGuard sg(m_Lock);

   InfoBlockMap::const_iterator s = m_mOpenedFiles.find(path);
   if (s == m_mOpenedFiles.end())
      return -1;

   FileCacheMap::iterator c = m_mFileCache.find(path);
   if (c == m_mFileCache.end())
      return 0;

   int64_t first_block;
   int64_t block_num;
   parseIndexOffset(offset, size, first_block, block_num);

   BlockIndexMap::iterator slot = c->second.find(first_block);
   assert(slot != c->second.end());

   // Update the first entry is enough, since they all point to the same physical block.
   for (BlockIndex::iterator block = slot->second.begin(); block != slot->second.end(); ++ block)
   {
      if ((**block)->m_llBlockID == id)
      {
         (**block)->m_bWrite = false;
         break;
      }
   }

   // Cache may not be reduced by other operations if app does write only, so we try to reduce cache block here.
   shrink();

   return 0;
}

void Cache::parseIndexOffset(const int64_t& offset, const int64_t& size, int64_t& index_off, int64_t& block_num)
{
   index_off = offset / m_iBlockUnitSize;
   block_num = (offset + size) / m_iBlockUnitSize - index_off + 1;
}

void Cache::releaseBlock(CacheBlock* cb)
{
   int64_t first_block;
   int64_t block_num;
   parseIndexOffset(cb->m_llOffset, cb->m_llSize, first_block, block_num);

   FileCacheMap::iterator c = m_mFileCache.find(cb->m_strFile);
   assert(c != m_mFileCache.end());

   for (int i = first_block, n = first_block + block_num; i < n; ++ i)
   {
      for (BlockIndex::iterator block = c->second[i].begin(); block != c->second[i].end(); ++ block)
      {
         if ((**block)->m_llBlockID == cb->m_llBlockID)
         {
            c->second[i].erase(block);
            break;
         }
      }
      if (c->second[i].empty())
         c->second.erase(i);
   }
   if (c->second.empty())
      m_mFileCache.erase(c);

   m_llCacheSize -= cb->m_llSize;
   m_iBlockNum --;

   delete [] cb->m_pcBlock;
   delete cb;
}
