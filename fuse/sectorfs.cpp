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
   Yunhong Gu, last updated 03/21/2011
*****************************************************************************/

#include <fstream>
#include <iostream>
#include <ctime>

#include "common.h"
#include "sectorfs.h"
#include "fusedircache.h"
#include "../common/log.h"

using namespace std;

Sector SectorFS::g_SectorClient;
Session SectorFS::g_SectorConfig;
map<string, FileTracker*> SectorFS::m_mOpenFileList;
pthread_mutex_t SectorFS::m_OpenFileLock = PTHREAD_MUTEX_INITIALIZER;
bool SectorFS::g_bConnected = false;
pthread_mutex_t SectorFS::m_reinitLock = PTHREAD_MUTEX_INITIALIZER;

time_t SectorFS::g_iDfTs = 0;
time_t SectorFS::g_iDfTimeout=600;
bool SectorFS::g_bDfBeingEvaluated = false;
int64_t SectorFS::g_iDfUsedSpace = 0;
int64_t SectorFS::g_iDfAvailSpace = 0;
CMutex SectorFS::g_DfLock;

namespace
{
   inline logger::LogAggregate& log()
   {
      static logger::LogAggregate& myLogger = logger::getLogger( "SectorFS" );
      static bool                  setLogLevelYet = false;

      if( !setLogLevelYet )
      {
         setLogLevelYet = true;
         myLogger.setLogLevel( logger::Debug );
      }

      return myLogger;
   }

   std::string parentDirOf( std::string path )
   {
      std::string parent = path;
      if( parent[ parent.length() - 1 ] == '/' )
         parent = parent.substr( 0, parent.length() - 2 );
      parent = parent.substr( 0, parent.rfind( '/' ) );
      return parent;
   }
}


#define CONN_CHECK( fn ) \
{\
   if (!g_bConnected) \
      restart();\
   if (!g_bConnected) {\
      log().error << __PRETTY_FUNCTION__ << " exited, rc = -1, due to failure to connect to master!" << std::endl; \
      return -1;\
   }\
}


void* SectorFS::init(struct fuse_conn_info * /*conn*/)
{
   const ClientConf& conf = g_SectorConfig.m_ClientConf;

   logger::config( "/tmp", "sector-fuse" );
   log().setLogLevel( logger::Debug );
   log().trace << __PRETTY_FUNCTION__ << " entered" << std::endl;

   g_SectorClient.init();
   g_SectorClient.configLog(conf.m_strLog.c_str(), false, conf.m_iLogLevel);
   g_SectorClient.setMaxCacheSize(conf.m_llMaxCacheSize);

   bool master_conn = false;
   for (set<Address, AddrComp>::const_iterator i = conf.m_sMasterAddr.begin(); i != conf.m_sMasterAddr.end(); ++ i)
   {
      log().debug << "Logging into master at " << i->m_strIP << ':' << i->m_iPort
        << ", user=" << conf.m_strUserName << ", password=" << conf.m_strPassword << ", cert=" << conf.m_strCertificate
        << std::endl;

      if (g_SectorClient.login(i->m_strIP, i->m_iPort, conf.m_strUserName,
                               conf.m_strPassword, conf.m_strCertificate.c_str()) >= 0)
      {
         log().debug << "Login to master successful!" << std::endl;
         master_conn = true;
         break;
      }
   }

   if (!master_conn)
   {
      log().debug << "Failed to login to any master!" << std::endl;
      log().trace << __PRETTY_FUNCTION__ << " exited" << std::endl;
      g_SectorClient.close();
      return NULL;
   }

   g_bConnected = true;
   DirCache::instance().init_root( g_SectorClient );
   DirCache::clear();

   log().trace << "Directory cache lifetimes:" << std::endl;
   for( ClientConf::CacheLifetimes::const_iterator i = conf.m_pathCache.begin(); i != conf.m_pathCache.end(); ++i )
      log().trace << "   " << i->m_sPathMask << " => " << i->m_seconds << " secs" << std::endl;
   log().trace << __PRETTY_FUNCTION__ << " exited" << std::endl;
   return NULL;
}

void SectorFS::destroy(void *)
{
   log().trace << __PRETTY_FUNCTION__ << " entered" << std::endl;
   g_SectorClient.logout();
   g_SectorClient.close();
   DirCache::destroy();
   log().trace << __PRETTY_FUNCTION__ << " exited" << std::endl;
}

int SectorFS::getattr(const char* path, struct stat* st)
{
//   log().trace << __PRETTY_FUNCTION__ << " entered" << std::endl
//     << " Path = " << path << std::endl;

   CONN_CHECK( path );

   SNode s;
   int rv = DirCache::instance().get(path, g_SectorClient, s);
   if( rv < 0 )
   {
      checkConnection(rv);
      log().trace << __PRETTY_FUNCTION__ << " exited (err=" << rv
         << ", rc = " << translateErr(rv) << ')' << std::endl;
      return translateErr(rv);
   }

   if( rv == 1 )
   {
      int r = g_SectorClient.stat(path, s);
      if (r < 0)
      {        
         DirCache::clearLastUnresolvedStat();
         checkConnection(r);         
         log().trace << __PRETTY_FUNCTION__ << " exited (err=" << r
            << ", rc = " << translateErr(r) << ')' << std::endl;
         return translateErr(r);
      }
   }

   if (s.m_bIsDir)
      st->st_mode = S_IFDIR | 0755;
   else
      st->st_mode = S_IFREG | 0755;
   st->st_nlink = 1;
   st->st_uid = 0;
   st->st_gid = 0;
   st->st_size = s.m_llSize;
   st->st_blksize = g_iBlockSize;
   st->st_blocks = st->st_size / st->st_blksize;
   if ((st->st_size % st->st_blksize) != 0)
      ++ st->st_blocks;
   st->st_atime = st->st_mtime = st->st_ctime = s.m_llTimeStamp;

   if (st->st_size == 0) DirCache::clearLastUnresolvedStat();
//   log().trace << __PRETTY_FUNCTION__ << " exited, rc = 0" << std::endl;
   return 0;
}

int SectorFS::fgetattr(const char* path, struct stat* st , struct fuse_file_info *)
{
//   log().trace << __PRETTY_FUNCTION__ << " entered" << std::endl
//      << " Path = " << path << std::endl;
   int rc = getattr(path, st);
//   log().trace << __PRETTY_FUNCTION__ << " exited, rc = " << rc << std::endl;
   return rc;
}

int SectorFS::mknod(const char *, mode_t, dev_t)
{
   log().trace << __PRETTY_FUNCTION__ << " entered" << std::endl;
   log().warning << "STUB FUNCTION " << __PRETTY_FUNCTION__ << " CALLED" << std::endl;
   log().trace << __PRETTY_FUNCTION__ << " exited, rc = 0" << std::endl;
   return 0;
}

int SectorFS::mkdir(const char* path, mode_t mode )
{
   log().trace << __PRETTY_FUNCTION__ << " entered" << std::endl
      << " Path = " << path << ", mode = " << std::oct << mode << std::dec << std::endl;

   log().warning << "mode parameter is ignored" << std::endl;

   CONN_CHECK( path );

   DirCache::clear( parentDirOf( path ) );

   int r = g_SectorClient.mkdir(path);
   if (r < 0)
   {
      checkConnection(r);
      log().trace << __PRETTY_FUNCTION__ << " exited (err=" << r
        << ", rc=" << translateErr(r) << ')' << std::endl;
      return translateErr(r);
   }

   DirCache::clear( parentDirOf( path ) );

   log().trace << __PRETTY_FUNCTION__ << " exited, rc = 0" << std::endl;
   return 0;
}

int SectorFS::unlink(const char* path)
{
   log().trace << __PRETTY_FUNCTION__ << " entered" << std::endl
      << " Path = " << path << std::endl;

   CONN_CHECK( path );

   DirCache::clear( parentDirOf( path ) );

   // If the file has been opened, close it first.
   if (lookup(path) != NULL)
      release(path, NULL);

   int r = g_SectorClient.remove(path);
   if (r < 0)
   {
      checkConnection(r);
      log().trace << __PRETTY_FUNCTION__ << " exited (err=" << r
        << ", rc=-1)" << std::endl;
      return -1;
   }

   DirCache::clear( parentDirOf( path ) );

   log().trace << __PRETTY_FUNCTION__ << " exited, rc = 0" << std::endl;
   return 0;
}

int SectorFS::rmdir(const char* path)
{
   log().trace << __PRETTY_FUNCTION__ << " entered" << std::endl
      << " Path = " << path << std::endl;

   CONN_CHECK( path );

   DirCache::clear( path );
   DirCache::clear( parentDirOf( path ) );

   int r = g_SectorClient.remove(path);
   if (r < 0)
   {
      checkConnection(r);
      log().trace << __PRETTY_FUNCTION__ << " exited (err=" << r
        << ", rc=-1)" << std::endl;
      return -1;
   }

   DirCache::clear( path );
   DirCache::clear( parentDirOf( path ) );

   log().trace << __PRETTY_FUNCTION__ << " exited, rc = 0" << std::endl;
   return 0;
}

int SectorFS::rename(const char* src, const char* dst)
{
   log().trace << __PRETTY_FUNCTION__ << " entered" << std::endl
      << " from = " << src << ", to = " << dst << std::endl;

   CONN_CHECK( src << ' ' << dst );

   DirCache::clear_recursive( parentDirOf( src ) );
   DirCache::clear( dst );
   DirCache::clear( parentDirOf( dst ) );

   // If the file has been opened, close it first.
   if (lookup(src) != NULL)
      release(src, NULL);

   int r = g_SectorClient.move(src, dst);
   if (r < 0)
   {
      checkConnection(r);
      log().trace << __PRETTY_FUNCTION__ << " exited (err=" << r
        << ", rc=" << translateErr(r) << ')' << std::endl;
      return translateErr(r);
   }

   DirCache::clear_recursive( parentDirOf( src ) );
   DirCache::clear( dst );
   DirCache::clear( parentDirOf( dst ) );

   log().trace << __PRETTY_FUNCTION__ << " exited, rc = 0" << std::endl;
   return 0;
}

int SectorFS::statfs(const char* path, struct statvfs* buf)
{
//   log().trace << __PRETTY_FUNCTION__ << " entered" << std::endl
//      << "path = " << path << std::endl;

//   log().warning << "path parameter is ignored" << std::endl;

   CONN_CHECK( "df" );

   time_t tsNow = time(0);
   int64_t usedSpace;
   int64_t availSpace;
   g_DfLock.acquire();
   if (g_iDfTs < tsNow)
   {
      if (g_bDfBeingEvaluated)
      {
         usedSpace = g_iDfUsedSpace;
         availSpace = g_iDfAvailSpace;
         g_DfLock.release();
      } else
      {
         g_bDfBeingEvaluated = true;
         g_DfLock.release();

         int r = g_SectorClient.df(availSpace, usedSpace);
         if (r < 0)
         {
            checkConnection(r);
            log().trace << __PRETTY_FUNCTION__ << " exited (err=" << r
              << ", rc=" << translateErr(r) << ')' << std::endl;
            return translateErr(r);
         }

         g_DfLock.acquire();
         g_bDfBeingEvaluated = false;
         g_iDfUsedSpace = usedSpace;
         g_iDfAvailSpace = availSpace;
         if (availSpace == 0)
         { // Sector still starting - no slaves - smaller timeout
           g_iDfTs = tsNow + 2;
         } else
         {
           g_iDfTs = tsNow + g_iDfTimeout;
         }
         g_DfLock.release();
         log().trace << "Df recaching: used space " << usedSpace << " avail space " << availSpace
             << " will expire in " << g_iDfTimeout << " sec" << std::endl;
         }
      } else
      {
         usedSpace = g_iDfUsedSpace;
         availSpace = g_iDfAvailSpace;
         g_DfLock.release();
      }

//   log().trace << "Df" << std::endl;

   buf->f_namemax = 256;
   buf->f_bsize = g_iBlockSize;
   buf->f_frsize = buf->f_bsize;
   buf->f_blocks = (availSpace + usedSpace) / buf->f_bsize;
   buf->f_bfree = buf->f_bavail = availSpace / buf->f_bsize;
   buf->f_files = 0;
   buf->f_ffree = 0xFFFFFFFFULL;

//   log().trace << __PRETTY_FUNCTION__ << " exited, rc = 0" << std::endl;
   return 0;
}

int SectorFS::utime(const char* path, struct utimbuf* ubuf)
{
//   log().trace << __PRETTY_FUNCTION__ << " entered" << std::endl
//      << " path = " << path << ", actime = " << ubuf->actime << ", modtime = " << ubuf->modtime << std::endl;

   CONN_CHECK( path );

   DirCache::clear( parentDirOf( path ) );

   int r = g_SectorClient.utime(path, ubuf->modtime);
   if (r < 0)
   {
      checkConnection(r);
      log().trace << __PRETTY_FUNCTION__ << " exited (err=" << r
        << ", rc=" << translateErr(r) << ')' << std::endl;
      return translateErr(r);
   }

   DirCache::clear( parentDirOf( path ) );

//   log().trace << __PRETTY_FUNCTION__ << " exited, rc = 0" << std::endl;
   return 0;
}

int SectorFS::utimens(const char* path, const struct timespec tv[2])
{
   log().trace << __PRETTY_FUNCTION__ << " entered" << std::endl
      << " path = " << path << std::endl;

   CONN_CHECK( path );

   DirCache::clear( parentDirOf( path ) );

   int r = g_SectorClient.utime(path, tv[1].tv_sec);
   if (r < 0)
   {
      checkConnection(r);
      log().trace << __PRETTY_FUNCTION__ << " exited (err=" << r
        << ", rc=" << translateErr(r) << ')' << std::endl;
      return translateErr(r);
   }

   DirCache::clear( parentDirOf( path ) );

   log().trace << __PRETTY_FUNCTION__ << " exited, rc = 0" << std::endl;
   return 0;
}

int SectorFS::opendir(const char * path, struct fuse_file_info *)
{
   log().trace << __PRETTY_FUNCTION__ << " entered" << std::endl
      << " path = " << path << std::endl;
   log().warning << "STUB FUNCTION " << __PRETTY_FUNCTION__ << " CALLED" << std::endl;
   log().trace << __PRETTY_FUNCTION__ << " exited, rc = 0" << std::endl;
   return 0;
}

int SectorFS::readdir(const char* path, void* buf, fuse_fill_dir_t filler, off_t offset, struct fuse_file_info* /*info*/)
{
//   log().trace << __PRETTY_FUNCTION__ << " entered" << std::endl
//      << " path = " << path << ", offset = " << offset << std::endl;  

//   log().warning << "offset parameter is ignored" << std::endl;

   CONN_CHECK( path );
   vector<SNode> filelist;

   int r = 0;
   bool foundInCache = DirCache::instance().readdir( path, filelist ) == 0;
   if( !foundInCache )
       r = g_SectorClient.list(path, filelist,false);
   if (r < 0)
   {
      checkConnection(r);
      log().trace << __PRETTY_FUNCTION__ << " exited (err=" << r
        << ", rc=" << translateErr(r) << ')' << std::endl;
      return translateErr(r);
   }

   if( !foundInCache )
       DirCache::instance().add(path, filelist);
   for (vector<SNode>::iterator i = filelist.begin(); i != filelist.end(); ++ i)
   {
      struct stat st;
      if (i->m_bIsDir)
         st.st_mode = S_IFDIR | 0755;
      else
         st.st_mode = S_IFREG | 0755;
      st.st_nlink = 1;
      st.st_uid = 0;
      st.st_gid = 0;
      st.st_size = i->m_llSize;
      st.st_blksize = g_iBlockSize;
      st.st_blocks = st.st_size / st.st_blksize;
      if ((st.st_size % st.st_blksize) != 0)
         ++ st.st_blocks;
      st.st_atime = st.st_mtime = st.st_ctime = i->m_llTimeStamp;

      filler(buf, i->m_strName.c_str(), &st, 0);
   }

   struct stat st;
   st.st_mode = S_IFDIR | 0755;
   st.st_nlink = 1;
   st.st_uid = 0;
   st.st_gid = 0;
   st.st_size = 4096;
   st.st_blksize = g_iBlockSize;
   st.st_blocks = st.st_size / st.st_blksize;
   if ((st.st_size % st.st_blksize) != 0)
      ++ st.st_blocks;
   st.st_atime = st.st_mtime = st.st_ctime = 0;
   filler(buf, ".", &st, 0);
   filler(buf, "..", &st, 0);

//   log().trace << __PRETTY_FUNCTION__ << " exited, rc = 0" << std::endl;
   return 0;
}

int SectorFS::fsyncdir(const char *, int, struct fuse_file_info *)
{
   log().trace << __PRETTY_FUNCTION__ << " entered" << std::endl;
   log().warning << "STUB FUNCTION " << __PRETTY_FUNCTION__ << " CALLED" << std::endl;
   log().trace << __PRETTY_FUNCTION__ << " exited, rc = 0" << std::endl;
   return 0;
}

int SectorFS::releasedir(const char *, struct fuse_file_info *)
{
   log().trace << __PRETTY_FUNCTION__ << " entered" << std::endl;
   log().warning << "STUB FUNCTION " << __PRETTY_FUNCTION__ << " CALLED" << std::endl;
   log().trace << __PRETTY_FUNCTION__ << " exited, rc = 0" << std::endl;
   return 0;
}

int SectorFS::chmod(const char *, mode_t)
{
   log().trace << __PRETTY_FUNCTION__ << " entered" << std::endl;
   log().warning << "STUB FUNCTION " << __PRETTY_FUNCTION__ << " CALLED" << std::endl;
   log().trace << __PRETTY_FUNCTION__ << " exited, rc = 0" << std::endl;
   return 0;
}

int SectorFS::chown(const char *, uid_t, gid_t)
{
   log().trace << __PRETTY_FUNCTION__ << " entered" << std::endl;
   log().warning << "STUB FUNCTION " << __PRETTY_FUNCTION__ << " CALLED" << std::endl;
   log().trace << __PRETTY_FUNCTION__ << " exited, rc = 0" << std::endl;
   return 0;
}

int SectorFS::create(const char* path, mode_t mode, struct fuse_file_info* info)
{
   log().trace << __PRETTY_FUNCTION__ << " entered" << std::endl
      << " Path = " << path << ", mode = " << std::oct << mode << std::dec << std::endl;

   log().warning << "mode parameter is ignored" << std::endl;

   CONN_CHECK( path );

   DirCache::clear( parentDirOf( path ) );

   SNode s;
   if (g_SectorClient.stat(path, s) < 0)
   {
      fuse_file_info option;
      option.flags = O_WRONLY;
      open(path, &option);
      release(path, &option);
   }

   int r = open(path, info);
   if (r < 0)
   {
      log().trace << __PRETTY_FUNCTION__ << " exited (err=" << r
        << ", rc=-1)" << std::endl;
      return -1;
   }

   DirCache::clear( parentDirOf( path ) );

   log().trace << __PRETTY_FUNCTION__ << " exited, rc = " << r << std::endl;
   return r;
}

int SectorFS::truncate(const char* path, off_t size )
{
   log().trace << __PRETTY_FUNCTION__ << " entered" << std::endl
      << " Path = " << path << ", size = " << size << std::endl;

   CONN_CHECK( path );

   DirCache::clear( parentDirOf( path ) );

   // Otherwise open the file and perform trunc.
   fuse_file_info option;
   option.flags = O_WRONLY | O_TRUNC;
   open(path, &option);
   release(path, &option);

   DirCache::clear( parentDirOf( path ) );

   log().trace << __PRETTY_FUNCTION__ << " exited, rc = 0" << std::endl;
   return 0;
}

int SectorFS::ftruncate(const char* path, off_t offset, struct fuse_file_info *)
{
   log().trace << __PRETTY_FUNCTION__ << " entered" << std::endl
      << " Path = " << path << ", offset = " << offset << std::endl;
   int rc = truncate(path, offset);
   log().trace << __PRETTY_FUNCTION__ << " exited, rc = " << rc << std::endl;
   return rc;
}

int SectorFS::open(const char* path, struct fuse_file_info* fi)
{
   log().trace << __PRETTY_FUNCTION__ << " entered" << std::endl
      << " Path = " << path << std::endl;

   CONN_CHECK( path );

   // TODO: file option should be checked.
   bool owner = false;
   FileTracker* ft = NULL;

   FileTracker::State state = FileTracker::NEXIST;
   pthread_mutex_lock(&m_OpenFileLock);
   map<string, FileTracker*>::iterator t = m_mOpenFileList.find(path);
   if (t != m_mOpenFileList.end())
   {
      state = t->second->m_State;
      t->second->m_iCount ++;
      if (FileTracker::OPEN == state)
      {
         pthread_mutex_unlock(&m_OpenFileLock);
         log().trace << __PRETTY_FUNCTION__ << " exited, rc = 0" << std::endl;
         return 0;
      }
      else
      {
         ft = t->second;
      }
   }
   else
   {
      ft = new FileTracker;
      ft->m_strName = path;
      ft->m_iCount = 1;
      ft->m_State = FileTracker::OPENING;
      ft->m_pHandle = NULL;
      ft->m_bModified = false;
      m_mOpenFileList[path] = ft;
      owner = true;
   }
   pthread_mutex_unlock(&m_OpenFileLock);

   if (!owner)
   {
      // A spin loop to wait for the file to be opened/closed by another thread.
      // TODO: add signal support. This should happen rarely anyway.
      while (true)
      {
         { // mutex proteced block.
            CGuard fl(m_OpenFileLock);
            if ((FileTracker::CLOSING != ft->m_State) && (FileTracker::OPENING != ft->m_State))
            {
               // Another thread has opened the file, no need to open again.
               if (FileTracker::OPEN == ft->m_State)
               {
                  log().trace << __PRETTY_FUNCTION__ << " exited, rc = 0" << std::endl;
                  return 0;
               }

               if (FileTracker::CLOSED == ft->m_State)
                  ft->m_State = FileTracker::OPENING;

               break;
            }
         } // end of mutex protection.

         usleep(1);
      }
   }

   // Others are waiting for this thread to open the file.
   SectorFile* f = g_SectorClient.createSectorFile();

   int permission = SF_MODE::READ;
   if (fi->flags & O_WRONLY)
      permission = SF_MODE::WRITE;
   else if (fi->flags & O_RDWR)
      permission = SF_MODE::READ | SF_MODE::WRITE;

   if (fi->flags & O_TRUNC)
      permission |= SF_MODE::TRUNC;

   if (fi->flags & O_APPEND)
      permission |= SF_MODE::APPEND;

   if( permission & ( SF_MODE::WRITE | SF_MODE::TRUNC | SF_MODE::APPEND ) )
      DirCache::clear( parentDirOf( path ) );

   int r = f->open(path, permission);
   if (r < 0)
   {
      pthread_mutex_lock(&m_OpenFileLock);
      ft->m_State = FileTracker::CLOSED;
      ft->m_iCount --;
      if (0 == ft->m_iCount)
      {
         m_mOpenFileList.erase(path);
         delete ft;
      }
      pthread_mutex_unlock(&m_OpenFileLock);

      log().trace << __PRETTY_FUNCTION__ << " exited (err=" << r
        << ", rc=-1)" << std::endl;
      return -1;
   }

   pthread_mutex_lock(&m_OpenFileLock);
   ft->m_pHandle = f;
   ft->m_State = FileTracker::OPEN;
   pthread_mutex_unlock(&m_OpenFileLock);

   if( permission & ( SF_MODE::WRITE | SF_MODE::TRUNC | SF_MODE::APPEND ) )
      DirCache::clear( parentDirOf( path ) );

   log().trace << __PRETTY_FUNCTION__ << " exited, rc = 0" << std::endl;
   return 0;
}

int SectorFS::read(const char* path, char* buf, size_t size, off_t offset, struct fuse_file_info* /*info*/)
{
//   log().trace << __PRETTY_FUNCTION__ << " entered" << std::endl
//      << " Path = " << path << ", size = " << size << ", offset = " << offset << std::endl;

   CONN_CHECK( path );

   SectorFile* h = lookup(path);
   if (NULL == h) {
      log().trace << __PRETTY_FUNCTION__ << " exited, rc = " << -EBADF << std::endl;
      return -EBADF;
   }

   // FUSE read buffer is too small; we use prefetch buffer to improve read performance
   int r = h->read(buf, offset, size, g_SectorConfig.m_ClientConf.m_iFuseReadAheadBlock);
   if (r < 0) {
     log().trace << __PRETTY_FUNCTION__ << " exited, (err=" << r << ", rc=-1)" << std::endl;
     return -1;
   }
   if (r == 0) {
      r = h->read(buf, offset, size, g_SectorConfig.m_ClientConf.m_iFuseReadAheadBlock);
      if (r < 0) {
         log().trace << __PRETTY_FUNCTION__ << " exited, (err=" << r << ", rc=-1)" << std::endl;
         return -1;
      } 
   }

//   log().trace << __PRETTY_FUNCTION__ << " exited, rc = " << r << std::endl;
   return r;
}

int SectorFS::write(const char* path, const char* buf, size_t size, off_t offset, struct fuse_file_info* /*info*/)
{
//   log().trace << __PRETTY_FUNCTION__ << " entered" << std::endl
//      << " Path = " << path << ", size = " << size << ", offset = " << offset << std::endl;

   CONN_CHECK( path );

   pthread_mutex_lock(&m_OpenFileLock);
   map<string, FileTracker*>::iterator t = m_mOpenFileList.find(path);
   if( t == m_mOpenFileList.end() ) 
   {
      pthread_mutex_unlock( &m_OpenFileLock );
      log().error << __PRETTY_FUNCTION__ << " failed to find file " << path << std::endl;
      log().trace << __PRETTY_FUNCTION__ << " exited, rc = " << -EBADF << std::endl;
      return -EBADF;
   }

   SectorFile* h = t->second->m_pHandle;
   if (NULL == h) {
      pthread_mutex_unlock( &m_OpenFileLock );
      log().trace << __PRETTY_FUNCTION__ << " exited, rc = " << -EBADF << std::endl;
      return -EBADF;
   }

   t->second->m_bModified = true;
   pthread_mutex_unlock( &m_OpenFileLock );

   int r = h->write(buf, offset, size);
   if (r < 0) {
      log().trace << __PRETTY_FUNCTION__ << " exited, (err=" << r << ", rc=-1)" << std::endl;
      return -1;
   }

//   log().trace << __PRETTY_FUNCTION__ << " exited, rc = " << r << std::endl;
   return r;
}

int SectorFS::flush (const char *, struct fuse_file_info *)
{
//   log().trace << __PRETTY_FUNCTION__ << " entered" << std::endl;
//   log().warning << "STUB FUNCTION " << __PRETTY_FUNCTION__ << " CALLED" << std::endl;
//   log().trace << __PRETTY_FUNCTION__ << " exited, rc = 0" << std::endl;
   return 0;
}

int SectorFS::fsync(const char *, int, struct fuse_file_info *)
{
   log().trace << __PRETTY_FUNCTION__ << " entered" << std::endl;
   log().warning << "STUB FUNCTION " << __PRETTY_FUNCTION__ << " CALLED" << std::endl;
   log().trace << __PRETTY_FUNCTION__ << " exited, rc = 0" << std::endl;
   return 0;
}

int SectorFS::release(const char* path, struct fuse_file_info* /*info*/)
{
   log().trace << __PRETTY_FUNCTION__ << " entered" << std::endl
      << " Path = " << path << std::endl;

   pthread_mutex_lock(&m_OpenFileLock);
   map<string, FileTracker*>::iterator t = m_mOpenFileList.find(path);
   if ((t == m_mOpenFileList.end()) || (FileTracker::OPEN != t->second->m_State))
   {
      pthread_mutex_unlock(&m_OpenFileLock);
      log().trace << __PRETTY_FUNCTION__ << " exited, rc = " << -EBADF << std::endl;
      return -EBADF;
   }

   if( t->second->m_bModified )
      DirCache::clear( parentDirOf( path ) );

   if (t->second->m_iCount > 1)
   {
      t->second->m_iCount --;
      pthread_mutex_unlock(&m_OpenFileLock);
      log().trace << __PRETTY_FUNCTION__ << " exited, rc = 0" << std::endl;
      return 0;
   }
   FileTracker* ft = t->second;
   ft->m_State = FileTracker::CLOSING;
   pthread_mutex_unlock(&m_OpenFileLock);
   // File close/release may take some time, so do it out of mutex protection.
   ft->m_pHandle->close();
   g_SectorClient.releaseSectorFile(ft->m_pHandle);

   if( t->second->m_bModified )
      DirCache::clear( parentDirOf( path ) );

   pthread_mutex_lock(&m_OpenFileLock);
   ft->m_pHandle = NULL;
   ft->m_State = FileTracker::CLOSED;
   t->second->m_iCount --;
   if (t->second->m_iCount == 0)
   {
      delete ft;
      m_mOpenFileList.erase(t);
   }
   pthread_mutex_unlock(&m_OpenFileLock);

//   log().trace << __PRETTY_FUNCTION__ << " exited, rc = 0" << std::endl;
   return 0;
}

int SectorFS::access(const char * /*path*/, int /*mode*/)
{
//   log().trace << __PRETTY_FUNCTION__ << " entered" << std::endl
//    << " path = " << path << ", mode = " << std::oct << mode << std::dec << std::endl;
//   log().warning << "STUB FUNCTION " << __PRETTY_FUNCTION__ << " CALLED" << std::endl;
//   log().trace << __PRETTY_FUNCTION__ << " exited, rc = 0" << std::endl;
   return 0;
}

int SectorFS::lock(const char *, struct fuse_file_info *, int, struct flock *)
{
   log().trace << __PRETTY_FUNCTION__ << " entered" << std::endl;
   log().warning << "STUB FUNCTION " << __PRETTY_FUNCTION__ << " CALLED" << std::endl;
   log().trace << __PRETTY_FUNCTION__ << " exited, rc = 0" << std::endl;
   return 0;
}

int SectorFS::translateErr(int err)
{
   switch (err)
   {
   case SectorError::E_PERMISSION:
      return -EACCES;

   case SectorError::E_NOEXIST:
      return -ENOENT;

   case SectorError::E_EXIST:
      return -EEXIST;

   case SectorError::E_MASTER:
      return -EHOSTDOWN;

   case SectorError::E_CONNECTION:
      return -EHOSTDOWN;

   case SectorError::E_BUSY:
      return -EBUSY;
   }

   return -1;
}

int SectorFS::restart()
{
   log().trace << __PRETTY_FUNCTION__ << " entered" << std::endl;

   if (g_bConnected)
   {
      log().trace << __PRETTY_FUNCTION__ << " exited, rc = 0" << std::endl;
      return 0;
   }

   // CGuard lock( m_reinitLock );  FIXME: uncomment this later

   g_SectorClient.logout();
   g_SectorClient.close();
   init(NULL);
   log().trace << __PRETTY_FUNCTION__ << " exited, rc = 0" << std::endl;
   return 0;
}

void SectorFS::checkConnection(int res)
{
   if ((res == SectorError::E_MASTER) || (res == SectorError::E_EXPIRED))
   {
      log().debug << __PRETTY_FUNCTION__ << " err code " << res
        << " indicates no longer connected to master!" << std::endl;
      g_bConnected = false;
   }
}

SectorFile* SectorFS::lookup(const string& path)
{
//   log().trace << __PRETTY_FUNCTION__ << " entered" << std::endl
//    << " path = " << path << std::endl;

   pthread_mutex_lock(&m_OpenFileLock);
   map<string, FileTracker*>::iterator t = m_mOpenFileList.find(path);
   if (t == m_mOpenFileList.end())
   {
      pthread_mutex_unlock(&m_OpenFileLock);
      log().trace << __PRETTY_FUNCTION__ << " exited, rc = NULL" << std::endl;
      return NULL;
   }
   SectorFile* h = t->second->m_pHandle;
   pthread_mutex_unlock(&m_OpenFileLock);

//   log().trace << __PRETTY_FUNCTION__ << " exited, rc = " << std::hex << h << std::dec << std::endl;
   return h;
}
