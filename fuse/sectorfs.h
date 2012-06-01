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
   Yunhong Gu, last updated 04/01/2011
*****************************************************************************/


#ifndef __SECTOR_FS_FUSE_H__
#define __SECTOR_FS_FUSE_H__

#define FUSE_USE_VERSION 26

#include <dirent.h>
#include <errno.h>
#include <fcntl.h>
#include <fuse.h>
#include <fuse/fuse_opt.h>
#include <map>
#include <stdio.h>
#include <string>
#include <string.h>
#include <unistd.h>
#include <time.h>

#include "sector.h"
#include "osportable.h"

struct FileTracker
{
   enum State {NEXIST = 1, OPENING, OPEN, CLOSING, CLOSED};

   std::string m_strName;
   int m_iCount;
   State m_State;
   SectorFile* m_pHandle;
   bool m_bModified;
};

class SectorFS
{
public:
   static void* init(struct fuse_conn_info *conn);
   static void destroy(void *);

   static int getattr(const char *, struct stat *);
   static int fgetattr(const char *, struct stat *, struct fuse_file_info *);
   static int mknod(const char *, mode_t, dev_t);
   static int mkdir(const char *, mode_t);
   static int unlink(const char *);
   static int rmdir(const char *);
   static int rename(const char *, const char *);
   static int statfs(const char *, struct statvfs *);
   static int utime(const char *, struct utimbuf *);
   static int utimens(const char *, const struct timespec tv[2]);
   static int opendir(const char *, struct fuse_file_info *);
   static int readdir(const char *, void *, fuse_fill_dir_t, off_t, struct fuse_file_info *);
   static int releasedir(const char *, struct fuse_file_info *);
   static int fsyncdir(const char *, int, struct fuse_file_info *);

   //static int readlink(const char *, char *, size_t);
   //static int symlink(const char *, const char *);
   //static int link(const char *, const char *);

   static int chmod(const char *, mode_t);
   static int chown(const char *, uid_t, gid_t);

   static int create(const char *, mode_t, struct fuse_file_info *);
   static int truncate(const char *, off_t);
   static int ftruncate(const char *, off_t, struct fuse_file_info *);
   static int open(const char *, struct fuse_file_info *);
   static int read(const char *, char *, size_t, off_t, struct fuse_file_info *);
   static int write(const char *, const char *, size_t, off_t, struct fuse_file_info *);
   static int flush(const char *, struct fuse_file_info *);
   static int fsync(const char *, int, struct fuse_file_info *);
   static int release(const char *, struct fuse_file_info *);

   //static int setxattr(const char *, const char *, const char *, size_t, int);
   //static int getxattr(const char *, const char *, char *, size_t);
   //static int listxattr(const char *, char *, size_t);
   //static int removexattr(const char *, const char *);

   static int access(const char *, int);

   static int lock(const char *, struct fuse_file_info *, int cmd, struct flock *);

public:
   static Sector g_SectorClient;
   static Session g_SectorConfig;

// debug variable for duration calculation
//   static suseconds_t ts_pr;

private:
   static std::map<std::string, FileTracker*> m_mOpenFileList;
   static pthread_mutex_t m_OpenFileLock;
   static pthread_mutex_t m_reinitLock;

private:
   static int translateErr(int err);
   static int restart();
   static void checkConnection(int res);
   static SectorFile* lookup(const std::string& path);

private:
   static const int g_iBlockSize = 512;
   static bool g_bConnected;

private:
   static CMutex g_DfLock;
   static time_t g_iDfTs;
   static time_t g_iDfTimeout;
   static bool g_bDfBeingEvaluated;
   static int64_t g_iDfUsedSpace;
   static int64_t g_iDfAvailSpace;

};

#endif
