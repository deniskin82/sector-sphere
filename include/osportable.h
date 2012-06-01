/*****************************************************************************
Copyright 2010, 2011 Sergio Ruiz.

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
   Sergio Ruiz, last updated 10/03/2010
updated by
   Yunhong Gu, last updated 03/16/2011
*****************************************************************************/

#ifndef __OS_PORTABLE_H__
#define __OS_PORTABLE_H__

#ifndef WIN32
   #include <errno.h>
   #include <pthread.h>
   #include <sys/time.h>
   #include <sys/uio.h>
#else
   #include <windows.h>
#endif
#include <cstdlib>
#include <string>
#include <vector>

#include "sector.h"

#ifndef WIN32
   #include <stdint.h>

   #define SECTOR_API
#else
   #ifdef SECTOR_EXPORTS
      #define SECTOR_API __declspec(dllexport)
   #else
      #define SECTOR_API __declspec(dllimport)
   #endif
   #pragma warning( disable: 4251 )
#endif

#ifdef WIN32
    // Windows compability
    #include <sys/types.h>
    #include <sys/stat.h>
    #include <sys/timeb.h>

    inline int gettimeofday(struct timeval *tp, void *tzp)
    {
        struct _timeb timebuffer;

        errno_t err = _ftime_s (&timebuffer);
        tp->tv_sec = (long)timebuffer.time;
        tp->tv_usec = timebuffer.millitm * 1000;

        return err;
    }

    #if _WIN32_WINNT <= _WIN32_WINNT_WS03
    const char *inet_ntop(int af, const void *src, char *dst, int cnt);
    int inet_pton(int af, const char* s, void* d);
    #endif
#else

#ifndef __APPLE__
extern "C" void srandomdev();
#endif

#endif

class CMutex
{
friend class CCond;

public:
    CMutex();
    ~CMutex();

    bool acquire();
    bool release();
    bool trylock();

private:
#ifdef WIN32
   CRITICAL_SECTION m_Lock;           // mutex to be protected
#else
   pthread_mutex_t m_Lock;            // allow public access for now, to use with pthread_cond_t
#endif

   CMutex& operator=(const CMutex&);
};

class CGuardEx
{
public:
    CGuardEx(CMutex& mutex);
    ~CGuardEx();

private:
   CMutex& m_Lock;            // Alias name of the mutex to be protected
   bool m_bLocked;            // Locking status

   CGuardEx& operator=(const CGuardEx&);
};

class CCond
{
public:
    CCond();
    ~CCond();
 
    bool signal();
    bool broadcast();

#ifndef WIN32
    inline timeval& adjust(timeval & t);
#endif

    bool wait(CMutex & mutex);
    bool wait(CMutex & mutex, int msecs, bool * timedout = NULL);

private:
#ifndef WIN32
   pthread_cond_t m_Cond;
#else
   HANDLE m_Cond;
#endif

   CCond& operator=(const CCond&);
};

enum RWLockState {RW_READ, RW_WRITE};

class RWLock
{
public:
   RWLock();
   ~RWLock();

   int acquire_shared();
   int acquire_exclusive();
   int release_shared();
   int release_exclusive();

private:
#ifndef WIN32
   pthread_rwlock_t m_Lock;
#else
   PSRWLOCK m_Lock;
#endif
};

class RWGuard
{
public:
   RWGuard(RWLock& lock, const RWLockState = RW_READ);
   ~RWGuard();

private:
   RWLock& m_Lock;
   int m_iLocked;
   RWLockState m_LockState;
};

class LocalFS
{
public:
   static int mkdir(const std::string& path);
   static int rmdir(const std::string& path);
   static int erase(const std::string& filename);
   static int clean_dir(const std::string& path);
   static int list_dir(const std::string& path, std::vector<SNode>& filelist);
   static int stat(const std::string& path, SNode& s);
   static int rename(const std::string& src, const std::string& dst);
   static int copy(const std::string& src, const std::string& dst);
   static int get_dir_space(const std::string& path, int64_t& avail);
};

// TODO: wrap some common threading utility, create, join, etc.
class Thread
{
};

#endif
