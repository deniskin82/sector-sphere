/********************************************************************
Short-term non-persistent directory cache for Fuse
Goal of cache is to get rid of as many as possible stat calls, leaving all calls to ls in place
Written by  SergeyC last modified 9/23/11
*********************************************************************/

#ifndef __SECTOR_FUSE_DIR_CACHE_H__
#define __SECTOR_FUSE_DIR_CACHE_H__

#include <list>
#include <fstream>
#include <vector>
#include <pthread.h>

#include "sector.h"

class Lock {
    pthread_mutex_t* mutex;

public:
    Lock(pthread_mutex_t& mux) : mutex(&mux) { pthread_mutex_lock(mutex); }
    ~Lock() { pthread_mutex_unlock(mutex); }
};

class DirCache {
    static const time_t DEFAULT_TIME_OUT = 2; // expiration time for cache - 2 sec

    typedef std::map<std::string, SNode> FileMap;

    struct CacheRec {
        std::string path;
        time_t expirationTime;
// filelist is primary placeholder, while filemap contains pointers to SNodes in filelist 
// and used for fast search by file name
//        std::vector<SNode> filelist;
        FileMap filemap;
    }; 
    typedef std::map<std::string,CacheRec> CacheMap;

public:
    DirCache();
    ~DirCache();

    void add(const std::string& path, const std::vector<SNode>& filelist);
    int readdir( std::string path, std::vector<SNode>& filelist);
    int get(const std::string& path, Sector& sectorClient, SNode& node);

    int init_root( Sector& sectorClient );
    void clear_cache();
    void clear_cache( std::string path );
    void clear_cache_recursive( std::string path );

    void clearLastUnresolvedStatLocal();

    static DirCache& instance()
    {
        if( !inst )
            inst = new DirCache();

        return *inst;
    }

    static void clear() {
        instance().clear_cache();
    }

    static void clear( const std::string& path ) {
        instance().clear_cache( path );
    }

    static void clear_recursive( const std::string& path ) {
        instance().clear_cache_recursive( path );
    }

    static void clearLastUnresolvedStat() {
	instance().clearLastUnresolvedStatLocal();
    }

    static void destroy()
    {
        if( inst )
            delete inst;
        inst = NULL;
    }

private:
    SNode rootNode;
    CacheMap cache;

    std::string lastUnresolvedStatPath;
    time_t lastUnresolvedStatPathTs;

    pthread_mutex_t mutex;

    std::ofstream log;

    static DirCache* inst;
};

#endif

