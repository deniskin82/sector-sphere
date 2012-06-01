#include <iostream>
#include "fusedircache.h"
#include "sectorfs.h"
#include "../common/log.h"
#include "../common/meta.h"


namespace
{
   inline logger::LogAggregate& LOG()
   {
      static logger::LogAggregate& myLogger = logger::getLogger( "DirCache" );
      static bool                  once     = false;

      if( !once )
      {
          once = true;
          myLogger.setLogLevel( logger::Debug );
      }

      return myLogger;
   }
}


DirCache* DirCache::inst = NULL;


DirCache::DirCache()
{
    pthread_mutex_init( &mutex, 0 );
}

DirCache::~DirCache()
{
    pthread_mutex_destroy( &mutex );
}

// Must be called prior to multithreaded use because we do not acquire lock
int DirCache::init_root( Sector& sectorClient ) 
{
    return sectorClient.stat("/", rootNode);
}

void DirCache::clear_cache() 
{
//    LOG().trace << __PRETTY_FUNCTION__ << " entered" << std::endl;

    Lock l(mutex);
//    LOG().info << "**DIR CACHE CLEARED **" << std::endl;
    cache.clear();
    lastUnresolvedStatPathTs = 0;

    LOG().trace << __PRETTY_FUNCTION__ << " exited" << std::endl;
}

void DirCache::clearLastUnresolvedStatLocal()
{
//    LOG().trace << __PRETTY_FUNCTION__ << " entered" << std::endl;
    lastUnresolvedStatPathTs = 0;
//    LOG().trace << __PRETTY_FUNCTION__ << " exited" << std::endl;
}

void DirCache::add( const std::string& path, const std::vector<SNode>& filelist ) 
{
//    LOG().trace << __PRETTY_FUNCTION__ << " entered" << std::endl
//       << " Path = " << path << ", filelist (" << filelist.size() << " entries)" << std::endl;

    ClientConf& config = SectorFS::g_SectorConfig.m_ClientConf;

    std::string canonical_path = Metadata::revisePath( path );
    
//    LOG().debug << "Normalized path to " << canonical_path << " for insertion of " << filelist.size() << " entries" << std::endl;

    pthread_mutex_lock( &mutex );

    time_t tsNow = time(0);

    for( CacheMap::iterator nit = cache.begin(), end = cache.end(); nit != end; )
    {
       if( nit->second.expirationTime < tsNow )
       {
//           LOG().debug << " Entry " << nit->first << " expired, removing..." << std::endl;   
           cache.erase( nit++ );
       }
       else
       {
//           LOG().debug << " Entry " << nit->first << " expires in " << ( nit->second.expirationTime - tsNow )
//              << " seconds, keeping" << std::endl;   
           ++nit;
       }
    }

    pthread_mutex_unlock( &mutex );

    time_t expirationDuration = DEFAULT_TIME_OUT;

    typedef ClientConf::CacheLifetimes CacheLifetimes;
    for( CacheLifetimes::const_iterator i = config.m_pathCache.begin(), end = config.m_pathCache.end(); i != end; ++i )
        if( WildCard::match( i->m_sPathMask, canonical_path ) )
        {
            expirationDuration = i->m_seconds;
//            LOG().debug << "Pattern '" << i->m_sPathMask << "' matches path '" << path << "' with expiration time "
//               << i->m_seconds << " seconds." << std::endl;
            break;
        }

    pthread_mutex_lock( &mutex );

    std::pair< CacheMap::iterator, bool > rc = cache.insert( std::make_pair( canonical_path, CacheRec() ) );
    if( !rc.second )
        LOG().error << "Add: Path " << path << " already exists in cache map (time left = " << ( rc.first->second.expirationTime - time(0) )
           << ") ; will overwrite expiration time!" << std::endl;

    tsNow = time(0);
    rc.first->second.path = path;
    rc.first->second.expirationTime = tsNow + expirationDuration;
    rc.first->second.filemap.clear();
    for( std::vector<SNode>::const_iterator fit = filelist.begin(), fend = filelist.end(); fit != fend; ++fit )
        rc.first->second.filemap[fit->m_strName] = *fit;

    pthread_mutex_unlock( &mutex );

//    LOG().info << "Path " << path << " with " << filelist.size() << " entries added to dir cache at time = " << tsNow << std::endl;

//    LOG().trace << __PRETTY_FUNCTION__ << " exited" << std::endl;
}


// return codes : 1 - cache miss, 0 - cache hit, <0 - sector error
int DirCache::get( const std::string& path, Sector& sectorClient, SNode& node ) 
{
//    LOG().trace << __PRETTY_FUNCTION__ << " entered" << std::endl
//       << " Path = " << path << std::endl;

    std::string canonical_path = Metadata::revisePath( path );

    if ( canonical_path == "/" ) 
    {
        node = rootNode;
//        LOG().info << "CACHE HIT: path = " << path << std::endl;
//        LOG().trace << __PRETTY_FUNCTION__ << " exited, rc = 0" << std::endl;
        return 0;
    }

    time_t                 tsNow = time(0);
    std::string::size_type pos = canonical_path.find_last_of('/');
    std::string            dirpath( pos == 0 ? "/" : canonical_path.substr( 0, pos ) );
    std::string            filename( canonical_path.substr( pos + 1 ) );

//    LOG().debug << "GET Normalized path to " << canonical_path << " for lookup" << std::endl;
//    LOG().debug << "GET Path = " << dirpath << ", file = " << filename << std::endl;

    { // BEGIN CRITICAL SECTION
        Lock lock(mutex);
      
        CacheMap::iterator it = cache.find(dirpath);
        if (it != cache.end())
        {
            if (it->second.expirationTime < tsNow)
            {
//                LOG().debug << "Entry " << dirpath << " expired, removing..." << std::endl;   
                cache.erase(it);
            }
            else 
            {
//                LOG().info << "Found cache for directory " << dirpath << ", looking up file now" << std::endl;
                FileMap::iterator fit = it->second.filemap.find(filename);
                if (fit != it->second.filemap.end())
                {
//                    LOG().info << "GET CACHE HIT: dir = " << dirpath << ", file = " << filename << ", expiration in "
//                       << it->second.expirationTime - tsNow << " seconds" << std::endl;
                    node = fit->second;
//                    LOG().trace << __PRETTY_FUNCTION__ << " exited, rc = 0" << std::endl;
                    return 0;
                }
            }
        }
    } // END CRITICAL SECTION

//    LOG().info << "GET CACHE MISS: dir = " << dirpath << ", file = " << filename << std::endl;

    bool listParentDir = false;
    // Check if this stat belong to dir listed as non-default timeout, i.e. should be cached for long time
    ClientConf& config = SectorFS::g_SectorConfig.m_ClientConf;
    typedef ClientConf::CacheLifetimes CacheLifetimes;
    for( CacheLifetimes::const_iterator i = config.m_pathCache.begin(), end = config.m_pathCache.end(); i != end; ++i )
        if( WildCard::match( i->m_sPathMask, dirpath ) )
        {
            listParentDir = true;
            break;
        }
    
    
    if (!listParentDir) // check last unresolved path
    { // BEGIN CRITICAL SECTION
       Lock lock( mutex );
       listParentDir = lastUnresolvedStatPathTs + DEFAULT_TIME_OUT >= tsNow && lastUnresolvedStatPath == dirpath;
    } // END CRITICAL SECTION

    if (listParentDir) 
    {
//         LOG().debug << "Is last unresolved stat path and not expired: dir = " << dirpath << std::endl;
//         LOG().info << "Asking master for ls of " << dirpath << std::endl;

         std::vector<SNode> pfilelist;
         int r = sectorClient.list( dirpath, pfilelist, false );
         if (r < 0) 
         {
//             LOG().error << "CACHE FAIL: ls of " << dirpath << " on master failed, rc = " << r << std::endl;
//             LOG().trace << __PRETTY_FUNCTION__ << " exited, rc = " << r << std::endl;
             return r;
         }

//         LOG().debug << "Master ls of path " << dirpath << " returned " << pfilelist.size() << " entries" << std::endl;

         add( dirpath, pfilelist );

//         LOG().debug << "Searching for " << filename << "..." << std::endl;

         for (std::vector<SNode>::iterator i = pfilelist.begin(); i != pfilelist.end(); ++ i)
         {
              if (i->m_strName == filename)
              {
                   node = *i;
//                   LOG().info << "GET CACHE HIT (after master ls): dir = " << dirpath << ", file = " << filename << std::endl;
//                   LOG().trace << __PRETTY_FUNCTION__ << " exited, rc = 0" << std::endl;
                   return 0;
              }
         }
     }
     else 
     {
//         LOG().debug << "Not last unresolved stat path; setting unresolved stat path to " << dirpath << ", ts = " << tsNow << std::endl;
         { // Scoping to avoid holding lock while doing logging
             Lock lock (mutex);
             lastUnresolvedStatPath = dirpath;
             lastUnresolvedStatPathTs = tsNow;
         } // End scoping
//         LOG().trace << __PRETTY_FUNCTION__ << " exited, rc = 1" << std::endl;
         return 1;
     } 

//     LOG().trace << __PRETTY_FUNCTION__ << " exited, rc = 1" << std::endl;
     return 1;
}


// return codes : 1 - cache miss, 0 - cache hit
int DirCache::readdir( std::string dirpath, std::vector<SNode>& filelist) 
{
//    LOG().trace << __PRETTY_FUNCTION__ << " entered" << std::endl
//       << " Path = " << dirpath << std::endl;

    std::string canonical_path = Metadata::revisePath( dirpath );
    time_t      tsNow = time(0);

//    LOG().debug << "Normalized path to " << canonical_path << " for lookup" << std::endl;

    Lock lock(mutex);
      
    CacheMap::iterator it = cache.find(canonical_path);
    if (it != cache.end())
    {
      if (it->second.expirationTime < tsNow)
      {
//        LOG().debug << "Entry " << dirpath << " expired, removing..." << std::endl;   
        cache.erase(it);
//        LOG().info << "DIR CACHE MISS (entry expired): dir = " << dirpath << std::endl;
//        LOG().trace << __PRETTY_FUNCTION__ << " exited, rc = 1" << std::endl;
        return 1;
      }
      else 
      {
//        LOG().info << "DIR CACHE HIT: dir = " << dirpath << " with " << it->second.filemap.size() << " entries" << std::endl;
        filelist.clear();
        filelist.reserve( it->second.filemap.size() );
        for( FileMap::iterator fit = it->second.filemap.begin(), end = it->second.filemap.end(); fit != end; ++fit )
            filelist.push_back( fit->second );
//        LOG().trace << __PRETTY_FUNCTION__ << " exited, rc = 0" << std::endl;
        return 0;
      }
    }

//    LOG().info << "DIR CACHE MISS: dir = " << dirpath << std::endl;
//    LOG().trace << __PRETTY_FUNCTION__ << " exited, rc = 0" << std::endl;
    return 1;
}


// Clears the cache entry for path, where path is assumed to be a directory.  It DOES NOT clear the cache
// entries for subdirectories contained within this directory.
void DirCache::clear_cache( std::string path ) 
{
//    LOG().trace << __PRETTY_FUNCTION__ << " entered" << std::endl
//       << " Path = " << path << std::endl;

    std::string canonical_path = Metadata::revisePath( path );

//    LOG().debug << "Normalized path to " << canonical_path << " for lookup" << std::endl;

    Lock lock(mutex);
    
    CacheMap::iterator it = cache.find( canonical_path );
    if (it != cache.end())
    {
//        LOG().debug << "Entry " << path << " found, removing..." << std::endl;   
        cache.erase(it);
    }
//    else
//       LOG().debug << "Entry " << path << " not found in cache" << std::endl;

//    LOG().trace << __PRETTY_FUNCTION__ << " exited" << std::endl;
}


// Clears the cache entry for path, where path is assumed to be a directory, and all entries for all directories
// below this directory.
void DirCache::clear_cache_recursive( std::string path )
{
//    LOG().trace << __PRETTY_FUNCTION__ << " entered" << std::endl
//       << " Path = " << path << std::endl;

    std::string canonical_path = Metadata::revisePath( path );

//    LOG().debug << "Normalized path to " << canonical_path << " for lookup" << std::endl;

    Lock lock(mutex);
    
    for( CacheMap::iterator it = cache.begin(); it != cache.end(); )
    {
       if( it->first.find( path ) == 0 )
       {
//          LOG().debug << "Entry " << path << " matches, removing..." << std::endl;   
          cache.erase( it++ );
       }
       else
          ++it;
    }

//    LOG().trace << __PRETTY_FUNCTION__ << " exited" << std::endl;
}


