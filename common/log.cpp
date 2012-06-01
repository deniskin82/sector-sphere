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
   Yunhong Gu, last updated 05/19/2011
*****************************************************************************/

#include <cstring>
#include <time.h>

#include <iostream>
#include <sstream>
#include <string>

#include "common.h"
#include "log.h"

#ifdef WIN32
   #define snprintf sprintf_s
   #define pthread_t int
   #define pthread_self() GetCurrentThreadId()
#endif

#include <bits/char_traits.h>
#include <ctime>
#include <fcntl.h>
#include <iomanip>
#include <map>
#include <memory>
#include <ostream>
#include <pthread.h>
#include <sstream>
#include <stdexcept>
#include <streambuf>
#include <sys/syscall.h>
#include <sys/timeb.h>
#include <sys/types.h>
#include <unistd.h>
#include "strconcat.h"

namespace {

#ifdef MULTITHREADED_LOGGER
  class Locker {
    public:
      Locker( pthread_mutex_t& lock ) : lock( lock ), held( true )
        { pthread_mutex_lock( &lock ); }

      ~Locker()
        { release(); }

      void release()
        { if( held ) pthread_mutex_unlock( &lock ); held = false; }

    private:
      pthread_mutex_t& lock;
      bool             held;
  };
#endif

  class RW_Read_Locker {
    public:
      RW_Read_Locker( pthread_rwlock_t& lock ) : lock( lock ), held( true )
        { pthread_rwlock_rdlock( &lock ); }

      ~RW_Read_Locker()
        { release(); }

      void release()
        { if( held ) pthread_rwlock_unlock( &lock ); held = false; }

    private:
      pthread_rwlock_t& lock;
      bool              held;
  };


  class RW_Write_Locker {
    public:
      RW_Write_Locker( pthread_rwlock_t& lock ) : lock( lock ), held( true )
        { pthread_rwlock_wrlock( &lock ); }

      ~RW_Write_Locker()
        { release(); }

      void release()
        { if( held ) pthread_rwlock_unlock( &lock ); held = false; }

    private:
      pthread_rwlock_t& lock;
      bool              held;
  };


  static std::string numericalDate() {
    time_t currentTimeInSecs = time( 0 );
    struct tm currentDateAndTime;

    localtime_r( &currentTimeInSecs, &currentDateAndTime );

    std::stringstream result;

    result << 1900 + currentDateAndTime.tm_year << std::setw( 2 ) << std::setfill( '0' )
      << 1 + currentDateAndTime.tm_mon << std::setw( 2 ) << std::setfill( '0' )
      << currentDateAndTime.tm_mday;

    return result.str();
  }


  const char* levelToName( logger::LogLevel level ) {
    switch( level ) {
      case logger::Screen:  return "SCR";
      case logger::Error:   return "ERR";
      case logger::Warning: return "WRN";
      case logger::Info:    return "INF";
      case logger::Trace:   return "TRC";
      case logger::Debug:   return "DBG";
    }

    return 0;
  }

  template< typename ResultType, typename InputType >
  ResultType lexical_cast( const InputType& in ) {
    std::stringstream os;
    ResultType        out;

    os << in;
    os >> out;
    return out;
  }

  template< typename T >
  struct unowned_ptr {
      unowned_ptr() : ptr() {}
      explicit unowned_ptr( T* t ) : ptr( t ) {}
      T& operator*() { return *ptr; }
      
    private:
      T* ptr;
  };
}

namespace logger {

  class logbuf : public std::basic_streambuf< char > {
    public:
      typedef char                     char_type;
      typedef std::char_traits<char>   traits_type;
      typedef traits_type::int_type    int_type;
      typedef traits_type::pos_type    pos_type;
      typedef traits_type::off_type    off_type;

    public:
      logbuf( const char* name, LogLevel level ) : name( name ), level( level )
        {
#ifdef MULTITHREADED_LOGGER
        pthread_mutex_init( &currentLinesLock, 0 );
#endif
        }

    protected:
      virtual int_type overflow( int_type __c = traits_type::eof() );
      virtual std::streamsize xsputn( const char_type* __s, std::streamsize __n );

    private:
      std::string                        name;
      LogLevel                           level;
#ifdef MULTITHREADED_LOGGER
      pthread_mutex_t                    currentLinesLock;
      std::map< pid_t, std::string >     currentLines;
#else
      std::string                        currentLine;
#endif
  };


  class nulllogbuf : public std::basic_streambuf< char > {
    public:
      typedef char                     char_type;
      typedef std::char_traits<char>   traits_type;
      typedef traits_type::int_type    int_type;
      typedef traits_type::pos_type    pos_type;
      typedef traits_type::off_type    off_type;

    protected:
      virtual std::streamsize xsgetn( char_type*, std::streamsize ) { return 0; }
      virtual std::streamsize xsputn( const char_type*, std::streamsize s ) { return s; }
      virtual int_type overflow( int_type c = traits_type::eof() ) { return c; }
  };


  typedef std::string                                                 log_key_t;
  typedef LogAggregate*                                               log_mapped_value_t;
  typedef std::map< log_key_t, log_mapped_value_t >                   loggers_t;
  typedef unowned_ptr< std::basic_ostream<char> >                     shp_stream_t;


  struct log_tuple {
    log_tuple( const shp_stream_t& screen, const shp_stream_t& error, const shp_stream_t& warning, const shp_stream_t& info, const shp_stream_t& trace, const shp_stream_t& debug ) :
      screen( screen ), error( error ), warning( warning ), info( info ), trace( trace ), debug( debug )  {}

   shp_stream_t get( size_t n ) {
      switch( n ) {
        case 0: return screen;
        case 1: return error;
        case 2: return warning;
        case 3: return info;
        case 4: return trace;
        case 5: return debug;
        default: return shp_stream_t();
     }
   }

   shp_stream_t screen;
   shp_stream_t error;
   shp_stream_t warning;
   shp_stream_t info;
   shp_stream_t trace;
   shp_stream_t debug;
  };

  typedef log_tuple                                                   stream_instance_t;
  typedef std::map< log_key_t, stream_instance_t >                    stream_instance_map_t;
  typedef std::map< pid_t, std::string >                              thread_names_t;

  static nulllogbuf                nullBuffer;
  static std::basic_ostream<char>  bitBucket( &nullBuffer );

  static pthread_rwlock_t          configurationLock = PTHREAD_RWLOCK_INITIALIZER;
  static std::string               outputDir( "/tmp" );
  static std::string               fileNamePrefix( "log" );
  static int                       fd( -1 );
  static struct tm                 currentDate;

  static pthread_rwlock_t          currentLoggersLock = PTHREAD_RWLOCK_INITIALIZER;
  static loggers_t                 currentLoggers;

  static pthread_rwlock_t          instancesLock = PTHREAD_RWLOCK_INITIALIZER;
  static stream_instance_map_t     instances;

  static pthread_rwlock_t          threadNamesLock = PTHREAD_RWLOCK_INITIALIZER;
  static thread_names_t            threadNames;


  static void reopenLogFile() {
    (void)close( fd );

    std::string fileToOpen;
    concatenate( fileToOpen, outputDir, '/', fileNamePrefix, '-', numericalDate(), ".log" );

    fd = open( fileToOpen.c_str(), O_CREAT | O_WRONLY, 0666 );
    if( fd >= 0 )
      lseek( fd, 0, SEEK_END );
  }


  // Must be called with configurationLock held
  bool dayHasChanged() {
    time_t    currentTimeInSecs = time( 0 );
    struct tm currentDateAndTime;

    localtime_r( &currentTimeInSecs, &currentDateAndTime );
    return currentDateAndTime.tm_mday != currentDate.tm_mday
      || currentDateAndTime.tm_mon != currentDate.tm_mon
      || currentDateAndTime.tm_year != currentDate.tm_year;
  }


  // Must be called with configurationLock held (write lock)
  void updateCurrentDate() {
    time_t currentTimeInSecs = time( 0 );
    localtime_r( &currentTimeInSecs, &currentDate );
  }

  std::string dateAndTime() {
    struct timeb currentTime;
    struct tm currentDateAndTime;

    ftime( &currentTime );
    localtime_r( &currentTime.time, &currentDateAndTime );

    std::stringstream result;

    result << 1900 + currentDateAndTime.tm_year << '-' << std::setw( 2 ) << std::setfill( '0' )
      << 1 + currentDateAndTime.tm_mon << '-' << std::setw( 2 ) << std::setfill( '0' )
      << currentDateAndTime.tm_mday << ' ' << std::setw( 2 ) << std::setfill( '0' )
      << currentDateAndTime.tm_hour << ':' << std::setw( 2 ) << std::setfill( '0' )
      << currentDateAndTime.tm_min << ':' << std::setw( 2 ) << std::setfill( '0' )
      << currentDateAndTime.tm_sec << '.' << std::setw( 3 ) << std::setfill( '0' )
      << currentTime.millitm;

    return result.str();
  }


  std::string getThreadName() {
    std::string name;

    { // Begin critical section
      RW_Read_Locker critSec( threadNamesLock );
      thread_names_t::const_iterator iter = threadNames.find( syscall( SYS_gettid ) );
      name = iter == threadNames.end() ? "TID-" + lexical_cast<std::string>( syscall( SYS_gettid ) ) : iter->second;
    } // End critical section

    return '[' + name + ']';
  }


  std::streamsize logbuf::xsputn( const char_type* __s, std::streamsize __n ) {
    if( !__s || !__n )
      return 0;

#ifdef MULTITHREADED_LOGGER
    Locker       critSec( currentLinesLock );
    std::string& currentLine( currentLines[ syscall( SYS_gettid ) ] );
#endif

    for( std::streamsize i = 0; i < __n; ++i ) {
      if( currentLine.empty() ) {
        currentLine.reserve( 100 );
        if( level == Screen )
          concatenate( currentLine, dateAndTime(), ' ', getThreadName(), ' ', name, " - " );
        else 
          concatenate( currentLine, levelToName( level ), ' ', dateAndTime(), ' ', getThreadName(), ' ', name, " - " );
      }

      currentLine += __s[ i ];
  
      if( __s[ i ] == '\n' ) {
        if( level == Screen ) 
          std::cerr << currentLine;
        else
        { // Begin critical section
          RW_Write_Locker critSec( configurationLock );
          if( dayHasChanged() ) {
            reopenLogFile();
            updateCurrentDate();
          }
          write( fd, &currentLine[0], currentLine.size() );
        } // End critical section
  
        currentLine.clear();
      }
    }

    return __n;
  }


  logbuf::int_type logbuf::overflow( int_type c ) {
#ifdef MULTITHREADED_LOGGER
    Locker       critSec( currentLinesLock );
    std::string& currentLine( currentLines[ syscall( SYS_gettid ) ] );
#endif

    if( currentLine.empty() && c != '\n' ) {
      currentLine.reserve( 100 );
      if( level == Screen )
        concatenate( currentLine, dateAndTime(), ' ', getThreadName(), ' ', name, " - " );
      else 
        concatenate( currentLine, levelToName( level ), ' ', dateAndTime(), ' ', getThreadName(), ' ', name, " - " );
    }

    currentLine += (char)c;

    if( c == '\n' ) {
      if( level == Screen ) 
        std::cerr << currentLine;
      else
      { // Begin critical section
        RW_Write_Locker critSec( configurationLock );
        if( dayHasChanged() ) {
          reopenLogFile();
          updateCurrentDate();
        }
        write( fd, &currentLine[0], currentLine.size() );
      } // End critical section

      currentLine.clear();
    }

    return c;
  }


  void LogAggregate::setLogLevel( LogLevel lvl ) {
    std::string        myName = loggerName;
    log_mapped_value_t newLogger;

    RW_Write_Locker critSec( currentLoggersLock );
    loggers_t::iterator iter = currentLoggers.find( myName );

    { // Begin critical section
      RW_Read_Locker critSec2( instancesLock );
      stream_instance_t instance = instances.find( myName )->second;

      // Because the user may hold a reference to a LogAggregate (by calling getLogger), we have to ensure that
      // those references remain valid, even though we are constructing a new LogAggregate here.  Thus, we
      // must use placement new.
      switch( lvl ) {
        case Screen:  newLogger = new (iter->second) LogAggregate( myName, *instance.get(Screen), bitBucket, bitBucket, bitBucket, bitBucket, bitBucket ); break;
        case Error:   newLogger = new (iter->second) LogAggregate( myName, *instance.get(Screen), *instance.get(Error), bitBucket, bitBucket, bitBucket, bitBucket ); break;
        case Warning: newLogger = new (iter->second) LogAggregate( myName, *instance.get(Screen), *instance.get(Error), *instance.get(Warning), bitBucket, bitBucket, bitBucket ); break;
        case Info:    newLogger = new (iter->second) LogAggregate( myName, *instance.get(Screen), *instance.get(Error), *instance.get(Warning), *instance.get(Info), bitBucket, bitBucket ); break;
        case Trace:   newLogger = new (iter->second) LogAggregate( myName, *instance.get(Screen), *instance.get(Error), *instance.get(Warning), *instance.get(Info), *instance.get(Trace), bitBucket ); break;
        case Debug:   newLogger = new (iter->second) LogAggregate( myName, *instance.get(Screen), *instance.get(Error), *instance.get(Warning), *instance.get(Info), *instance.get(Trace), *instance.get(Debug) ); break;
      }
    } // End Critical section
  }


  loggers_t::iterator makeLogger( const char* name ) {
    std::auto_ptr<logbuf> screenBuf( new logbuf( name, Screen ) );
    std::auto_ptr<logbuf> errorBuf( new logbuf( name, Error ) );
    std::auto_ptr<logbuf> warningBuf( new logbuf( name, Warning ) );
    std::auto_ptr<logbuf> infoBuf( new logbuf( name, Info ) );
    std::auto_ptr<logbuf> traceBuf( new logbuf( name, Trace ) );
    std::auto_ptr<logbuf> debugBuf( new logbuf( name, Debug ) );

    stream_instance_t instance(
        shp_stream_t( new std::basic_ostream<char>( screenBuf.release() ) ),
        shp_stream_t( new std::basic_ostream<char>( errorBuf.release() ) ),
        shp_stream_t( new std::basic_ostream<char>( warningBuf.release() ) ),
        shp_stream_t( new std::basic_ostream<char>( infoBuf.release() ) ),
        shp_stream_t( new std::basic_ostream<char>( traceBuf.release() ) ),
        shp_stream_t( new std::basic_ostream<char>( debugBuf.release() ) )
    );

    { // Begin critical section
      RW_Write_Locker critSec( instancesLock );
      instances.insert( std::make_pair( name, instance ) );
    } // End critical section

    // Note that all LogAggregates constructed here are stored as unmanaged pointers in the logger map, thus
    // technically this is a memory leak.  However, we provide no facility to delete an existing logger, so
    // the map will only be emptied when the process exits anyway.
    log_mapped_value_t actualLog( new LogAggregate(
        name,
        *instance.get(Screen),
        *instance.get(Error),
        *instance.get(Warning),
        *instance.get(Info),
        bitBucket,
        bitBucket
      )
    );

    RW_Write_Locker critSec( currentLoggersLock );
    return currentLoggers.insert( std::make_pair( name, actualLog ) ).first;
  }


  LogAggregate& getLogger( const char* which ) {
    if( !which )
      which = "";

    RW_Read_Locker locker( currentLoggersLock );
    loggers_t::iterator logIter = currentLoggers.find( which );
    if( logIter == currentLoggers.end() ) {
      locker.release();
      logIter = makeLogger( which );
    }

    return *( logIter->second );
  }


  LogAggregate& getLogger( const std::string& which ) {
    return getLogger( which.c_str() );
  }


  void config( const std::string& outDir, const std::string& prefix ) {
    outputDir = outDir;
    fileNamePrefix = prefix;
    RW_Write_Locker critSec( configurationLock );
    reopenLogFile();
  }


  void setThreadName( const std::string& name ) {
    RW_Write_Locker critSec( threadNamesLock );
    threadNames[ syscall( SYS_gettid ) ] = name;
  }
}

// End of new logger, below is old logger for compatibility

using namespace std;


static logger::LogAggregate& log_ = logger::getLogger( "Sector" );


LogStringTag::LogStringTag(const int tag, const int level):
m_iTag(tag),
m_iLevel(level),
m_strSrcFile(__FILE__),
m_iLine(__LINE__),
m_strFunc(__func__)
{
}


SectorLog::SectorLog():
m_iLevel(1)
{
}

SectorLog::~SectorLog()
{
}

int SectorLog::init(const char* path)
{
   // NULL path means users do not want to write to log file.
   if( path )
      logger::config( path, "sector" );
   return 0;
}

void SectorLog::close()
{
}

void SectorLog::setLevel(const int level)
{
   if (level >= 0) 
   {
      CGuardEx lg(m_LogLock);
      m_iLevel = level;
      log_.setLogLevel( static_cast<logger::LogLevel>( std::min( level, 5 ) ) );
   }
}

void SectorLog::copyScreen(const bool)
{
}

SectorLog& SectorLog::operator<<(const LogStringTag& tag)
{
   CGuardEx lg(m_LogLock);

   pthread_t key = pthread_self();

   if (tag.m_iTag == LogTag::START)
   {
      LogString ls;
      ls.m_iLevel = tag.m_iLevel;
      m_mStoredString[key] = ls;
   }
   else if (tag.m_iTag == LogTag::END)
   {
      ThreadIdStringMap::iterator i = m_mStoredString.find(key);
      if (i != m_mStoredString.end())
      {
         insert_(i->second.m_strLog.c_str(), i->second.m_iLevel);
         m_mStoredString.erase(i);
      }
   }

   return *this;
}

SectorLog& SectorLog::operator<<(const std::string& message)
{
   CGuardEx lg(m_LogLock);

   pthread_t key = pthread_self();

   ThreadIdStringMap::iterator i = m_mStoredString.find(key);
   if (i == m_mStoredString.end())
   {
      // no start tag, use default: level = SCREEN
      LogString ls;
      ls.m_iLevel = 0;
      m_mStoredString[key] = ls;
      i = m_mStoredString.find(key);
   }

   i->second.m_strLog += message;

   return *this;
}

SectorLog& SectorLog::operator<<(const int64_t& val)
{
   CGuardEx lg(m_LogLock);

   pthread_t key = pthread_self();

   ThreadIdStringMap::iterator i = m_mStoredString.find(key);
   if (i != m_mStoredString.end())
   {
      stringstream valstr;
      valstr << val;
      i->second.m_strLog += valstr.str();
   }

   return *this;
}

SectorLog& SectorLog::endl(SectorLog& log)
{
   CGuardEx lg(log.m_LogLock);
   pthread_t key = pthread_self();
   ThreadIdStringMap::iterator i = log.m_mStoredString.find(key);
   if (i != log.m_mStoredString.end())
   {
      log.insert_(i->second.m_strLog.c_str(), i->second.m_iLevel);
      log.m_mStoredString.erase(i);
   }

   return log;
}

void SectorLog::insert(const char* text, const int level)
{
   CGuardEx lg(m_LogLock);
   insert_( text, level );
}


void SectorLog::insert_(const char* text, const int level)
{
   switch( level ) {
       case 0:   log_.screen << text; break;
       case 1:   log_.error << text; break;
       case 2:   log_.warning << text; break;
       case 3:   log_.info << text; break;
       case 4:   log_.trace << text; break;
       default:  log_.debug << text; break;
   }

   if( text && text[ strlen( text ) - 1 ] != '\n' ) {
     switch( level ) {
         case 0:   log_.screen << std::endl; break;
         case 1:   log_.error << std::endl; break;
         case 2:   log_.warning << std::endl; break;
         case 3:   log_.info << std::endl; break;
         case 4:   log_.trace << std::endl; break;
         default:  log_.debug << std::endl; break;
     }
   }
}

