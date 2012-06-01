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
   Yunhong Gu, last updated 08/19/2010
*****************************************************************************/


#ifndef __SECTOR_LOG_H__
#define __SECTOR_LOG_H__

#include <fstream>
#include <map>
#include <string>
#include <iosfwd>

#include "osportable.h"
#include "udt.h"

struct LogLevel
{
   static const int LEVEL_0 = 0;
   static const int LEVEL_1 = 1;
   static const int LEVEL_2 = 2;
   static const int LEVEL_3 = 3;
   static const int LEVEL_4 = 4;
   static const int LEVEL_5 = 5;
   static const int LEVEL_6 = 6;
   static const int LEVEL_7 = 7;
   static const int LEVEL_8 = 8;
   static const int LEVEL_9 = 9;
   static const int SCREEN = 10;
};

struct LogTag
{
   static const int START = 0;
   static const int END = 1;
};

struct LogString
{
   int m_iLevel;
   std::string m_strLog;
};

struct LogStringTag
{
   LogStringTag(const int tag = LogTag::START, const int level = LogLevel::SCREEN);

   int m_iTag;
   int m_iLevel;

   std::string m_strSrcFile;
   int m_iLine;
   std::string m_strFunc;
};

struct LogStart: LogStringTag
{
   LogStart(const int level = LogLevel::SCREEN) { m_iTag = LogTag::START; m_iLevel = level; }
};

struct LogEnd: LogStringTag
{
   LogEnd() { m_iTag = LogTag::END; };
};


//TODO: make LogStart and LogEnd optional. Support endl and setLevel inline.

class SectorLog
{
public:
   SectorLog();
   ~SectorLog();

public:
   int init(const char* path);
   void close();

   void setLevel(const int level);
   void copyScreen(bool screen);

   void insert(const char* text, const int level = 1);

   SectorLog& operator<<(const LogStringTag& tag);
   SectorLog& operator<<(const std::string& message);
   SectorLog& operator<<(const int64_t& val);
   static SectorLog& endl(SectorLog& log);

private:
   void insert_(const char* text, const int level = 1);

   int m_iLevel;

   CMutex m_LogLock;

   typedef std::map<pthread_t, LogString> ThreadIdStringMap;
   ThreadIdStringMap m_mStoredString;
};

namespace logger {

  // These are highest to lowest 'severity'.  Note: these values are used as indices into a tuple; do not change!
  enum LogLevel {
    Screen  = 0,
    Error   = 1,
    Warning = 2,
    Info    = 3,
    Trace   = 4,
    Debug   = 5
  };


  struct LogAggregate {
    public:
      typedef std::basic_ostream<char> stream_type;

    public:
      inline LogAggregate( const std::string& loggerName, stream_type& screen, stream_type& error, stream_type& warning,
        stream_type& info, stream_type& trace, stream_type& debug ) : loggerName( loggerName ), screen( screen ),
        error( error ), warning( warning ), info( info ), trace( trace ), debug( debug ) {}

      void setLogLevel( LogLevel lvl );

    private: // noncopyable
      LogAggregate( const LogAggregate& );
      LogAggregate& operator=( const LogAggregate& );

    private:
      std::string  loggerName;

    public:
      stream_type& screen;
      stream_type& error;
      stream_type& warning;
      stream_type& info;
      stream_type& trace;
      stream_type& debug;
  };


  void          config( const std::string& outputDir, const std::string& fileNamePrefix );
  LogAggregate& getLogger( const char* name = 0 );
  LogAggregate& getLogger( const std::string& name );
  void          setThreadName( const std::string& name );
}

#endif
