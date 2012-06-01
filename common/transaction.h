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
   Yunhong Gu, last updated 08/19/2010
*****************************************************************************/


#ifndef __SECTOR_TRANS_H__
#define __SECTOR_TRANS_H__

#include <set>
#include <sstream>
#include <vector>
#include <map>
#include <string>
#ifndef WIN32
   #include <pthread.h>
#endif

struct TransType
{
   static const int FILE = 1;
   static const int SPHERE = 2;
   static const int DB = 3;
   static const int REPLICA = 4;
};

namespace 
{
  template< typename ResultType, typename InputType >
  ResultType lexical_cast( const InputType& in ) {
    std::stringstream os;
    ResultType        out;

    os << in;
    os >> out;
    return out;
  }
}

struct FileChangeType
{
   static const int32_t FILE_UPDATE_NO = 0;
   static const int32_t FILE_UPDATE_NEW = 1;
   static const int32_t FILE_UPDATE_WRITE = 2;
   static const int32_t FILE_UPDATE_REPLICA = 3;
   static const int32_t FILE_UPDATE_NEW_FAILED = 4;
   static const int32_t FILE_UPDATE_WRITE_FAILED = 5;
   static const int32_t FILE_UPDATE_REPLICA_FAILED = 6;
 
   inline static std::string toString( int x ) {
      switch( x ) {
         case FILE_UPDATE_NO:              return "FILE_UPDATE_NO"; break;
         case FILE_UPDATE_NEW:             return "FILE_UPDATE_NEW"; break;
         case FILE_UPDATE_WRITE:           return "FILE_UPDATE_WRITE"; break;
         case FILE_UPDATE_REPLICA:         return "FILE_UPDATE_REPLICA"; break;
         case FILE_UPDATE_NEW_FAILED:      return "FILE_UPDATE_NEW_FAILED"; break;
         case FILE_UPDATE_WRITE_FAILED:    return "FILE_UPDATE_WRITE_FAILED"; break;
         case FILE_UPDATE_REPLICA_FAILED:  return "FILE_UPDATE_REPLICA_FAILED"; break;
         default:                          return lexical_cast<std::string>( x );
      }
   }
};

struct Transaction
{
   int m_iTransID;		// unique id
   int m_iType;			// TransType
   int64_t m_llStartTime;	// start time
   std::string m_strFile;	// if type = FILE, this is the file being accessed
   int m_iMode;			// if type = FILE, this is the file access mode
   std::set<int> m_siSlaveID;	// set of slave id involved in this transaction
   int m_iUserKey;		// user key
   int m_iCommand;		// user's command, 110, 201, etc.

   std::map<int, std::string> m_mResults;	// results for write operation
};

class TransManager
{
public:
   TransManager();
   ~TransManager();

public:
   int create(const int type, const int key, const int cmd, const std::string& file, const int mode);
   int addSlave(int transid, int slaveid);
   int retrieve(int transid, Transaction& trans);
   int retrieve(int slaveid, std::vector<int>& trans);
   int updateSlave(int transid, int slaveid);
   int getUserTrans(int key, std::vector<int>& trans);
   int addWriteResult(int transid, int slaveid, const std::string& result);
   int getFileTrans(const std::string& fileName, std::vector<int>& trans);

public:
   unsigned int getTotalTrans();

public:
   std::map<int, Transaction> m_mTransList;	// list of active transactions
   int m_iTransID;				// seed of transaction id

   pthread_mutex_t m_TLLock;
};

#endif
