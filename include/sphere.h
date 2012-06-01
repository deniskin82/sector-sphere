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

#ifndef __SPHERE_H__
#define __SPHERE_H__

#ifndef WIN32
   #include <stdint.h>
#endif
#include <set>
#include <vector>
#include <map>
#include <string>
#include <udt.h>

#ifndef WIN32
   #define SECTOR_API
#else
   #ifdef SECTOR_EXPORTS
      #define SECTOR_API __declspec(dllexport)
   #else
      #define SECTOR_API __declspec(dllimport)
   #endif
   #pragma warning( disable: 4251 )
#endif

class SECTOR_API SInput
{
public:
   char* m_pcUnit;		// input data
   int m_iRows;			// number of records/rows
   int64_t* m_pllIndex;		// record index

   char* m_pcParam;		// parameter, NULL is no parameter
   int m_iPSize;		// size of the parameter, 0 if no parameter
};

class SECTOR_API SOutput
{
public:
   char* m_pcResult;		// buffer to store the result
   int m_iBufSize;		// size of the physical buffer
   int m_iResSize;		// size of the result

   int64_t* m_pllIndex;		// record index of the result
   int m_iIndSize;		// size of the index structure (physical buffer size)
   int m_iRows;			// number of records/rows

   int* m_piBucketID;		// bucket ID

   int64_t m_llOffset;		// last data position (file offset) of the current processing
				// file processing only. starts with 0 and the last process should set this to -1.

   std::string m_strError;	// error text to be send back to client

public:
   int resizeResBuf(const int& newsize);
   int resizeIdxBuf(const int& newsize);
};

struct SECTOR_API MemObj
{
   std::string m_strName;
   void* m_pLoc;
   std::string m_strUser;
   int64_t m_llCreationTime;
   int64_t m_llLastRefTime;
};

class SECTOR_API MOMgmt
{
public:
   MOMgmt();
   ~MOMgmt();

public:
   int add(const std::string& name, void* loc, const std::string& user);
   void* retrieve(const std::string& name);
   int remove(const std::string& name);

public:
   int update(std::vector<MemObj>& tba, std::vector<std::string>& tbd);

private:
   std::map<std::string, MemObj> m_mObjects;
#ifndef WIN32
   pthread_mutex_t m_MOLock;
#else
   HANDLE m_MOLock;
#endif

private:
   std::vector<MemObj> m_vTBA;
   std::vector<std::string> m_vTBD;
};

class SECTOR_API SFile
{
public:
   std::string m_strHomeDir;		// Sector data home directory: constant
   std::string m_strLibDir;		// the directory that stores the library files available to the current process: constant
   std::string m_strTempDir;		// Sector temporary directory
   std::set<std::string> m_sstrFiles; 	// list of modified files

   int m_iSlaveID;			// unique slave ID
   MOMgmt* m_pInMemoryObjects;		// Handle to the in-memory objects management module
};

#endif
