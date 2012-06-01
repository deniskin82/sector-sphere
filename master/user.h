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


#ifndef __SECTOR_USER_H__
#define __SECTOR_USER_H__

#include <string>
#include <vector>
#include <map>
#include <stdint.h>
#include <osportable.h>

class User
{
public:
   int deserialize(std::vector<std::string>& dirs, const std::string& buf);
   bool match(const std::string& path, int rwx) const;

   void incUseCount();
   bool decUseCount();
   int getUseCount();
   bool hasLoggedOut();
   void setLogout(bool logout);

public:
   int serialize(char*& buf, int& size);
   int deserialize(const char* buf, const int& size);

public:
   std::string m_strName;			// user name

   std::string m_strIP;				// client IP address
   int m_iPort;					// client port (GMP)
   int m_iDataPort;				// data channel port

   int32_t m_iKey;				// client key

   unsigned char m_pcKey[16];			// client crypto key
   unsigned char m_pcIV[8];			// client crypto iv

   int64_t m_llLastRefreshTime;			// timestamp of last activity
   std::vector<std::string> m_vstrReadList;	// readable directories
   std::vector<std::string> m_vstrWriteList;	// writable directories
   bool m_bExec;				// permission to run Sphere application
   
   int m_iUseCount;
   bool m_bLoggedOut;
};

class UserManager
{
public:
   UserManager();
   ~UserManager();

public:
   int insert(User* u);
   int checkInactiveUsers(std::vector<User*>& iu, int timeout);
   int serializeUsers(int& num, std::vector<char*>& buf, std::vector<int>& size);
   User* acquire(int key);
   void release(User* user);
   int remove(int key);

public:
   std::map<int, User*> m_mActiveUsers;
   CMutex m_Lock;
};

#endif
