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

#include "user.h"
#include <common.h>
#include <string.h>

using namespace std;


int User::deserialize(vector<string>& dirs, const string& buf)
{
   unsigned int s = 0;
   while (s < buf.length())
   {
      unsigned int t = buf.find(';', s);

      if (buf.c_str()[s] == '/')
         dirs.insert(dirs.end(), buf.substr(s, t - s));
      else
         dirs.insert(dirs.end(), "/" + buf.substr(s, t - s));
      s = t + 1;
   }

   return dirs.size();
}

bool User::match(const string& path, int32_t rwx) const
{
   // check read flag bit 1 and write flag bit 2
   rwx &= 3;

   if ((rwx & 1) != 0)
   {
      for (vector<string>::const_iterator i = m_vstrReadList.begin(); i != m_vstrReadList.end(); ++ i)
      {
         if ((path.length() >= i->length()) && (path.substr(0, i->length()) == *i) && ((path.length() == i->length()) || (path.c_str()[i->length()] == '/') || (*i == "/")))
         {
            rwx ^= 1;
            break;
         }
      }
   }

   if ((rwx & 2) != 0)
   {
      for (vector<string>::const_iterator i = m_vstrWriteList.begin(); i != m_vstrWriteList.end(); ++ i)
      {
         if ((path.length() >= i->length()) && (path.substr(0, i->length()) == *i) && ((path.length() == i->length()) || (path.c_str()[i->length()] == '/') || (*i == "/")))
         {
            rwx ^= 2;
            break;
         }
      }
   }

   return (rwx == 0);
}

void User::incUseCount()
{
   ++m_iUseCount;
}

// Returns true if user is still in use, false otherwise
bool User::decUseCount()
{
   return --m_iUseCount;
}

int User::getUseCount()
{
   return m_iUseCount;
}

bool User::hasLoggedOut()
{
   return m_bLoggedOut;
}

void User::setLogout(bool logout)
{
   m_bLoggedOut = logout;
}

int User::serialize(char*& buf, int& size)
{
   buf = new char[65536];
   char* p = buf;
   *(int32_t*)p = m_strName.length() + 1;
   p += 4;
   strcpy(p, m_strName.c_str());
   p += m_strName.length() + 1;
   *(int32_t*)p = m_strIP.length() + 1;
   p += 4;
   strcpy(p, m_strIP.c_str());
   p += m_strIP.length() + 1;
   *(int32_t*)p = m_iPort;
   p += 4;
   *(int32_t*)p = m_iDataPort;
   p += 4;
   *(int32_t*)p = m_iKey;
   p += 4;
   memcpy(p, m_pcKey, 16);
   p += 16;
   memcpy(p, m_pcIV, 8);
   p += 8;
   *(int32_t*)p = m_vstrReadList.size();
   p += 4;
   for (vector<string>::iterator i = m_vstrReadList.begin(); i != m_vstrReadList.end(); ++ i)
   {
      *(int32_t*)p = i->length() + 1;
      p += 4;
      strcpy(p, i->c_str());
      p += i->length() + 1;
   }
   *(int32_t*)p = m_vstrWriteList.size();
   p += 4;
   for (vector<string>::iterator i = m_vstrWriteList.begin(); i != m_vstrWriteList.end(); ++ i)
   {
      *(int32_t*)p = i->length() + 1;
      p += 4;
      strcpy(p, i->c_str());
      p += i->length() + 1;
   }
   *(int32_t*)p = m_bExec;
   p += 4;

   size = p - buf;
   return size;
}

int User::deserialize(const char* buf, const int& size)
{
   char* p = (char*)buf;
   m_strName = p + 4;
   p += 4 + m_strName.length() + 1;
   m_strIP = p + 4;
   p += 4 + m_strIP.length() + 1;
   m_iPort = *(int32_t*)p;
   p += 4;
   m_iDataPort = *(int32_t*)p;
   p += 4;
   m_iKey = *(int32_t*)p;
   p += 4;
   memcpy(m_pcKey, p, 16);
   p += 16;
   memcpy(m_pcIV, p, 8);
   p += 8;
   int num = *(int32_t*)p;
   p += 4;
   for (int i = 0; i < num; ++ i)
   {
      p += 4;
      m_vstrReadList.push_back(p);
      p += strlen(p) + 1;
   }
   num = *(int32_t*)p;
   p += 4;
   for (int i = 0; i < num; ++ i)
   {
      p += 4;
      m_vstrWriteList.push_back(p);
      p += strlen(p) + 1;
   }
   m_bExec = *(int32_t*)p;

   return size;
}


UserManager::UserManager()
{
}

UserManager::~UserManager()
{
}

int UserManager::insert(User* u)
{
   CGuardEx ug(m_Lock);

   m_mActiveUsers[u->m_iKey] = u;
   return 0;
}

int UserManager::checkInactiveUsers(vector<User*>& iu, int timeout)
{
   CGuardEx ug(m_Lock);

   iu.clear();

   for (map<int, User*>::iterator i = m_mActiveUsers.begin(); i != m_mActiveUsers.end(); ++ i)
   {
      // slave and master are special users and they should never timeout
      if (0 == i->first)
         continue;

      if (0 >= i->second->getUseCount() && CTimer::getTime() - i->second->m_llLastRefreshTime > timeout * 1000000ULL)
         iu.push_back(i->second);
   }

   for (vector<User*>::iterator i = iu.begin(); i != iu.end(); ++ i)
      m_mActiveUsers.erase((*i)->m_iKey);

   return iu.size();
}

int UserManager::serializeUsers(int& num, vector<char*>& buf, vector<int>& size)
{
   CGuardEx ug(m_Lock);

   buf.clear();
   size.clear();

   for (map<int, User*>::iterator i = m_mActiveUsers.begin(); i != m_mActiveUsers.end(); ++ i)
   {
      if (0 == i->first)
         continue;

      char* ubuf = NULL;
      int usize = 0;
      i->second->serialize(ubuf, usize);

      buf.push_back(ubuf);
      size.push_back(usize);
   }

   num = m_mActiveUsers.size() - 1;

   return num;
}

User* UserManager::acquire(int key)
{
   CGuardEx ug(m_Lock);

   map<int, User*>::iterator i = m_mActiveUsers.find(key);
   if (i == m_mActiveUsers.end())
      return NULL;

   i->second->incUseCount();
   return i->second;
}

void UserManager::release(User* user)
{
   CGuardEx ug(m_Lock);

   if( !user->decUseCount() && user->hasLoggedOut() )
   {
      map<int,User*>::iterator i = m_mActiveUsers.find( user->m_iKey );
      if( i != m_mActiveUsers.end() )
         m_mActiveUsers.erase(i);
      delete user;
   }
}

int UserManager::remove(int key)
{
   CGuardEx ug(m_Lock);

   map<int, User*>::iterator i = m_mActiveUsers.find(key);
   if (i == m_mActiveUsers.end())
      return -1;

   delete i->second;
   m_mActiveUsers.erase(i);
   return 0;
} 
