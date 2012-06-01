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

#include <meta.h>
#include <common.h>
#include <sphere.h>

using namespace std;

MOMgmt::MOMgmt()
{
   #ifndef WIN32
      pthread_mutex_init(&m_MOLock, NULL);
   #else
      m_MOLock = CreateMutex(NULL, false, NULL);      
   #endif
}

MOMgmt::~MOMgmt()
{
   #ifndef WIN32
      pthread_mutex_destroy(&m_MOLock);
   #else
      CloseHandle(m_MOLock);
   #endif
}

int MOMgmt::add(const string& name, void* loc, const string& user)
{
   CGuard molock(m_MOLock);

   string revised_name = Metadata::revisePath(name);

   if ((revised_name.length() < 7) || (revised_name.substr(0, 7) != "/memory"))
   {
      // all in-memory objects must be named in the directory of "/memory"
      return -1;
   }

   map<string, MemObj>::iterator i = m_mObjects.find(revised_name);

   if (i == m_mObjects.end())
   {
      MemObj object;
      object.m_strName = revised_name;
      object.m_pLoc = loc;
      object.m_strUser = user;
      object.m_llCreationTime = object.m_llLastRefTime = CTimer::getTime();

      m_mObjects[revised_name] = object;

      m_vTBA.push_back(object);

      return 0;
   }

   if (i->second.m_pLoc != loc)
      return -1;

   i->second.m_llLastRefTime = CTimer::getTime();
   return 0;
}

void* MOMgmt::retrieve(const string& name)
{
   CGuard molock(m_MOLock);

   string revised_name = Metadata::revisePath(name);

   map<string, MemObj>::iterator i = m_mObjects.find(revised_name);

   if (i == m_mObjects.end())
      return NULL;

   i->second.m_llLastRefTime = CTimer::getTime();
   return i->second.m_pLoc;
}

int MOMgmt::remove(const string& name)
{
   CGuard molock(m_MOLock);

   string revised_name = Metadata::revisePath(name);

   map<string, MemObj>::iterator i = m_mObjects.find(revised_name);

   if (i == m_mObjects.end())
      return -1;

   m_vTBD.push_back(i->first);

   m_mObjects.erase(i);
   return 0;
}

int MOMgmt::update(vector<MemObj>& tba, vector<string>& tbd)
{
   CGuard molock(m_MOLock);

   tba = m_vTBA;
   m_vTBA.clear();
   tbd = m_vTBD;
   m_vTBD.clear();

   return tba.size() + tbd.size();
}
