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
   Yunhong Gu, last updated 08/23/2010
*****************************************************************************/

#include <common.h>
#include <cstring>
#include "routing.h"

using namespace std;

Routing::Routing():
m_iKeySpace(32)
{
   CGuard::createMutex(m_Lock);
}

Routing::~Routing()
{
   CGuard::releaseMutex(m_Lock);
}

void Routing::init()
{
   m_vFingerTable.clear();
   m_mAddressList.clear();
}

int Routing::insert(const uint32_t& key, const Address& node)
{
   CGuard rg(m_Lock);

   if (m_mAddressList.find(key) != m_mAddressList.end())
      return -1;

   m_mAddressList[key] = node;

   bool found = false;
   for (vector<uint32_t>::iterator i = m_vFingerTable.begin(); i != m_vFingerTable.end(); ++ i)
   {
      if (key > *i)
      {
         m_vFingerTable.insert(i, key);
         found = true;
         break;
      }
   }
   if (!found)
      m_vFingerTable.insert(m_vFingerTable.end(), key);

   return 1;
}

int Routing::remove(const uint32_t& key)
{
   CGuard rg(m_Lock);

   map<uint32_t, Address>::iterator k = m_mAddressList.find(key);
   if (k == m_mAddressList.end())
      return -1;

   m_mAddressList.erase(k);

   for (vector<uint32_t>::iterator i = m_vFingerTable.begin(); i != m_vFingerTable.end(); ++ i)
   {
      if (key == *i)
      {
         m_vFingerTable.erase(i);
         break;
      }
   }

   return 1;
}

int Routing::lookup(const uint32_t& key, Address& node)
{
   CGuard rg(m_Lock);

   if (m_vFingerTable.empty())
      return -1;

   int f = key % m_vFingerTable.size();
   int r = m_vFingerTable[f];
   node = m_mAddressList[r];

   return 1;
}

int Routing::lookup(const string& path, Address& node)
{
   uint32_t key = DHash::hash(path.c_str(), m_iKeySpace);
   return lookup(key, node);
}

int Routing::getEntityID(const string& path)
{
   CGuard rg(m_Lock);

   uint32_t key = DHash::hash(path.c_str(), m_iKeySpace);

   if (m_vFingerTable.empty())
      return -1;

   return key % m_vFingerTable.size();
}

int Routing::getRouterID(const uint32_t& key)
{
   CGuard rg(m_Lock);

   int pos = 0;
   for (vector<uint32_t>::const_iterator i = m_vFingerTable.begin(); i != m_vFingerTable.end(); ++ i)
   {
      if (*i == key)
         return pos;
      ++ pos;
   }
   return -1;
}

int Routing::getRouterID(const Address& node)
{
   CGuard rg(m_Lock);

   for (map<uint32_t, Address>::iterator i = m_mAddressList.begin(); i != m_mAddressList.end(); ++ i)
   {
      if ((i->second.m_iPort == node.m_iPort) && (i->second.m_strIP == node.m_strIP))
         return i->first;
   }

   return -1;
}

bool Routing::match(const uint32_t& cid, const uint32_t& key)
{
   CGuard rg(m_Lock);

   if (m_vFingerTable.empty())
      return false;

   return key == m_vFingerTable[cid % m_vFingerTable.size()];
}

bool Routing::match(const char* path, const uint32_t& key)
{
   CGuard rg(m_Lock);

   if (m_vFingerTable.empty())
      return false;

   uint32_t pid = DHash::hash(path, m_iKeySpace);

   return key == m_vFingerTable[pid % m_vFingerTable.size()];
}

int Routing::getPrimaryMaster(Address& node)
{
   CGuard rg(m_Lock);

   if (m_mAddressList.empty())
      return -1;

   node = m_mAddressList.begin()->second;
   return 0;
}

int Routing::getNumOfMasters()
{
   CGuard rg(m_Lock);

   return m_mAddressList.size();
}

void Routing::getListOfMasters(map<uint32_t, Address>& al)
{
   CGuard rg(m_Lock);

   al = m_mAddressList;
}

int Routing::serializeMasterInfo(char*& buf, int& size)
{
   CGuard rg(m_Lock);

   size = 4 + m_mAddressList.size() * 24;
   buf = new char[size];

   *(int32_t*)buf = m_mAddressList.size();

   char* p = buf + 4;
   for (map<uint32_t, Address>::iterator i = m_mAddressList.begin(); i != m_mAddressList.end(); ++ i)
   {
      *(int32_t*)p = i->first;
      p += 4;
      strncpy(p, i->second.m_strIP.c_str(), 16);
      p += 16;
      *(int32_t*)p = i->second.m_iPort;
      p += 4;
   }

   return size;
}
