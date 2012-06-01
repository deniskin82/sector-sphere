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


#ifndef __SECTOR_ROUTING_H__
#define __SECTOR_ROUTING_H__

#include <vector>
#include <map>
#include <string>
#include "dhash.h"
#include <udt.h>
#include <sector.h>
#ifndef WIN32
   #include <pthread.h>
#endif

class Routing
{
public:
   Routing();
   ~Routing();

public:
   void init();

   int insert(const uint32_t& key, const Address& node);
   int remove(const uint32_t& key);

   int lookup(const std::string& path, Address& node);
   int lookup(const uint32_t& key, Address& node);

   int getEntityID(const std::string& path);

   int getRouterID(const uint32_t& key);
   int getRouterID(const Address& node);

   bool match(const uint32_t& cid, const uint32_t& key);
   bool match(const char* path, const uint32_t& key);

   int getPrimaryMaster(Address& node);

   int getNumOfMasters();

   void getListOfMasters(std::map<uint32_t, Address>& al);

   int serializeMasterInfo(char*& buf, int& size);

private:
   std::vector<uint32_t> m_vFingerTable;
   std::map<uint32_t, Address> m_mAddressList;
   std::map<Address, uint32_t, AddrComp> m_mKeyList;

   int m_iKeySpace;

private:
   pthread_mutex_t m_Lock;
};

#endif
