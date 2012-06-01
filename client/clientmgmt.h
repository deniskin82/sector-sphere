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
   Yunhong Gu, last updated 01/12/2010
*****************************************************************************/


#ifndef __SECTOR_CLIENT_MGMT_H__
#define __SECTOR_CLIENT_MGMT_H__

#include <map>
#include "client.h"
#include "fsclient.h"
#include "dcclient.h"
#ifndef WIN32
   #include <pthread.h>
#endif

namespace sector
{

class ClientMgmt
{
public:
   ClientMgmt();
   ~ClientMgmt();

   Client* lookupClient(const int& id);
   FSClient* lookupFS(const int& id);
   DCClient* lookupDC(const int& id);

   int insertClient(Client* c);
   int insertFS(FSClient* f);
   int insertDC(DCClient* d);

   int removeClient(const int& id);
   int removeFS(const int& id);
   int removeDC(const int& id);

private:
   std::map<int, Client*> m_mClients;
   std::map<int, FSClient*> m_mSectorFiles;
   std::map<int, DCClient*> m_mSphereProcesses;

   int m_iID;

   pthread_mutex_t m_CLock;
   pthread_mutex_t m_FSLock;
   pthread_mutex_t m_DCLock;
};

}  // namespace sector

#endif
