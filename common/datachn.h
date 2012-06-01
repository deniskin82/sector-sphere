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


#ifndef __CB_DATACHN_H__
#define __CB_DATACHN_H__

#include <list>
#include <map>
#include <string>

#include "crypto.h"
#include "sector.h"
#include "udttransport.h"

namespace sector
{

struct RcvData
{
   RcvData(): m_pcData(NULL) {}

   int m_iSession;
   int m_iSize;
   char* m_pcData;
};

class ChnInfo
{
public:
   ChnInfo();
   ~ChnInfo();

public:
   UDTTransport* m_pTrans;
   std::list<RcvData> m_lDataQueue;
   pthread_mutex_t m_SndLock;
   pthread_mutex_t m_RcvLock;
   pthread_mutex_t m_QueueLock;
   int m_iCount;
   int64_t m_llTotalQueueSize;
   bool m_bSecKeySet;
};

class DataChn
{
public:
   DataChn();
   ~DataChn();

   int init(const std::string& ip, int& port);
   int getPort() {return m_iPort;}

   int garbageCollect();

public:
   bool isConnected(const std::string& ip, int port);

   int connect(const std::string& ip, int port);
   int remove(const std::string& ip, int port);

   int send(const std::string& ip, int port, int session, const char* data, int size, Crypto* encoder = NULL);
   int recv(const std::string& ip, int port, int session, char*& data, int& size, Crypto* decoder = NULL);
   int64_t sendfile(const std::string& ip, int port, int session, std::fstream& ifs, int64_t offset, int64_t size, Crypto* encoder = NULL);
   int64_t recvfile(const std::string& ip, int port, int session, std::fstream& ofs, int64_t offset, int64_t& size, Crypto* decorder = NULL);

   int recv4(const std::string& ip, int port, int session, int32_t& val);
   int recv8(const std::string& ip, int port, int session, int64_t& val);

   int64_t getRealSndSpeed(const std::string& ip, int port);

   int getSelfAddr(const std::string& peerip, int peerport, std::string& localip, int& localport);

   int sendError(const std::string& ip, int port, int session);

private:
   std::map<Address, ChnInfo*, AddrComp> m_mChannel;

   UDTTransport m_Base;
   std::string m_strIP;
   int m_iPort;

   pthread_mutex_t m_ChnLock;

private:
   ChnInfo* locate(const std::string& ip, int port);
};

}  // namespace sector

#endif
