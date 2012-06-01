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


#ifndef __SSL_TRANSPORT_H__
#define __SSL_TRANSPORT_H__

#include <openssl/bio.h>
#include <openssl/err.h>
#include <openssl/ssl.h>
#include <string>

#include "transport.h"
#include "udt.h"

namespace sector
{

class SSLTransport //: public Transport
{
public:
   SSLTransport();
   ~SSLTransport();

public:
   static void init();
   static void destroy();

public:
   int initServerCTX(const char* cert, const char* key);
   int initClientCTX(const char* cert);
   int releaseCTX() { return 0; } // TODO: release CTX manually by Sector, not destructor.

   int open(const char* ip, const int& port);

   // DO NOT close the listening SSL transport until all accepted transports are closed.
   int listen();

   SSLTransport* accept(char* ip, int& port);
   int connect(const char* ip, const int& port);
   int close();

   int send(const char* data, const int& size);
   int recv(char* data, const int& size);

   int64_t sendfile(const char* file, const int64_t& offset, const int64_t& size);
   int64_t recvfile(const char* file, const int64_t& offset, const int64_t& size);

   int getLocalIP(std::string& ip);

private:
   bool m_bClientCTX;
   SSL_CTX* m_pCTX;
   SSL* m_pSSL;
#ifndef WIN32
   int m_iSocket;
#else
   SOCKET m_iSocket;
#endif

   bool m_bConnected;

private:
   static int g_iInstance;
};

}  // namespace sector

#endif
