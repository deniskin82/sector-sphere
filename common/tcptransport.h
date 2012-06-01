/*****************************************************************************
Copyright 2005 - 2011 The Board of Trustees of the University of Illinois.

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
   Yunhong Gu, last updated 01/02/2011
*****************************************************************************/


#ifndef __TCP_TRANSPORT_H__
#define __TCP_TRANSPORT_H__

#include <transport.h>
#include <string>

namespace sector
{

class TCPTransport: public Transport
{
public:
   TCPTransport();
   virtual ~TCPTransport();

public:
   virtual int open(int& port, bool rendezvous = false, bool reuseaddr = false);

   virtual int listen();
   virtual TCPTransport* accept(std::string& ip, int& port);
   virtual int connect(const std::string& ip, int port);
   virtual int close();

   virtual int send(const char* data, int size);
   virtual int recv(char* data, int size);
   virtual int64_t sendfile(std::fstream& ifs, int64_t offset, int64_t size);
   virtual int64_t recvfile(std::fstream& ofs, int64_t offset, int64_t size);

   virtual bool isConnected();
   virtual int64_t getRealSndSpeed();
   virtual int getLocalAddr(std::string& ip, int& port);

private:
#ifndef WIN32
   int m_iSocket;
#else
   SOCKET m_iSocket;
#endif
   bool m_bConnected;
};

}

#endif
