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


#ifndef WIN32
   #include <sys/types.h>
   #include <sys/socket.h>
   #include <netinet/in.h>
   #include <netdb.h>
   #include <arpa/inet.h>
#else
   #include <winsock2.h>
   #include <ws2tcpip.h>
#endif
#include <sector.h>
#include <tcptransport.h>
#include <cstring>
#include <cstdlib>
#include <sstream>
#include <iostream>
#include <fstream>

using namespace std;
using namespace sector;

TCPTransport::TCPTransport():
m_iSocket(0),
m_bConnected(false)
{

}

TCPTransport::~TCPTransport()
{
   close();
}

int TCPTransport::open(int& port, bool /*rendezvous*/, bool reuseaddr)
{
   struct addrinfo hints, *local;

   memset(&hints, 0, sizeof(struct addrinfo));
   hints.ai_flags = AI_PASSIVE;
   hints.ai_family = AF_INET;
   hints.ai_socktype = SOCK_STREAM;

   stringstream service;
   service << port;

   if (0 != getaddrinfo(NULL, service.str().c_str(), &hints, &local))
      return -1;

   m_iSocket = socket(local->ai_family, local->ai_socktype, local->ai_protocol);

   int reuse = reuseaddr;
   ::setsockopt(m_iSocket, SOL_SOCKET, SO_REUSEADDR, (char*)&reuse, sizeof(reuse));

   if (::bind(m_iSocket, local->ai_addr, local->ai_addrlen) < 0)
   {
      freeaddrinfo(local);
      return SectorError::E_RESOURCE;
   }

   freeaddrinfo(local);

   return 0;
}

int TCPTransport::listen()
{
   return ::listen(m_iSocket, 1024);
}

TCPTransport* TCPTransport::accept(string& ip, int& port)
{
   TCPTransport* t = new TCPTransport;

   sockaddr_in addr;
   socklen_t addrlen = sizeof(sockaddr_in);
   if ((t->m_iSocket = ::accept(m_iSocket, (sockaddr*)&addr, &addrlen)) < 0)
   {
      delete t;
      return NULL;
   }

   char clienthost[NI_MAXHOST];
   char clientport[NI_MAXSERV];
   getnameinfo((sockaddr*)&addr, addrlen, clienthost, sizeof(clienthost), clientport, sizeof(clientport), NI_NUMERICHOST|NI_NUMERICSERV);

   ip = clienthost;
   port = atoi(clientport);

   t->m_bConnected = true;

   return t;
}

int TCPTransport::connect(const string& host, int port)
{
   if (m_bConnected)
      return 0;

   addrinfo hints, *peer;

   memset(&hints, 0, sizeof(struct addrinfo));
   hints.ai_flags = AI_PASSIVE;
   hints.ai_family = AF_INET;
   hints.ai_socktype = SOCK_STREAM;

   stringstream portstr;
   portstr << port;

   if (0 != getaddrinfo(host.c_str(), portstr.str().c_str(), &hints, &peer))
   {
      return SectorError::E_CONNECTION;
   }

   if (::connect(m_iSocket, peer->ai_addr, peer->ai_addrlen) < 0)
   {
      freeaddrinfo(peer);
      return SectorError::E_CONNECTION;
   }

   freeaddrinfo(peer);

   m_bConnected = true;

   return 0;
}

int TCPTransport::close()
{
   if (!m_bConnected)
      return 0;

   m_bConnected = false;

#ifndef WIN32
   return ::close(m_iSocket);
#else
   return closesocket(m_iSocket);
#endif
}

int TCPTransport::send(const char* data, int size)
{
   if (!m_bConnected)
      return -1;

   int ts = size;
   while (ts > 0)
   {
      int s = ::send(m_iSocket, data + size - ts, ts, 0);
      if (s <= 0)
         return -1;
      ts -= s;
   }

   return size;
}

int TCPTransport::recv(char* data, int size)
{
   if (!m_bConnected)
      return -1;

   int tr = size;
   while (tr > 0)
   {
      int r = ::recv(m_iSocket, data + size - tr, tr, 0);
      if (r <= 0)
         return -1;
      tr -= r;
   }

   return size;
}

int64_t TCPTransport::sendfile(std::fstream& ifs, int64_t offset, int64_t size)
{
   if (!m_bConnected)
      return -1;

   if (ifs.bad() || ifs.fail())
      return -1;

   ifs.seekg(offset);

   int block = 1000000;
   char* buf = new char[block];
   int64_t sent = 0;
   while (sent < size)
   {
      int unit = int((size - sent) > block ? block : size - sent);
      ifs.read(buf, unit);
      send(buf, unit);
      sent += unit;
   }

   delete [] buf;

   return sent;
}

int64_t TCPTransport::recvfile(std::fstream& ofs, int64_t offset, int64_t size)
{
   if (!m_bConnected)
      return -1;

   if (ofs.bad() || ofs.fail())
      return -1;

   ofs.seekp(offset);

   int block = 1000000;
   char* buf = new char[block];
   int64_t recd = 0;
   while (recd < size)
   {
      int unit = int((size - recd) > block ? block : size - recd);
      recv(buf, unit);
      ofs.write(buf, unit);
      recd += unit;
   }

   delete [] buf;

   return recd;
}

bool TCPTransport::isConnected()
{
   return m_bConnected;
}

int64_t TCPTransport::getRealSndSpeed()
{
   return -1;
}

int TCPTransport::getLocalAddr(std::string& ip, int& port)
{
   sockaddr_in addr;
   socklen_t size = sizeof(sockaddr_in);

   if (::getsockname(m_iSocket, (sockaddr*)&addr, &size) < 0)
      return -1;

   char clienthost[NI_MAXHOST];
   char clientport[NI_MAXSERV];
   getnameinfo((sockaddr*)&addr, size, clienthost, sizeof(clienthost), clientport, sizeof(clientport), NI_NUMERICHOST|NI_NUMERICSERV);

   ip = clienthost;
   port = atoi(clientport);

   return 0;
}
