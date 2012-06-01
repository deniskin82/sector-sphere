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
   Yunhong Gu, last updated 10/21/2010
*****************************************************************************/


#ifndef WIN32
   #include <arpa/inet.h>
   #include <netdb.h>
   #include <netinet/in.h>
   #include <sys/socket.h>
   #include <sys/types.h>
   #include <unistd.h>
#else
   #include <winsock2.h>
   #include <ws2tcpip.h>
#endif
#include <fstream>
#include <iostream>
#include <sstream>
#include <string.h>

#include "sector.h"
#include "ssltransport.h"
#include <iostream>
using namespace std;
using namespace sector;

int SSLTransport::g_iInstance = 0;

SSLTransport::SSLTransport():
m_bClientCTX(false),
m_pCTX(NULL),
m_pSSL(NULL),
m_iSocket(0),
m_bConnected(false)
{
}

SSLTransport::~SSLTransport()
{
   close();

   // Server m_pCTX is shared for all server sockets (same as listening socket), so cannot be freed.
   if ((NULL != m_pCTX) && m_bClientCTX)
      SSL_CTX_free(m_pCTX);
   //TODO: there is memory leak for the server CTX.

   if (NULL != m_pSSL)
      SSL_free(m_pSSL);
}

void SSLTransport::init()
{
   if (0 == g_iInstance)
   {
      SSL_load_error_strings();
      ERR_load_SSL_strings();
      SSL_library_init();
   }

   g_iInstance ++;
}

void SSLTransport::destroy()
{
   g_iInstance --;

   if (0 == g_iInstance)
   {
      //CRYPTO_cleanup_all_ex_data();
   }
}

int SSLTransport::initServerCTX(const char* cert, const char* key)
{
   m_pCTX = SSL_CTX_new(TLSv1_server_method());
   if (m_pCTX == NULL)
   {
      cerr << "Failed to init CTX. Aborting.\n";
      return SectorError::E_INITCTX;
   }

   if (!SSL_CTX_use_certificate_file(m_pCTX, cert, SSL_FILETYPE_PEM) || !SSL_CTX_use_PrivateKey_file(m_pCTX, key, SSL_FILETYPE_PEM))
   {
      ERR_print_errors_fp(stderr);
      SSL_CTX_free(m_pCTX);
      m_pCTX = NULL;
      return SectorError::E_INITCTX;
   }

   return 0;
}

int SSLTransport::initClientCTX(const char* cert)
{
   m_pCTX = SSL_CTX_new(TLSv1_client_method());

   if(!SSL_CTX_load_verify_locations(m_pCTX, cert, NULL))
   {
      cerr << "Error loading trust store: " << cert << endl;
      SSL_CTX_free(m_pCTX);
      m_pCTX = NULL;
      return SectorError::E_INITCTX;
   }
   m_bClientCTX = true;
   return 0;
}

int SSLTransport::open(const char* ip, const int& port)
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

   if ((NULL == ip) && (0 == port))
   {
      freeaddrinfo(local);
      return 0;
   }

   int reuse = 1;
   ::setsockopt(m_iSocket, SOL_SOCKET, SO_REUSEADDR, (char*)&reuse, sizeof(reuse));

   if (::bind(m_iSocket, local->ai_addr, local->ai_addrlen) < 0)
   {
      cerr << "SSL socket unable to bind on address " << ip << " " << port << endl;
      freeaddrinfo(local);
      return SectorError::E_RESOURCE;
   }

   freeaddrinfo(local);

   return 0;
}

int SSLTransport::listen()
{
   return ::listen(m_iSocket, 1024);
}

SSLTransport* SSLTransport::accept(char* ip, int& port)
{
   SSLTransport* t = new SSLTransport;

   sockaddr_in addr;
   socklen_t size = sizeof(sockaddr_in);
   if ((t->m_iSocket = ::accept(m_iSocket, (sockaddr*)&addr, &size)) < 0)
   {
      delete t;
      return NULL;
   }

   inet_ntop(AF_INET, &(addr.sin_addr), ip, 64);
   port = addr.sin_port;

   // CTX is shared for all accepted SSL connections.
   t->m_pSSL = SSL_new(m_pCTX);
   SSL_set_fd(t->m_pSSL, t->m_iSocket);

   int rc = SSL_accept(t->m_pSSL);
   if (rc <= 0)
   {
      delete t;
      return NULL;
   }

   t->m_bConnected = true;

   return t;
}

int SSLTransport::connect(const char* host, const int& port)
{
   if (m_bConnected)
      return 0;

   if (NULL == m_pCTX)
      return -1;

   addrinfo hints, *peer;
   memset(&hints, 0, sizeof(struct addrinfo));
   hints.ai_flags = AI_PASSIVE;
   hints.ai_family = AF_INET;
   hints.ai_socktype = SOCK_STREAM;

   stringstream portstr;
   portstr << port;
   if (0 != getaddrinfo(host, portstr.str().c_str(), &hints, &peer))
   {
      cerr << "SSL connect: invalid address " << host << " " << port << endl;
      return SectorError::E_CONNECTION;
   }

   if (::connect(m_iSocket, peer->ai_addr, peer->ai_addrlen) < 0)
   {
      freeaddrinfo(peer);
      return SectorError::E_CONNECTION;
   }

   freeaddrinfo(peer);

   m_pSSL = SSL_new(m_pCTX);
   SSL_set_fd(m_pSSL, m_iSocket);

   if (SSL_connect(m_pSSL) <= 0)
   {
      SSL_free(m_pSSL);
      m_pSSL = NULL;
      return SectorError::E_SECURITY;
   }

   if (SSL_get_verify_result(m_pSSL) != X509_V_OK)
   {
      cerr << "failed to verify SSL certificate.\n";
      return SectorError::E_SECURITY;
   }

   // NOTE: When this is enabled, please check memory leak.
   //X509* peer_cert = SSL_get_peer_certificate(m_pSSL);
   //char peer_CN[256];
   //X509_NAME_get_text_by_NID(X509_get_subject_name(peer_cert), NID_commonName, peer_CN, 256);
   //if (strcasecmp(peer_CN, host))
   //{
   //   cerr << "server name does not match.\n";
   //   return -1;
   //}
   //X509_OBJECT_free_contents(peer_cert);

   m_bConnected = true;

   return 0;
}

int SSLTransport::close()
{
   if (m_bConnected) {
     m_bConnected = false;
   // SSL shutdown requires up to 4 rounds of attempts.
//     for (int i = 0; (i < 4) && (SSL_shutdown(m_pSSL) == 0); ++ i) {}
     SSL_shutdown(m_pSSL);
   }

#ifndef WIN32
   return ::close(m_iSocket);
#else
   return closesocket(m_iSocket);
#endif
}

int SSLTransport::send(const char* data, const int& size)
{
   if (!m_bConnected)
      return -1;

   int ts = size;
   while (ts > 0)
   {
      int s = SSL_write(m_pSSL, data + size - ts, ts);
      if (s <= 0)
         return -1;
      ts -= s;
   }
   return size;
}

int SSLTransport::recv(char* data, const int& size)
{
   if (!m_bConnected)
      return -1;

   int tr = size;
   while (tr > 0)
   {
      int r = SSL_read(m_pSSL, data + size - tr, tr);
      if (r <= 0)
         return -1;
      tr -= r;
   }
   return size;
}

int64_t SSLTransport::sendfile(const char* file, const int64_t& /*offset*/, const int64_t& size)
{
   if (!m_bConnected)
      return -1;

   ifstream ifs(file, ios::in | ios::binary);

   if (ifs.bad() || ifs.fail())
      return -1;

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
   ifs.close();

   return sent;
}

int64_t SSLTransport::recvfile(const char* file, const int64_t& /*offset*/, const int64_t& size)
{
   if (!m_bConnected)
      return -1;

   fstream ofs(file, ios::out | ios::binary);

   if (ofs.bad() || ofs.fail())
      return -1;

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
   ofs.close();
   return recd;
}

int SSLTransport::getLocalIP(string& ip)
{
   sockaddr_in addr;
   socklen_t size = sizeof(sockaddr_in);

   if (getsockname(m_iSocket, (sockaddr*)&addr, &size) < 0)
      return -1;

   char tmp[64];

   ip = inet_ntop(AF_INET, &(addr.sin_addr), tmp, 64);

   return 0;
}
