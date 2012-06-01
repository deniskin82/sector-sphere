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
   Yunhong Gu, last updated 04/24/2011
*****************************************************************************/

#include "security.h"
#include <sector.h>
#include <fstream>
#include <iostream>
#include <ctime>
#include <algorithm>
#include <signal.h>
#include <sys/types.h>
#ifndef WIN32
   #include <pthread.h>
   #include <sys/socket.h>
   #include <arpa/inet.h>
#endif

using namespace std;
using namespace sector;

int User::serialize(const vector<string>& input, string& buf) const
{
   buf = "";
   for (vector<string>::const_iterator i = input.begin(); i != input.end(); ++ i)
   {
      buf.append(*i);
      buf.append(";");
   }

   return buf.length() + 1;
}

SSource::~SSource()
{
}

SServer::SServer():
m_iKeySeed(time(0)),
m_iPort(0),
m_pSecuritySource(NULL)
{
}

int SServer::init(const int& port, const char* cert, const char* key)
{
   SSLTransport::init();

   m_iPort = port;

   if (m_SSL.initServerCTX(cert, key) < 0)
   {
      cerr << "cannot initialize security infomation with provided key/certificate.\n";
      return -1;
   }

   if (m_SSL.open(NULL, m_iPort) < 0)
   {
      cerr << "port is not available.\n";
      return -1;
   }

   m_SSL.listen();

   return 1;
}

void SServer::close()
{
   m_SSL.close();
   SSLTransport::destroy();
}

int SServer::setSecuritySource(SSource* src)
{
   if (NULL == src)
      return -1;

   m_pSecuritySource = src;
   return 0;
}

void SServer::run()
{
   // security source must be initialized and set before the server is running
   if (NULL == m_pSecuritySource)
      return;

#ifndef WIN32
   //ignore SIGPIPE
   sigset_t ps;
   sigemptyset(&ps);
   sigaddset(&ps, SIGPIPE);
   pthread_sigmask(SIG_BLOCK, &ps, NULL);
#endif

   while (true)
   {
      char ip[64];
      int port;
      SSLTransport* s = m_SSL.accept(ip, port);
      if (NULL == s)
         continue;

      // only a master node can query security information
      if (!m_pSecuritySource->matchMasterACL(ip))
      {
         s->close();
         continue;
      };

      Param* p = new Param;
      p->ip = ip;
      p->port = port;
      p->sserver = this;
      p->ssl = s;

#ifndef WIN32
      pthread_t t;
      pthread_create(&t, NULL, process, p);
      pthread_detach(t);
#else
      DWORD ThreadID;
      HANDLE hThread = CreateThread(NULL, 0, process, p, NULL, &ThreadID);
      CloseHandle (hThread);
#endif
   }
}

int32_t SServer::generateKey()
{
   return m_iKeySeed ++;
}

std::string getTimestamp() {
   time_t t = time(0);
   std::string asStr = ctime(&t); 
   asStr[ asStr.length() - 1 ] = ' ';
   return asStr;
}

template< size_t N >
std::string stringify( char (&buf)[ N ] )
{
   char* nul = std::find( buf, buf + N, '\0' );
   return std::string( buf, nul );
}

#ifndef WIN32
   void* SServer::process(void* p)
#else
   DWORD WINAPI SServer::process(void* p)
#endif
{
   SServer* self = ((Param*)p)->sserver;
   SSLTransport* s = ((Param*)p)->ssl;
   delete (Param*)p;

   while (true)
   {
      int32_t cmd;
      if (s->recv((char*)&cmd, 4) <= 0)
      {
         std::cout <<  getTimestamp() << "Error receiving command - exiting" << std::endl;
         goto EXIT;
      }

      // check if the security source has been updated (e.g., user account change)
      if (self->m_pSecuritySource->isUpdated())
         self->m_pSecuritySource->refresh();

      switch (cmd)
      {
      case 1: // slave node join
      {
         std::cout <<  getTimestamp() << "Start slave join" << std::endl;
         char ip[64];
         if (s->recv(ip, 64) <= 0)
            goto EXIT;

         int32_t key = self->generateKey();
         if (!self->m_pSecuritySource->matchSlaveACL(ip))
            key = SectorError::E_ACL;
         if (s->send((char*)&key, 4) <= 0)
            goto EXIT;

         std::cout <<  getTimestamp() << "Slave join done " << stringify( ip ) << " key " << key << std::endl;

         break;
      }

      case 2: // user login
      {

         std::cout << getTimestamp() << "Start processing user login" << std::endl;

         char user[64];
         if (s->recv(user, 64) <= 0)
            goto EXIT;
         char password[128];
         if (s->recv(password, 128) <= 0)
            goto EXIT;
         char ip[64];
         if (s->recv(ip, 64) <= 0)
            goto EXIT;

         int32_t key;
         User u;

         if (self->m_pSecuritySource->retrieveUser(user, password, ip, u) >= 0)
            key = self->generateKey();
         else
            key = SectorError::E_SECURITY;

         std::cout << getTimestamp() << "Login info: user " << stringify( user ) << " IP " << stringify( ip ) <<
           " key " << key << std::endl;

         if (s->send((char*)&key, 4) <= 0)
         {
            cout << "Error sending key " << key << std::endl;
            goto EXIT;
         }

         if (key > 0)
         {
            string buf;
            int32_t size;

            size = u.serialize(u.m_vstrReadList, buf);
            if ((s->send((char*)&size, 4) <= 0) || (s->send(buf.c_str(), size) <= 0))
               goto EXIT;

            size = u.serialize(u.m_vstrWriteList, buf);
            if ((s->send((char*)&size, 4) <= 0) || (s->send(buf.c_str(), size) <= 0))
               goto EXIT;

            int exec = u.m_bExec ? 1 : 0;
            if (s->send((char*)&exec, 4) <= 0)
               goto EXIT;
            std::cout << getTimestamp() << "Privileges sent to master" << std::endl;
         } else
         {
            std::cout << getTimestamp() << "Authorization error" << std::endl;
         }

         break;
      }

      case 3: // master join
      {
         std::cout <<  getTimestamp() << "Start master join" << std::endl;
         char ip[64];
         if (s->recv(ip, 64) <= 0)
            goto EXIT;

         int32_t res = 1;
         if (!self->m_pSecuritySource->matchMasterACL(ip))
            res = SectorError::E_ACL;
         if (s->send((char*)&res, 4) <= 0)
            goto EXIT;
         std::cout <<  getTimestamp() << "Master join complete " << stringify( ip ) << std::endl;

         break;
      }

      case 4: // master init
      {
         std::cout <<  getTimestamp() << "Start master init" << std::endl;
        
         int32_t key = self->generateKey();

         if (s->send((char*)&key, 4) <= 0)
            goto EXIT;
         std::cout <<  getTimestamp() << "Master init complete, key " << key << std::endl;

         break;
      }

      default:
         std::cout <<  getTimestamp() << "Strange command " << cmd << " - exiting" << std::endl;
         goto EXIT;
      }
   }

EXIT:
   s->close();
   delete s;
   return NULL;
}
