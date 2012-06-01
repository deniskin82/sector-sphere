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

#ifndef WIN32
   #include <sys/types.h>
   #include <sys/socket.h>
   #include <arpa/inet.h>
#else
   #include <winsock2.h>
   #include <ws2tcpip.h>
   #define atoll _atoi64
#endif
#include <osportable.h>
#include <iostream>
#include <fstream>
#include <filesrc.h>
#include <sector.h>
#include <conf.h>
#include <meta.h>

using namespace std;
using namespace sector;

FileSrc::FileSrc():
m_llLastUpdateTime(0)
{
}

FileSrc::~FileSrc()
{
}

int FileSrc::init(const void* param)
{
   m_strConfLoc = string((char*)param) + "/conf/";

   if (loadACL(m_vMasterACL, m_strConfLoc + "master_acl.conf") < 0)
   {
      cerr << "WARNING: failed to read master ACL configuration file master_acl.conf. No masters would be able to join.\n";
      return -1;
   }

   if (loadACL(m_vSlaveACL, m_strConfLoc + "slave_acl.conf") < 0)
   {
      cerr << "WARNING: failed to read slave ACL configuration file slave_acl.conf. No slaves would be able to join.\n";
      return -1;
   }

   if (loadUsers(m_mUsers, m_strConfLoc + "users") < 0)
   {
      cerr << "WARNING: no users account initialized.\n";
      return -1;
   }

   SNode s;
   LocalFS::stat(m_strConfLoc + "users", s);
   m_llLastUpdateTime = s.m_llTimeStamp;

   return 0;
}

bool FileSrc::matchMasterACL(const char* ip)
{
   return match(m_vMasterACL, ip);
}

bool FileSrc::matchSlaveACL(const char* ip)
{
   return match(m_vSlaveACL, ip);
}

int FileSrc::retrieveUser(const char* name, const char* password, const char* ip, User& user)
{
   map<string, User>::const_iterator i = m_mUsers.find(name);

   if (i == m_mUsers.end())
      return -1;

   if (i->second.m_strPassword != password)
      return -1;

   if (!match(i->second.m_vACL, ip))
      return -1;

   user = i->second;
   return 0;
}

bool FileSrc::match(const vector<IPRange>& acl, const char* ip)
{
   in_addr addr;
   if (inet_pton(AF_INET, ip, &addr) < 0)
      return false;

   for (vector<IPRange>::const_iterator i = acl.begin(); i != acl.end(); ++ i)
   {
      if ((addr.s_addr & i->m_uiMask) == (i->m_uiIP & i->m_uiMask))
      return true;
   }

   return false;
} 

int FileSrc::loadACL(vector<IPRange>& acl, const string& path)
{
   ifstream af(path.c_str(), ios::in | ios::binary);

   if (af.fail() || af.bad())
      return -1;

   acl.clear();

   char line[256];
   while (!af.eof())
   {
      af.getline(line, 256);
      if (*line == '\0')
         continue;

      IPRange ipr;
      if (parseIPRange(ipr, line) >= 0)
        acl.push_back(ipr);
   }

   af.close();

   return acl.size();
}

int FileSrc::loadUsers(map<string, User>& users, const string& path)
{
   vector<SNode> filelist;
   if (LocalFS::list_dir(path, filelist) < 0)
      return -1;

   users.clear();

   for (vector<SNode>::iterator i = filelist.begin(); i != filelist.end(); ++ i)
   {
      // skip "." and ".."
      if (i->m_strName.empty() || (i->m_strName.c_str()[0] == '.'))
         continue;

      if (i->m_bIsDir)
         continue;

      User u;
      if (parseUser(u, i->m_strName.c_str(), (path + "/" + i->m_strName).c_str()) >= 0)
         users[u.m_strName] = u;
   }

   return users.size();
}

int FileSrc::parseIPRange(IPRange& ipr, const char* ip)
{
   char* buf = new char[strlen(ip) + 128];
   unsigned int i = 0;
   for (unsigned int n = strlen(ip); i < n; ++ i)
   {
      if ('/' == ip[i])
         break;

      buf[i] = ip[i];
   }
   buf[i] = '\0';

   in_addr addr;
   if (inet_pton(AF_INET, buf, &addr) <= 0)
   {
      delete [] buf;
      return -1;
   }

   ipr.m_uiIP = addr.s_addr;
   ipr.m_uiMask = 0xFFFFFFFF;

   if (i == strlen(ip))
      return 0;

   if ('/' != ip[i])
   {
      delete [] buf;
      return -1;
   }
   ++ i;

   bool format = false;
   int j = 0;
   for (unsigned int n = strlen(ip); i < n; ++ i, ++ j)
   {
      if ('.' == ip[i])
         format = true;

      buf[j] = ip[i];
   }
   buf[j] = '\0';

   if (format)
   {
      //255.255.255.0
      if (inet_pton(AF_INET, buf, &addr) < 0)
      {
         delete [] buf;
         return -1;
      }
      ipr.m_uiMask = addr.s_addr;
   }
   else
   {
      char* p;
      int bit = strtol(buf, &p, 10);

      if ((p == buf) || (bit > 32) || (bit < 0))
      {
         delete [] buf;
         return -1;
      }

      if (bit < 32)
         ipr.m_uiMask = (bit == 0) ? 0 : htonl(~((1 << (32 - bit)) - 1));
   }

   delete [] buf;
   return 0;
}

int FileSrc::parseUser(User& user, const char* name, const char* ufile)
{
   user.m_iID = 0;
   user.m_strName = name;
   user.m_strPassword = "";
   user.m_vstrReadList.clear();
   user.m_vstrWriteList.clear();
   user.m_bExec = false;
   user.m_llQuota = -1;

   ConfParser parser;
   Param param;

   if (0 != parser.init(ufile))
      return -1;

   while (parser.getNextParam(param) >= 0)
   {
      if (param.m_vstrValue.empty())
         continue;

      if ("PASSWORD" == param.m_strName)
         user.m_strPassword = param.m_vstrValue[0];
      else if ("READ_PERMISSION" == param.m_strName)
      {
         for (vector<string>::iterator i = param.m_vstrValue.begin(); i != param.m_vstrValue.end(); ++ i)
         {
            string rp = Metadata::revisePath(*i);
            if (rp.length() > 0)
               user.m_vstrReadList.push_back(rp);
         }
      }
      else if ("WRITE_PERMISSION" == param.m_strName)
      {
         for (vector<string>::iterator i = param.m_vstrValue.begin(); i != param.m_vstrValue.end(); ++ i)
         {
            string rp = Metadata::revisePath(*i);
            if (rp.length() > 0)
               user.m_vstrWriteList.push_back(rp);
         }
      }
      else if ("EXEC_PERMISSION" == param.m_strName)
      {
         if (param.m_vstrValue[0] == "TRUE")
            user.m_bExec = true;
      }
      else if ("ACL" == param.m_strName)
      {
         for (vector<string>::iterator i = param.m_vstrValue.begin(); i != param.m_vstrValue.end(); ++ i)
         {
            IPRange ipr;
            parseIPRange(ipr, i->c_str());
            user.m_vACL.push_back(ipr);
         }
      }
      else if ("QUOTA" == param.m_strName)
         user.m_llQuota = atoll(param.m_vstrValue[0].c_str());
      else
         cerr << "unrecongnized user configuration parameter: " << param.m_strName << endl;
   }

   parser.close();

   return 0;
}

bool FileSrc::isUpdated()
{
   // TODO: fix this, "users" timestamp won't change

   SNode s;
   LocalFS::stat(m_strConfLoc + "users", s);
   if (m_llLastUpdateTime < s.m_llTimeStamp)
      return true;

   return false;
}

int FileSrc::refresh()
{
   if (loadACL(m_vMasterACL, m_strConfLoc + "master_acl.conf") < 0)
      return -1;

   if (loadACL(m_vSlaveACL, m_strConfLoc + "slave_acl.conf") < 0)
      return -1;

   if (loadUsers(m_mUsers, m_strConfLoc + "users") < 0)
      return -1;

   SNode s;
   LocalFS::stat(m_strConfLoc + "users", s);
   m_llLastUpdateTime = s.m_llTimeStamp;

   return 0;
}
