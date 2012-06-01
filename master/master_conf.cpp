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
   Yunhong Gu, last updated 03/07/2011
*****************************************************************************/

#ifndef WIN32
   #include <arpa/inet.h>
   #include <sys/socket.h>
   #include <netdb.h>
#else
   #include <winsock2.h>
   #include <ws2tcpip.h>
   #define atoll _atoi64
#endif
#include <iostream>
#include <sstream>

#include "conf.h"
#include "master.h"
#include "meta.h"

using namespace std;
using namespace sector;

MasterConf::MasterConf():
m_iServerPort(0),
m_strSecServIP(),
m_iSecServPort(0),
m_iMaxActiveUser(1024),
m_strHomeDir("./"),
m_iReplicaNum(1),
m_iReplicaDist(65536),
m_MetaType(MEMORY),
m_iSlaveTimeOut(300),
m_iSlaveRetryTime(600),
m_llSlaveMinDiskSpace(10000000000LL),
m_iClientTimeOut(600),
m_iLogLevel(1),
m_iProcessThreads(1)              // 1 thread
{
}

std::string MasterConf::toString()
{
  std::stringstream buf;

  buf << "Master configuration:" << std::endl;
  buf << "SECTOR_PORT: " << m_iServerPort << std::endl;
  buf << "SECURITY_SERVER: " << m_strSecServIP << ":" << m_iSecServPort << std::endl;
  buf << "MAX_ACTIVE_USER: " << m_iMaxActiveUser << std::endl;
  buf << "DATA_DIRECTORY: " << m_strHomeDir << std::endl;
  buf << "REPLICA_NUM: " << m_iReplicaNum << std::endl;
  buf << "REPLICA_DIST: " << m_iReplicaDist << std::endl;
  buf << "SLAVE_TIMEOUT: " << m_iSlaveTimeOut << std::endl;
  buf << "LOST_SLAVE_RETRY_TIME: " << m_iSlaveRetryTime << std::endl;
  buf << "SLAVE_MIN_DISK_SPACE: " << m_llSlaveMinDiskSpace << std::endl;
  buf << "CLIENT_TIMEOUT: " << m_iClientTimeOut << std::endl;
  buf << "LOG_LEVEL: " << m_iLogLevel << std::endl;
  buf << "PROCESS_THREADS: " << m_iProcessThreads << std::endl;
  buf << "WRITE_ONCE_PROTECTION:" << std::endl;
  for (std::vector<string>::const_iterator i = m_vWriteOncePath.begin(); i != m_vWriteOncePath.end(); i++)
  {
    buf << "  " << *i << std::endl;
  }
  return buf.str();
}

int MasterConf::init(const string& path)
{
   ConfParser parser;
   Param param;

   if (0 != parser.init(path))
      return -1;

   while (parser.getNextParam(param) >= 0)
   {
      if (param.m_vstrValue.empty())
         continue;

      if ("SECTOR_PORT" == param.m_strName)
         m_iServerPort = atoi(param.m_vstrValue[0].c_str());
      else if ("SECURITY_SERVER" == param.m_strName)
      {
         char buf[128];
         strncpy(buf, param.m_vstrValue[0].c_str(), 128);

         unsigned int i = 0;
         for (unsigned int n = strlen(buf); i < n; ++ i)
         {
            if (buf[i] == ':')
               break;
         }

         buf[i] = '\0';
         m_strSecServIP = buf;
         m_iSecServPort = atoi(buf + i + 1);
      }
      else if ("MAX_ACTIVE_USER" == param.m_strName)
         m_iMaxActiveUser = atoi(param.m_vstrValue[0].c_str());
      else if ("DATA_DIRECTORY" == param.m_strName)
      {
         m_strHomeDir = param.m_vstrValue[0];
         if (m_strHomeDir.c_str()[m_strHomeDir.length() - 1] != '/')
            m_strHomeDir += "/";
      }
      else if ("REPLICA_NUM" == param.m_strName)
         m_iReplicaNum = atoi(param.m_vstrValue[0].c_str());
      else if ("REPLICA_DIST" == param.m_strName)
         m_iReplicaDist = atoi(param.m_vstrValue[0].c_str());
      else if ("META_LOC" == param.m_strName)
      {
         if ("MEMORY" == param.m_vstrValue[0])
            m_MetaType = MEMORY;
         else if ("DISK" == param.m_vstrValue[0])
            m_MetaType = DISK;
      }
      else if ("SLAVE_TIMEOUT" == param.m_strName)
      {
         m_iSlaveTimeOut = atoi(param.m_vstrValue[0].c_str());

         // slave reports every 30 - 60 seconds
         if (m_iSlaveTimeOut < 120)
            m_iSlaveTimeOut = 120;
      }
      else if ("LOST_SLAVE_RETRY_TIME" == param.m_strName)
      {
         m_iSlaveRetryTime = atoi(param.m_vstrValue[0].c_str());
         if (m_iSlaveRetryTime < 0)
            m_iSlaveRetryTime = 0;
      }
      else if ("SLAVE_MIN_DISK_SPACE" == param.m_strName)
      {
         m_llSlaveMinDiskSpace = atoll(param.m_vstrValue[0].c_str()) * 1000000;
      }
      else if ("CLIENT_TIMEOUT" == param.m_strName)
      {
         m_iClientTimeOut = atoi(param.m_vstrValue[0].c_str());

         // client only sends heartbeat every 60 - 120 seconds, so this value cannot be too small
         if (m_iClientTimeOut < 300)
            m_iClientTimeOut = 300;
      }
      else if ("LOG_LEVEL" == param.m_strName)
      {
         m_iLogLevel = atoi(param.m_vstrValue[0].c_str());
      }
      else if ("PROCESS_THREADS" == param.m_strName)
      {
         m_iProcessThreads = atoi(param.m_vstrValue[0].c_str());
      }
      else if ("WRITE_ONCE_PROTECTION" == param.m_strName)
      {
         for (vector<string>::iterator i = param.m_vstrValue.begin(); i != param.m_vstrValue.end(); ++ i)
         {
            string rp = Metadata::revisePath(*i);
            m_vWriteOncePath.push_back(rp);
         }
      }
      else
      {
         cerr << "unrecongnized system parameter: " << param.m_strName << endl;
      }
   }

   parser.close();

   return 0;
}


bool SlaveStartInfo::skip(const char* line)
{
   if (*line == '\0')
      return true;

   int i = 0;
   int n = strlen(line);
   for (; i < n; ++ i)
   {
      if ((line[i] != ' ') && (line[i] != '\t'))
         break;
   }

   if ((i == n) || (line[i] == '#'))
      return true;

   return false;
}

int SlaveStartInfo::parse(char* line, string& addr, string& base, string& param)
{
   //FORMAT: addr(username@IP) base [param]

   addr.clear();
   base.clear();
   param.clear();

   char* start = line;

   // skip all blanks and TABs
   while ((*start == ' ') || (*start == '\t'))
      ++ start;
   if (*start == '\0')
      return -1;

   char* end = start;
   while ((*end != ' ') && (*end != '\t') && (*end != '\0'))
      ++ end;
   if (*end == '\0')
      return -1;

   char orig = *end;
   *end = '\0';
   addr = start;
   *end = orig;


   // skip all blanks and TABs
   start = end;
   while ((*start == ' ') || (*start == '\t'))
      ++ start;
   if (*start == '\0')
      return -1;

   end = start;
   while ((*end != ' ') && (*end != '\t') && (*end != '\0'))
      ++ end;

   orig = *end;
   *end = '\0';
   base = start;
   *end = orig;


   // skip all blanks and TABs
   start = end;
   while ((*start == ' ') || (*start == '\t'))
      ++ start;

   // parameter is optional
   if (*start == '\0')
      return 0;

   // TODO: contionue to parse slave options
   param = start;

   return 0;
}

int SlaveStartInfo::parse(char* line, string& key, string& val)
{
   //FORMAT:  *KEY=VAL

   char* start = line + 1;
   while (*line != '=')
   {
      if (*line == '\0')
         return -1;
      line ++;
   }
   *line = '\0';

   key = start;

   val = line + 1;

   return 0;
}

string SlaveStartInfo::getIP(const char* name)
{
   struct addrinfo hints, *peer;

   memset(&hints, 0, sizeof(struct addrinfo));
   hints.ai_flags = AI_PASSIVE;
   hints.ai_family = AF_INET;

   if (0 != getaddrinfo(name, NULL, &hints, &peer))
      return name;

   char clienthost[NI_MAXHOST];
   if (getnameinfo(peer->ai_addr, peer->ai_addrlen, clienthost, sizeof(clienthost), NULL, 0, NI_NUMERICHOST) < 0)
      return name;

   freeaddrinfo(peer);

   return clienthost;
}

int Master::loadSlaveStartInfo(const std::string& file, set<SlaveStartInfo, SSIComp>& ssi)
{
   // starting slaves on the slave list
   ifstream ifs(file.c_str());
   if (ifs.fail())
   {
      cout << "no slave list found at " << file << endl;
      return -1;
   }

   string mh, mp, log, h, ds;

   while (!ifs.eof())
   {
      char line[256];
      line[0] = '\0';
      ifs.getline(line, 256);

      if (SlaveStartInfo::skip(line))
         continue;

      //option line must NOT be preceded with blank or TAB
      if (*line == '*')
      {
         // global configuration for slaves
         string key, val;
         if (SlaveStartInfo::parse(line, key, val) == 0)
         {
            if ("DATA_DIRECTORY" == key)
               h = val;
            else if ("LOG_LEVEL" == key)
               log = val;
            else if ("MASTER_ADDRESS" == key)
            {
               mh = val.substr(0, val.find(':'));
               mp = val.substr(mh.length() + 1, val.length() - mh.length() - 1);
            }
            else if ("MAX_DATA_SIZE" == key)
               ds = val;
            else
               cout << "WARNING: unrecognized option (ignored): " << line << endl;
         }

         continue;
      }

      SlaveStartInfo info;

      if (SlaveStartInfo::parse(line, info.m_strAddr, info.m_strBase, info.m_strOption) < 0)
      {
         cout << "WARNING: incorrect slave line format (skipped): " << line << endl;
         continue;
      }

      // all path name must be standardized as they will be compared for exact match
      info.m_strBase = Metadata::revisePath(info.m_strBase);

      string global_conf = "";
      if (!mh.empty())
         global_conf += string(" -mh ") + mh;
      if (!mp.empty())
         global_conf += string(" -mp ") + mp;
      if (!h.empty())
      {
         global_conf += string(" -h ") + h;
         info.m_strStoragePath = h;
      }
      if (!log.empty())
         global_conf += string(" -log ") + log;
      if (!ds.empty())
         global_conf += string(" -ds ") + ds;

      // slave specific config will overwrite global config; these will overwrite local config, if exists
      info.m_strOption = global_conf + " " + info.m_strOption;

      // get IP address
      info.m_strIP = SlaveStartInfo::getIP(info.m_strAddr.substr(info.m_strAddr.find('@') + 1, info.m_strAddr.length()).c_str());

      ssi.insert(info);
   }

   return 0;
}
