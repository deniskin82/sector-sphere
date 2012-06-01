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
   Yunhong Gu, last updated 05/18/2011
*****************************************************************************/


#include <cstdlib>
#include <cstdio>
#include <cstring>
#include <sys/stat.h>
#include <iostream>
#include <sstream>

#include "conf.h"
#include "osportable.h"
#include "sector.h"
#include "meta.h"

#ifdef WIN32
   #define atoll _atoi64
#endif

using namespace std;

namespace 
{
    int parseItem(const string& input, string& path, time_t& val)
    {
       val = -1;

       //format: path num
       stringstream ssinput(input);
       ssinput >> path >> val;
       return val;
    }
}


ClientConf::ClientConf():
m_strUserName(),
m_strPassword(),
m_strCertificate(),
m_llMaxCacheSize(10000000),
m_iFuseReadAheadBlock(1000000),
m_llMaxWriteCacheSize(10000000),
m_strLog(),
m_iLogLevel(1)
{
}

int ClientConf::init(const string& path)
{
   ConfParser parser;
   Param param;

   if (0 != parser.init(path))
      return -1;

   while (parser.getNextParam(param) >= 0)
   {
      if (param.m_vstrValue.empty())
         continue;

      if ("MASTER_ADDRESS" == param.m_strName)
      {
         for (vector<string>::iterator i = param.m_vstrValue.begin(); i != param.m_vstrValue.end(); ++ i)
         {
            char buf[128];
            strncpy(buf, i->c_str(), 128);

            unsigned int p = 0;
            for (unsigned int n = strlen(buf); p < n; ++ p)
            {
               if (buf[p] == ':')
                  break;
            }
            buf[p] = '\0';

            Address addr;
            addr.m_strIP = buf;
            addr.m_iPort = atoi(buf + p + 1);
            m_sMasterAddr.insert(addr);
         }
      }
      else if ("USERNAME" == param.m_strName)
      {
         m_strUserName = param.m_vstrValue[0];
      }
      else if ("PASSWORD" == param.m_strName)
      {
         m_strPassword = param.m_vstrValue[0];
      }
      else if ("CERTIFICATE" == param.m_strName)
      {
         m_strCertificate = param.m_vstrValue[0];
      }
      else if ("MAX_CACHE_SIZE" == param.m_strName)
      {
         m_llMaxCacheSize = atoll(param.m_vstrValue[0].c_str()) * 1000000;
         if (m_llMaxCacheSize < 0)
            m_llMaxCacheSize = 0;
      }
      else if ("FUSE_READ_AHEAD_BLOCK" == param.m_strName)
      {
         m_iFuseReadAheadBlock = atoi(param.m_vstrValue[0].c_str()) * 1000000;
         if (m_iFuseReadAheadBlock < 0)
            m_iFuseReadAheadBlock = 0;
      }
      else if ("MAX_READ_CACHE_SIZE" == param.m_strName)
      {
         m_llMaxWriteCacheSize = atoll(param.m_vstrValue[0].c_str()) * 1000000;
         if (m_llMaxWriteCacheSize < 0)
            m_llMaxWriteCacheSize = 0;
      }
      else if ("LOG_LOCATION" == param.m_strName)
      {
         m_strLog = param.m_vstrValue[0];
      }
      else if ("LOG_LEVEL" == param.m_strName)
      {
         m_iLogLevel = atoi(param.m_vstrValue[0].c_str());
      }
      else if ("DIRECTORY_CACHE" == param.m_strName)
      {
         for (vector<string>::iterator i = param.m_vstrValue.begin(); i != param.m_vstrValue.end(); ++ i)
         {
            string path;
            time_t num;
            if (parseItem(*i, path, num) >= 0)
            {
               string rp = Metadata::revisePath(path);
               if (rp.length() > 0)
                  m_pathCache.push_back( DirCacheLifetimeSpec( path, num ) );
            }
         }
      }
      else
      {
         cerr << "unrecongnized client.conf parameter: " << param.m_strName << endl;
      }
   }

   parser.close();

   return 0;
}


int Session::loadInfo(const char* conf)
{
   string sector_home, conf_file_path;
   if (ConfLocation::locate(sector_home) < 0)
      conf_file_path = conf;
   else
      conf_file_path = sector_home + "/conf/client.conf";

   m_ClientConf.init(conf_file_path);

   if (m_ClientConf.m_sMasterAddr.empty())
   {
      cout << "please input the master address (e.g., 123.123.123.123:1234): ";
      string addr_str;
      cin >> addr_str;

      char buf[128];
      strncpy(buf, addr_str.c_str(), 128);

      unsigned int i = 0;
      for (unsigned int n = strlen(buf); i < n; ++ i)
      {
         if (buf[i] == ':')
            break;
      }
      buf[i] = '\0';

      Address addr;
      addr.m_strIP = buf;
      addr.m_iPort = atoi(buf + i + 1);
      m_ClientConf.m_sMasterAddr.insert(addr);
   }

   if (m_ClientConf.m_strUserName == "")
   {
      cout << "please input the user name: ";
      cin >> m_ClientConf.m_strUserName;
   }

   if (m_ClientConf.m_strPassword == "")
   {
      cout << "please input the password: ";
      cin >> m_ClientConf.m_strPassword;
   }

   SNode s;
   if (LocalFS::stat(m_ClientConf.m_strCertificate, s) < 0)
   {
      if (LocalFS::stat(sector_home + "/conf/master_node.cert", s) == 0)
      {
         m_ClientConf.m_strCertificate = sector_home + "/conf/master_node.cert";
      }
      else
      {
         m_ClientConf.m_strCertificate = "";
         cout << "WARNING: couldn't locate the master certificate, will try to download one from the master node.\n";
      }
   }

   return 0;
}

void Utility::print_error(int code)
{
   cerr << "ERROR: " << code << " " << SectorError::getErrorMsg(code) << endl;
}

int Utility::login(Sector& client)
{
   Session s;
   s.loadInfo("../conf/client.conf");

   int result = client.init();
   if (result < 0)
   {
      print_error(result);
      return -1;
   }

   bool master_conn = false;
   for (set<Address, AddrComp>::const_iterator i = s.m_ClientConf.m_sMasterAddr.begin(); i != s.m_ClientConf.m_sMasterAddr.end(); ++ i)
   {
      result = client.login(i->m_strIP, i->m_iPort,
                            s.m_ClientConf.m_strUserName, s.m_ClientConf.m_strPassword,
                            s.m_ClientConf.m_strCertificate.c_str());
      if (result < 0)
      {
         cerr << "trying to connect " << i->m_strIP << " " << i->m_iPort << endl;
         print_error(result);
      }
      else
      {
         master_conn = true;
         break;
      }
   }

   if (!master_conn)
   {
      cerr << "couldn't connect to any master. abort.\n";
      return -1;
   }

   return 0;
}

int Utility::logout(Sector& client)
{
   client.logout();
   client.close();
   return 0;
}

