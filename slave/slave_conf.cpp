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
   Yunhong Gu, last updated 01/04/2011
*****************************************************************************/

#include <slave.h>
#include <conf.h>
#include <iostream>

#ifdef WIN32
   #define atoll _atoi64
#endif

using namespace std;
using namespace sector;

SlaveConf::SlaveConf():
m_strMasterHost(),
m_iMasterPort(0),
m_strHomeDir(),
m_llMaxDataSize(0),
m_iMaxServiceNum(0),
m_strLocalIP(),
m_strPublicIP(),
m_iClusterID(0),
m_MetaType(DEFAULT),
m_iLogLevel(0),
m_bVerbose(false)
{
}

int SlaveConf::init(const string& path)
{
   // initialize these values; a slave must call init()
   // cannot initialize the following values in constructor because they are reserved for global conf
   m_iMasterPort = 6000;
   m_strHomeDir = "./";
   m_llMaxDataSize = -1;
   m_iMaxServiceNum = 64;
   m_MetaType = MEMORY;
   m_iLogLevel = 1;

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
         char buf[128];
         strncpy(buf, param.m_vstrValue[0].c_str(), 128);

         unsigned int i = 0;
         for (unsigned int n = strlen(buf); i < n; ++ i)
         {
            if (buf[i] == ':')
               break;
         }

         buf[i] = '\0';
         m_strMasterHost = buf;
         m_iMasterPort = atoi(buf + i + 1);
      }
      else if ("DATA_DIRECTORY" == param.m_strName)
      {
         m_strHomeDir = param.m_vstrValue[0];
         if (m_strHomeDir.c_str()[m_strHomeDir.length() - 1] != '/')
            m_strHomeDir += "/";
      }
      else if ("MAX_DATA_SIZE" == param.m_strName)
      {
         m_llMaxDataSize = atoll(param.m_vstrValue[0].c_str()) * 1024 * 1024;
      }
      else if ("MAX_SERVICE_INSTANCE" == param.m_strName)
         m_iMaxServiceNum = atoi(param.m_vstrValue[0].c_str());
      else if ("LOCAL_ADDRESS" == param.m_strName)
         m_strLocalIP = param.m_vstrValue[0];
      else if ("PUBLIC_ADDRESS" == param.m_strName)
         m_strPublicIP = param.m_vstrValue[0];
      else if ("META_LOC" == param.m_strName)
      {
         if ("MEMORY" == param.m_vstrValue[0])
            m_MetaType = MEMORY;
         else if ("DISK" == param.m_vstrValue[0])
            m_MetaType = DISK;
      }
      else if ("LOG_LEVEL" == param.m_strName)
      {
         m_iLogLevel = atoi(param.m_vstrValue[0].c_str());
      }
      else
      {
         cerr << "unrecongnized system parameter: " << param.m_strName << endl;
      }
   }

   parser.close();

   return 0;
}

int SlaveConf::set(const SlaveConf* global)
{
   if (NULL == global)
      return 0;

   if (global->m_strMasterHost.length() > 0)
      m_strMasterHost = global->m_strMasterHost;

   if (global->m_iMasterPort > 0)
      m_iMasterPort = global->m_iMasterPort;

   if (global->m_strHomeDir.length() > 0)
      m_strHomeDir = global->m_strHomeDir;

   if (global->m_llMaxDataSize != 0)
      m_llMaxDataSize = global->m_llMaxDataSize;

   if (global->m_iMaxServiceNum > 0)
      m_iMaxServiceNum = global->m_iMaxServiceNum;

   if (global->m_strLocalIP.length() > 0)
      m_strLocalIP = global->m_strLocalIP;

   if (global->m_strPublicIP.length() > 0)
      m_strPublicIP = global->m_strPublicIP;

   if (global->m_iClusterID > 0)
      m_iClusterID = global->m_iClusterID;

   if (global->m_MetaType != DEFAULT)
      m_MetaType = global->m_MetaType;

   if (global->m_iLogLevel > 0)
      m_iLogLevel = global->m_iLogLevel;

   return 0;
}
