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
   Yunhong Gu, last updated 03/27/2011
*****************************************************************************/

#ifdef WIN32
   #include <time.h>
   #define atoll _atoi64
   #define snprintf sprintf_s
#endif
#include <fstream>
#include <cstring>
#include <cstdlib>
#include <sys/types.h>
#include <sys/stat.h>
#include <sector.h>

using namespace std;

SNode::SNode():
m_strName(""),
m_bIsDir(false),
m_llTimeStamp(0),
m_llSize(0),
m_strChecksum(""),
m_iReplicaNum(1),
m_iReplicaDist(65536)
{
   m_llTimeStamp = time(NULL);
   m_sLocation.clear();
   m_mDirectory.clear();
}

SNode::~SNode()
{
}

int SNode::serialize(char*& buf, bool includeReplica) const
{
   int namelen = m_strName.length();
   int size = namelen + 128;
   if (includeReplica)
      size = size + m_sLocation.size() * 64;
   try
   {
      buf = new char[size];
   }
   catch (...)
   {
      return -1;
   }
   snprintf(buf, size, "%d,%s,%d,%lld,%lld,%d,%d,%d", namelen, m_strName.c_str(), m_bIsDir, (long long int)m_llTimeStamp, (long long int)m_llSize, m_iReplicaNum, m_iMaxReplicaNum, m_iReplicaDist);
   if (includeReplica)
   {
     char* p = buf + strlen(buf);
     size -= strlen(buf);
     for (set<Address, AddrComp>::const_iterator i = m_sLocation.begin(); i != m_sLocation.end(); ++ i)
     {
        snprintf(p, size, ",%s,%d", i->m_strIP.c_str(), i->m_iPort);
        int len = strlen(p);
        p = p + len;
        size -= len;
     }
   }
   return 0;
}

int SNode::serialize(char*& buf) const
{
  return serialize(buf, true);
}

int SNode::deserialize(const char* buf)
{
   int size = strlen(buf) + 1;
   char* buffer = new char[size];
   char* tmp = buffer;

   bool stop = true;

   // file name
   strncpy(tmp, buf, size);
   for (unsigned int i = 0; i < strlen(tmp); ++ i)
   {
      if (tmp[i] == ',')
      {
         stop = false;
         tmp[i] = '\0';
         break;
      }
   }
   unsigned int namelen = atoi(tmp);

   if (stop)
   {
      delete [] buffer;
      return -1;
   }

   tmp = tmp + strlen(tmp) + 1;
   if (strlen(tmp) < namelen)
   {
      delete [] buffer;
      return -1;
   }
   tmp[namelen] = '\0';
   m_strName = tmp;

   stop = true;

   // restore dir 
   tmp = tmp + strlen(tmp) + 1;
   for (unsigned int i = 0; i < strlen(tmp); ++ i)
   {
      if (tmp[i] == ',')
      {
         stop = false;
         tmp[i] = '\0';
         break;
      }
   }
   m_bIsDir = (atoi(tmp) != 0);

   if (stop)
   {
      delete [] buffer;
      return -1;
   }
   stop = true;

   // restore timestamp
   tmp = tmp + strlen(tmp) + 1;
   for (unsigned int i = 0; i < strlen(tmp); ++ i)
   {
      if (tmp[i] == ',')
      {
         stop = false;
         tmp[i] = '\0';
         break;
      }
   }
   m_llTimeStamp = atoll(tmp);

   if (stop)
   {
      delete [] buffer;
      return -1;
   }
   stop = true;

   // restore size
   tmp = tmp + strlen(tmp) + 1;
   for (unsigned int i = 0; i < strlen(tmp); ++ i)
   {
      if (tmp[i] == ',')
      {
         stop = false;
         tmp[i] = '\0';
         break;
      }
   }
   m_llSize = atoll(tmp);

   if (stop)
   {
      delete [] buffer;
      return -1;
   }
   stop = true;

   // restore replication number
   tmp = tmp + strlen(tmp) + 1;
   for (unsigned int i = 0; i < strlen(tmp); ++ i)
   {
      if (tmp[i] == ',')
      {
         stop = false;
         tmp[i] = '\0';
         break;
      }
   }
   m_iReplicaNum = atoi(tmp);

   if (stop)
   {
      delete [] buffer;
      return -1;
   }
   stop = true;

   // restore max replication number
   tmp = tmp + strlen(tmp) + 1;
   for (unsigned int i = 0; i < strlen(tmp); ++ i)
   {
      if (tmp[i] == ',')
      {
         stop = false;
         tmp[i] = '\0';
         break;
      }
   }
   m_iMaxReplicaNum = atoi(tmp);

   if (stop)
   {
      delete [] buffer;
      return -1;
   }
   stop = true;

   // restore replication distance
   tmp = tmp + strlen(tmp) + 1;
   for (unsigned int i = 0; i < strlen(tmp); ++ i)
   {
      if (tmp[i] == ',')
      {
         stop = false;
         tmp[i] = '\0';
         break;
      }
   }
   m_iReplicaDist = atoi(tmp);

   // restore locations
   while (!stop)
   {
      tmp = tmp + strlen(tmp) + 1;

      stop = true;

      Address addr;
      for (unsigned int i = 0; i < strlen(tmp); ++ i)
      {
         if (tmp[i] == ',')
         {
            stop = false;
            tmp[i] = '\0';
            break;
         }
      }
      addr.m_strIP = tmp;

      if (stop)
      {
         delete [] buffer;
         return -1;
      }
      stop = true;

      tmp = tmp + strlen(tmp) + 1;
      for (unsigned int i = 0; i < strlen(tmp); ++ i)
      {
         if (tmp[i] == ',')
         {
            stop = false;
            tmp[i] = '\0';
            break;
         }
      }
      addr.m_iPort = atoi(tmp);

      m_sLocation.insert(addr);
   }

   delete [] buffer;
   return 0;
}
