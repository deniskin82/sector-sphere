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
   Yunhong Gu, last updated 02/08/2011
*****************************************************************************/


#ifndef WIN32
   #include <unistd.h>
   #include <sys/types.h>
   #include <sys/socket.h>
   #include <arpa/inet.h>
   #include <netdb.h>
   #include <sys/time.h>
#else
   #include <winsock2.h>
   #include <ws2tcpip.h>
   #include <windows.h>
#endif

#include <cstdlib>
#include <cstring>
#include <fstream>
#include <sys/types.h>

#include <topology.h>

using namespace std;

SlaveNode::SlaveNode():
m_iNodeID(-1),
m_strIP("0.0.0.0"),
m_strPublicIP("0.0.0.0"),
m_iPort(0),
m_iDataPort(0),
m_strStoragePath("/tmp"),
m_strAddr(""),
m_strBase("/opt/sector"),
m_strOption(""),
m_llAvailDiskSpace(0),
m_llTotalFileSize(0),
m_llTimeStamp(-1),
m_llCurrMemUsed(0),
m_llCurrCPUUsed(0),
m_llTotalInputData(0),
m_llTotalOutputData(0),
m_llLastUpdateTime(0),
m_iStatus(1),
m_bDiskLowWarning(false),
m_llLastVoteTime(-1),
m_iActiveTrans(0)
{
}

int SlaveNode::deserialize(const char* buf, int size)
{
   char* p = (char*)buf;

   if (size < 56 + 4 * 4)
      return -1;

   m_llTimeStamp = *(int64_t*)p;
   m_llAvailDiskSpace = *(int64_t*)(p + 8);
   m_llTotalFileSize = *(int64_t*)(p + 16);
   m_llCurrMemUsed = *(int64_t*)(p + 24);
   m_llCurrCPUUsed = *(int64_t*)(p + 32);
   m_llTotalInputData = *(int64_t*)(p + 40);
   m_llTotalOutputData = *(int64_t*)(p + 48);

   p += 56;
   int n = *(int32_t*)p;
   p += 4;
   for (int i = 0; i < n; ++ i)
   {
      m_mSysIndInput[p] = *(int64_t*)(p + 16);
      p += 24;
   }
   n = *(int32_t*)p;
   p += 4;
   for (int i = 0; i < n; ++ i)
   {
      m_mSysIndOutput[p] = *(int64_t*)(p + 16);
      p += 24;
   }
   n = *(int32_t*)p;
   p += 4;
   for (int i = 0; i < n; ++ i)
   {
      m_mCliIndInput[p] = *(int64_t*)(p + 16);
      p += 24;
   }
   n = *(int32_t*)p;
   p += 4;
   for (int i = 0; i < n; ++ i)
   {
      m_mCliIndOutput[p] = *(int64_t*)(p + 16);
      p += 24;
   }

   return 0;
}


Topology::Topology():
m_uiLevel(1)
{
}

Topology::~Topology()
{
}

int Topology::init(const char* topoconf)
{
   ifstream ifs(topoconf, ios::in);

   if (ifs.bad() || ifs.fail())
   {
      // no configuration file, init default

      m_uiLevel = 1;
      TopoMap tm;
      tm.m_uiIP = 0;
      tm.m_uiMask = 0;
      tm.m_viPath.push_back(0);
      m_vTopoMap.push_back(tm);

      return 0;
   }

   char line[256];
   while (!ifs.eof())
   {
      ifs.getline(line, 256);

      if (('\0' == *line) || ('#' == *line))
         continue;

      // 192.168.136.0/24	/1/1

      unsigned int p = 0;
      for (unsigned int n = strlen(line); p < n; ++ p)
      {
         if ((line[p] == ' ') || (line[p] == '\t'))
            break;
      }

      string ip = string(line).substr(0, p);

      for (; p < strlen(line); ++ p)
      {
         if ((line[p] != ' ') && (line[p] != '\t'))
            break;
      }

      string topo = string(line).substr(p + 1, strlen(line));

      TopoMap tm;
      if (parseIPRange(ip.c_str(), tm.m_uiIP, tm.m_uiMask) < 0)
         return -1;
      if (parseTopo(topo.c_str(), tm.m_viPath) <= 0)
         return -1;

      m_vTopoMap.push_back(tm);

      if (m_uiLevel < tm.m_viPath.size())
         m_uiLevel = tm.m_viPath.size();
   }

   ifs.close();

   return m_vTopoMap.size();
}

int Topology::lookup(const char* ip, vector<int>& path)
{
   in_addr addr;
   if (inet_pton(AF_INET, ip, &addr) < 0)
      return -1;
   uint32_t digitip = ntohl(addr.s_addr);

   for (vector<TopoMap>::iterator i = m_vTopoMap.begin(); i != m_vTopoMap.end(); ++ i)
   {
      if ((digitip & i->m_uiMask) == (i->m_uiIP & i->m_uiMask))
      {
         path = i->m_viPath;
         return 0;
      }
   }

   for (unsigned int i = 0; i < m_uiLevel; ++ i)
      path.push_back(0);

   return -1;
}

unsigned int Topology::match(const vector<int>& p1, const vector<int>& p2)
{
   unsigned int level;
   if (p1.size() < p2.size())
      level = p1.size();
   else
      level = p2.size();

   for (unsigned int i = 0; i < level; ++ i)
   {
      if (p1[i] != p2[i])
         return i;
   }

   return level;
}

unsigned int Topology::distance(const char* ip1, const char* ip2)
{
   // Same IP address, return 0.
   if (strcmp(ip1, ip2) == 0)
      return 0;

   // Otherwise return the topology path difference, from 0 to level, plus 1.
   vector<int> p1, p2;
   lookup(ip1, p1);
   lookup(ip2, p2);
   return m_uiLevel - match(p1, p2) + 1;
}

unsigned int Topology::min_distance(const Address& addr, const set<Address, AddrComp>& loclist)
{
   if (loclist.find(addr) != loclist.end())
      return 0;

   unsigned int dist = 1000000000;
   for (set<Address, AddrComp>::const_iterator i = loclist.begin(); i != loclist.end(); ++ i)
   {
      unsigned int d = distance(addr.m_strIP.c_str(), i->m_strIP.c_str());
      if (d < dist)
         dist = d;
   }
   return dist;
}

unsigned int Topology::min_distance(const std::vector<int>& path, const std::vector< std::vector<int> >& path_list)
{
   if (path_list.empty())
      return m_uiLevel + 1;

   unsigned int md = 1000000000;
   for (vector< vector<int> >::const_iterator p = path_list.begin(); p != path_list.end(); ++ p)
   {
      unsigned int dist = m_uiLevel - match(path, *p) + 1;
      if (dist < md)
         md = dist;
   }

   return md;
}

unsigned int Topology::max_distance(const vector<int>& path, const vector< vector<int> >& path_list)
{
   if (path_list.empty())
      return m_uiLevel + 1;

   unsigned int md = 0;
   for (vector< vector<int> >::const_iterator p = path_list.begin(); p != path_list.end(); ++ p)
   {
      unsigned int dist = m_uiLevel - match(path, *p) + 1;
      if (dist > md)
         md = dist;
   }

   return md;
}

int Topology::getTopoDataSize()
{
   return 8 + m_vTopoMap.size() * (4 + 4 + m_uiLevel * 4);
}

int Topology::serialize(char* buf, int& size)
{
   if (size < int(8 + m_vTopoMap.size() * (4 + 4 + m_uiLevel * 4)))
      return -1;

   size = 8 + m_vTopoMap.size() * (4 + 4 + m_uiLevel * 4);
   int* p = (int*)buf;
   p[0] = m_vTopoMap.size();
   p[1] = m_uiLevel;
   p += 2;

   for (vector<TopoMap>::const_iterator i = m_vTopoMap.begin(); i != m_vTopoMap.end(); ++ i)
   {
      p[0] = i->m_uiIP;
      p[1] = i->m_uiMask;
      p += 2;
      for (vector<int>::const_iterator j = i->m_viPath.begin(); j != i->m_viPath.end(); ++ j)
         *p ++ = *j;
   }

   return 0;
}

int Topology::deserialize(const char* buf, const int& size)
{
   if (size < 8)
      return -1;

   int* p = (int*)buf;
   m_vTopoMap.resize(p[0]);
   m_uiLevel = p[1];
   p += 2;

   if (size < int(8 + m_vTopoMap.size() * (4 + 4 + m_uiLevel * 4)))
      return -1;

   for (vector<TopoMap>::iterator i = m_vTopoMap.begin(); i != m_vTopoMap.end(); ++ i)
   {
      i->m_uiIP = p[0];
      i->m_uiMask = p[1];
      p += 2;
      i->m_viPath.resize(m_uiLevel);
      for (vector<int>::iterator j = i->m_viPath.begin(); j != i->m_viPath.end(); ++ j)
         *j = *p ++;
   }

   return 0;
}

int Topology::parseIPRange(const char* ip, uint32_t& digit, uint32_t& mask)
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
   if (inet_pton(AF_INET, buf, &addr) < 0)
   {
      delete [] buf;
      return -1;
   }

   digit = ntohl(addr.s_addr);
   mask = 0xFFFFFFFF;

   if (i == strlen(ip))
      return 0;

   if ('/' != ip[i])
      return -1;
   ++ i;

   int j = 0;
   for (unsigned int n = strlen(ip); i < n; ++ i, ++ j)
      buf[j] = ip[i];
   buf[j] = '\0';

   char* p;
   int bit = strtol(buf, &p, 10);

   if ((p == buf) || (bit > 32) || (bit < 0))
   {
      delete [] buf;
      return -1;
   }

   mask <<= (32 - bit);

   delete [] buf;
   return 0;
}

int Topology::parseTopo(const char* topo, vector<int>& tm)
{
   int size = strlen(topo);
   char* buf = new char [size + 32];
   strncpy(buf, topo, size + 32);

   for (int i = 0; i < size; ++ i)
   {
      if (buf[i] == '/')
         buf[i] = '\0';
   }

   tm.clear();

   for (int i = 0; i < size; )
   {
      while ((buf[i] == '\0') && (i < size))
         ++ i;

      if (i >= size)
         break;

      //TODO, check atoi value
      tm.insert(tm.end(), atoi(buf + i));
      i += strlen(buf + i) + 1;
   }

   delete [] buf;

   return tm.size();
}
