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
   Yunhong Gu, last updated 08/19/2010
*****************************************************************************/


#ifndef __SECTOR_TOPOLOGY_H__
#define __SECTOR_TOPOLOGY_H__

#include <map>
#include <set>
#include <string>
#include <vector>
#ifndef WIN32
   #include <stdint.h>
#endif

#include "sector.h"

struct SlaveStatus
{
   static const int DOWN = 0;
   static const int NORMAL = 1;
   static const int DISKFULL = 2;
   static const int BAD = 3;
};

class SlaveNode
{
public:
   SlaveNode();

public:
   int m_iNodeID;					// unique slave node ID

   //Address m_Addr;
   std::string m_strIP;					// ip address (used for internal communication)
   std::string m_strPublicIP;				// public ip address, used for client communication. currently NOT used
   int m_iPort;						// GMP control port number
   int m_iDataPort;					// UDT data channel port number

   std::string m_strStoragePath;			// data storage path on the local file system
   std::string m_strAddr;               		// username@hostname/ip
   std::string m_strBase;               		// $SECTOR_HOME location on the slave
   std::string m_strOption;             		// slave options

   int64_t m_llAvailDiskSpace;				// available disk space
   int64_t m_llTotalFileSize;				// total data size

   int64_t m_llTimeStamp;				// last statistics refresh time
   int64_t m_llCurrMemUsed;				// physical memory used by the slave
   int64_t m_llCurrCPUUsed;				// CPU percentage used by the slave
   int64_t m_llTotalInputData;				// total network input
   int64_t m_llTotalOutputData;				// total network output
   std::map<std::string, int64_t> m_mSysIndInput;	// network input from each other slave
   std::map<std::string, int64_t> m_mSysIndOutput;	// network outout to each other slave
   std::map<std::string, int64_t> m_mCliIndInput;	// network input from each client
   std::map<std::string, int64_t> m_mCliIndOutput;	// network output to each client

   int64_t m_llLastUpdateTime;				// last update time
   int m_iStatus;					// 0: inactive 1: active-normal 2: active-disk full/read only 3: bad
   bool m_bDiskLowWarning;                              // if the disk low warning info has been updated to the master

   std::set<int> m_sBadVote;				// set of bad votes by other slaves
   int64_t m_llLastVoteTime;				// timestamp of last vote

   std::vector<int> m_viPath;				// topology path, from root to this node on the tree structure

   int m_iActiveTrans;					// number of active transactions

public:
   int deserialize(const char* buf, int size);
};

struct Cluster
{
   int m_iClusterID;
   std::vector<int> m_viPath;

   int m_iTotalNodes;
   int64_t m_llAvailDiskSpace;
   int64_t m_llTotalFileSize;

   int64_t m_llTotalInputData;
   int64_t m_llTotalOutputData;
   std::map<std::string, int64_t> m_mSysIndInput;
   std::map<std::string, int64_t> m_mSysIndOutput;
   std::map<std::string, int64_t> m_mCliIndInput;
   std::map<std::string, int64_t> m_mCliIndOutput;

   std::map<int, Cluster> m_mSubCluster;
   std::set<int> m_sNodes;
};

class Topology
{
friend class SlaveManager;

public:
   Topology();
   ~Topology();

public:
   int init(const char* topoconf);
   int lookup(const char* ip, std::vector<int>& path);

      // Functionality:
      //    compare two paths and return the longest matched path from the beginning
      // Parameters:
      //    0) [in] p1: first path;
      //    1) [in] p2: second path;
      // Returned value:
      //    length of longest matched patch.

   static unsigned int match(const std::vector<int>& p1, const std::vector<int>& p2);

      // Functionality:
      //    compute the distance between two IP addresses.
      // Parameters:
      //    0) [in] ip1: first IP address
      //    1) [in] ip2: second IP address
      // Returned value:
      //    0 if ip1 = ip2, 1 if on the same rack, etc.

   unsigned int distance(const char* ip1, const char* ip2);
   unsigned int min_distance(const Address& addr, const std::set<Address, AddrComp>& loclist);
   unsigned int min_distance(const std::vector<int>& path, const std::vector< std::vector<int> >& path_list);
   unsigned int max_distance(const std::vector<int>& path, const std::vector< std::vector<int> >& path_list);

   int getTopoDataSize();
   int serialize(char* buf, int& size);
   int deserialize(const char* buf, const int& size);

public:
   static int parseIPRange(const char* ip, uint32_t& digit, uint32_t& mask);
   static int parseTopo(const char* topo, std::vector<int>& tm);

private:
   unsigned int m_uiLevel;

   struct TopoMap
   {
      uint32_t m_uiIP;
      uint32_t m_uiMask;
      std::vector<int> m_viPath;
   };

   std::vector<TopoMap> m_vTopoMap;
};

#endif
