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
   Yunhong Gu, last updated 10/05/2010
*****************************************************************************/


#ifndef __SECTOR_SLAVEMGMT_H__
#define __SECTOR_SLAVEMGMT_H__

#include "osportable.h"

class Topology;

class SlaveManager
{
public:
   SlaveManager();
   ~SlaveManager();

public:
   int init(Topology* topo);

   int setSlaveMinDiskSpace(const int64_t& byteSize);

   int insert(SlaveNode& sn);
   int remove(int nodeid);

   bool checkDuplicateSlave(const std::string& ip, const std::string& path, int32_t& id, Address& addr);

public:
   int chooseReplicaNode(std::set<int>& loclist, SlaveNode& sn, const int64_t& filesize, const int rep_dist = 65536, const std::vector<int>* restrict_loc = NULL);
   int chooseIONode(std::set<int>& loclist, int mode, std::vector<SlaveNode>& sl, const SF_OPT& option, const int rep_dist = 65536, const std::vector<int>* restrict_loc = NULL);
   int chooseReplicaNode(std::set<Address, AddrComp>& loclist, SlaveNode& sn, const int64_t& filesize, const int rep_dist = 65536, const std::vector<int>* restrict_loc = NULL);
   int chooseIONode(std::set<Address, AddrComp>& loclist, int mode, std::vector<SlaveNode>& sl, const SF_OPT& option, const int rep_dist = 65536, const std::vector<int>* restrict_loc = NULL);
   int chooseSPENodes(const Address& client, std::vector<SlaveNode>& sl);
   int chooseLessReplicaNode(std::set<Address, AddrComp>& loclist, Address& addr);

public:
   int serializeTopo(char*& buf, int& size);
   int updateSlaveList(std::vector<Address>& sl, int64_t& last_update_time);
   int updateSlaveInfo(const Address& addr, const char* info, const int& len);
   int updateSlaveTS(const Address& addr);
   int checkBadAndLost(std::map<int, SlaveNode>& bad, std::map<int, SlaveNode>& lost, std::map<int, SlaveNode>& retry, std::map<int, SlaveNode>& dead, const int64_t& timeout, const int64_t& retrytime);
   int serializeSlaveList(char*& buf, int& size);
   int deserializeSlaveList(int num, const char* buf, int size);
   int getSlaveID(const Address& addr);
   int getSlaveAddr(const int& id, Address& addr);
   int voteBadSlaves(const Address& voter, int num, const char* buf);
   unsigned int getNumberOfClusters();
   unsigned int getNumberOfSlaves();
   int serializeClusterInfo(char*& buf, int& size);
   int serializeSlaveInfo(char*& buf, int& size);
   int getSlaveListByRack(std::map<int, Address>& sl, const std::string& topopath);
   int checkStorageBalance(std::map<int64_t, Address>& lowdisk, bool force);
   void incActTrans(const int& slaveid);
   void decActTrans(const int& slaveid);
   void getListActTrans( std::map<int, int>& lats );
   void getSlaveIPToClusterMap ( std::map<std::string, int>& );

public:
   uint64_t getTotalDiskSpace();
   void updateClusterStat();

private:
   bool checkduplicateslave_(const std::string& ip, const std::string& path, int32_t& id, Address& addr);
   void updateclusterstat_(Cluster& c);
   void updateclusterio_(Cluster& c, std::map<std::string, int64_t>& data_in, std::map<std::string, int64_t>& data_out, int64_t& total);
   int choosereplicanode_(std::set<int>& loclist, SlaveNode& sn, const int64_t& filesize, const int rep_dist, const std::vector<int>* restrict_loc);
   int findNearestNode(std::set<int>& loclist, const std::string& ip, SlaveNode& sn);

private:
   std::map<Address, int, AddrComp> m_mAddrList;		// list of slave addresses
   std::map<int, SlaveNode> m_mSlaveList;			// list of slaves

   Topology* m_pTopology;					// slave system topology definition
   Cluster m_Cluster;						// topology structure

   std::map<std::string, std::set<std::string> > m_mIPFSInfo;	// storage path on each slave node; used to avoid conflict

   int64_t m_llLastUpdateTime;					// last update time on the slave list

   int64_t m_llSlaveMinDiskSpace;				// minimum available disk space per slave node

private:
   CMutex m_SlaveLock;
};

#endif
