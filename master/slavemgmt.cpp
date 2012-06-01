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
   Yunhong Gu, last updated 03/19/2011
*****************************************************************************/


#ifndef WIN32
   #include <unistd.h>
   #include <sys/time.h>
   #include <sys/types.h>
#endif

#include <cstring>
#include <algorithm>

#include "common.h"
#include "../common/log.h"
#include "meta.h"
#include "slavemgmt.h"
#include "topology.h"

using namespace std;

namespace
{
   inline logger::LogAggregate& log()
   {
      static logger::LogAggregate& myLogger = logger::getLogger( "SlaveMgr" );
      static bool                  setLogLevelYet = false;

      if( !setLogLevelYet )
      {
         setLogLevelYet = true;
         myLogger.setLogLevel( logger::Debug );
      }

      return myLogger;
   }

   struct SortByFreeSpace {
      explicit SortByFreeSpace( const std::map<int,SlaveNode>& slaveList ) : slaveList( slaveList ) {}
      bool operator()( int lhs, int rhs ) const { return slaveList.find( lhs )->second.m_llAvailDiskSpace > slaveList.find( rhs )->second.m_llAvailDiskSpace; }
     private:
      const std::map<int,SlaveNode>& slaveList;
   };
}


SlaveManager::SlaveManager():
m_pTopology(NULL),
m_llLastUpdateTime(-1),
m_llSlaveMinDiskSpace(10000000000LL)
{
}

SlaveManager::~SlaveManager()
{
}

int SlaveManager::init(Topology* topo)
{
   m_Cluster.m_iClusterID = 0;
   m_Cluster.m_iTotalNodes = 0;
   m_Cluster.m_llAvailDiskSpace = 0;
   m_Cluster.m_llTotalFileSize = 0;
   m_Cluster.m_llTotalInputData = 0;
   m_Cluster.m_llTotalOutputData = 0;
   m_Cluster.m_viPath.clear();

   if (NULL == topo)
      return -1;

   m_pTopology = topo;

   Cluster* pc = &m_Cluster;

   // insert 0/0/0/....
   for (unsigned int i = 0; i < m_pTopology->m_uiLevel; ++ i)
   {
      Cluster c;
      c.m_iClusterID = 0;
      c.m_iTotalNodes = 0;
      c.m_llAvailDiskSpace = 0;
      c.m_llTotalFileSize = 0;
      c.m_llTotalInputData = 0;
      c.m_llTotalOutputData = 0;
      c.m_viPath = pc->m_viPath;
      c.m_viPath.push_back(0);

      pc->m_mSubCluster[0] = c;
      pc = &(pc->m_mSubCluster[0]);
   }

   for (vector<Topology::TopoMap>::iterator i = m_pTopology->m_vTopoMap.begin(); i != m_pTopology->m_vTopoMap.end(); ++ i)
   {
      pc = &m_Cluster;

      for (vector<int>::iterator l = i->m_viPath.begin(); l != i->m_viPath.end(); ++ l)
      {
         if (pc->m_mSubCluster.find(*l) == pc->m_mSubCluster.end())
         {
            Cluster c;
            c.m_iClusterID = *l;
            c.m_iTotalNodes = 0;
            c.m_llAvailDiskSpace = 0;
            c.m_llTotalFileSize = 0;
            c.m_llTotalInputData = 0;
            c.m_llTotalOutputData = 0;
            c.m_viPath = pc->m_viPath;
            c.m_viPath.push_back(*l);
            pc->m_mSubCluster[*l] = c;
         }
         pc = &(pc->m_mSubCluster[*l]);
      }
   }

   m_llLastUpdateTime = CTimer::getTime();

   return 1;
}

int SlaveManager::setSlaveMinDiskSpace(const int64_t& byteSize)
{
   m_llSlaveMinDiskSpace = byteSize;
   return 0;
}

int SlaveManager::insert(SlaveNode& sn)
{
   CGuardEx sg(m_SlaveLock);

   int id = 0;
   Address addr;
   if (checkduplicateslave_(sn.m_strIP, sn.m_strStoragePath, id, addr))
      return -1;

   sn.m_llLastUpdateTime = CTimer::getTime();
   sn.m_llLastVoteTime = CTimer::getTime();
   if (sn.m_llAvailDiskSpace > m_llSlaveMinDiskSpace)
      sn.m_iStatus = SlaveStatus::NORMAL;
   else
      sn.m_iStatus = SlaveStatus::DISKFULL;
   m_pTopology->lookup(sn.m_strIP.c_str(), sn.m_viPath);

   m_mSlaveList[sn.m_iNodeID] = sn;

   addr.m_strIP = sn.m_strIP;
   addr.m_iPort = sn.m_iPort;
   m_mAddrList[addr] = sn.m_iNodeID;

   Cluster* sc = &m_Cluster;
   map<int, Cluster>::iterator pc = sc->m_mSubCluster.end();
   for (vector<int>::iterator i = sn.m_viPath.begin(); ;)
   {
      pc = sc->m_mSubCluster.find(*i);
      if (pc == sc->m_mSubCluster.end())
      {
         //impossble
         break;
      }

      pc->second.m_iTotalNodes ++;
      if (sn.m_llAvailDiskSpace > m_llSlaveMinDiskSpace)
         pc->second.m_llAvailDiskSpace += sn.m_llAvailDiskSpace - m_llSlaveMinDiskSpace;
      pc->second.m_llTotalFileSize += sn.m_llTotalFileSize;

      if (++ i == sn.m_viPath.end())
         break;

      sc = &(pc->second);
   }

   if (pc != sc->m_mSubCluster.end())
   {
      pc->second.m_sNodes.insert(sn.m_iNodeID);
   }
   else
   {
      // IMPOSSIBLE
   }

   map<string, set<string> >::iterator i = m_mIPFSInfo.find(sn.m_strIP);
   if (i == m_mIPFSInfo.end())
      m_mIPFSInfo[sn.m_strIP].insert(Metadata::revisePath(sn.m_strStoragePath));
   else
      i->second.insert(sn.m_strStoragePath);

   m_llLastUpdateTime = CTimer::getTime();

   return 1;
}

int SlaveManager::remove(int nodeid)
{
   CGuardEx sg(m_SlaveLock);

   map<int, SlaveNode>::iterator sn = m_mSlaveList.find(nodeid);

   if (sn == m_mSlaveList.end())
      return -1;

   Address addr;
   addr.m_strIP = sn->second.m_strIP;
   addr.m_iPort = sn->second.m_iPort;
   m_mAddrList.erase(addr);

   vector<int> path;
   m_pTopology->lookup(sn->second.m_strIP.c_str(), path);
   map<int, Cluster>* sc = &(m_Cluster.m_mSubCluster);
   map<int, Cluster>::iterator pc = sc->end();
   for (vector<int>::iterator i = path.begin(); i != path.end(); ++ i)
   {
      if ((pc = sc->find(*i)) == sc->end())
      {
         // something wrong
         break;
      }

      pc->second.m_iTotalNodes --;
      if (sn->second.m_llAvailDiskSpace > m_llSlaveMinDiskSpace)
         pc->second.m_llAvailDiskSpace -= sn->second.m_llAvailDiskSpace - m_llSlaveMinDiskSpace;
      pc->second.m_llTotalFileSize -= sn->second.m_llTotalFileSize;

      sc = &(pc->second.m_mSubCluster);
   }

   pc->second.m_sNodes.erase(nodeid);

   map<string, set<string> >::iterator i = m_mIPFSInfo.find(sn->second.m_strIP);
   if (i != m_mIPFSInfo.end())
   {
      i->second.erase(sn->second.m_strStoragePath);
      if (i->second.empty())
         m_mIPFSInfo.erase(i);
   }
   else
   {
      //something wrong
   }

   m_mSlaveList.erase(sn);

   m_llLastUpdateTime = CTimer::getTime();

   return 1;
}

bool SlaveManager::checkDuplicateSlave(const string& ip, const string& path, int32_t& id, Address& addr)
{
   CGuardEx sg(m_SlaveLock);

   return checkduplicateslave_(ip, path, id, addr);
}

bool SlaveManager::checkduplicateslave_(const string& ip, const string& path, int32_t& id, Address& addr)
{
   map<string, set<string> >::iterator i = m_mIPFSInfo.find(ip);
   if (i == m_mIPFSInfo.end())
      return false;

   string revised_path = Metadata::revisePath(path);
   for (set<string>::iterator j = i->second.begin(); j != i->second.end(); ++ j)
   {
      // if there is overlap between the two storage paths, it means that there is a conflict
      // the new slave should be rejected in this case

      vector<string> dir1;
      vector<string> dir2;
      Metadata::parsePath(*j, dir1);
      Metadata::parsePath(path, dir2);

      int n = (dir1.size() < dir2.size()) ? dir1.size() : dir2.size();
      bool match = true;
      for (int i = 0; i < n; ++ i)
      {
         if (dir1[i] != dir2[i])
         {
            match = false;
            break;
         }
      }

      if (!match)
         continue;

      //TODO: optimize this search
      id = -1;
      for (map<int, SlaveNode>::const_iterator s = m_mSlaveList.begin(); s != m_mSlaveList.end(); ++ s)
      {
         if ((s->second.m_strIP == ip) && (s->second.m_strStoragePath == *j))
         {
            id = s->first;
            addr.m_strIP = s->second.m_strIP;
            addr.m_iPort = s->second.m_iPort;
            break;
         }
      }

      return true;
   }

   return false;
}

int SlaveManager::chooseReplicaNode(set<int>& loclist, SlaveNode& sn, const int64_t& filesize, const int rep_dist, const vector<int>* restrict_loc)
{
   CGuardEx sg(m_SlaveLock);
   return choosereplicanode_(loclist, sn, filesize, rep_dist, restrict_loc);
}

int SlaveManager::choosereplicanode_(set<int>& loclist, SlaveNode& sn, const int64_t& filesize, const int rep_dist, const vector<int>* restrict_loc)
{
   // If all source nodes are busy, we should skip the replica.
   /*
   bool idle = false;
   for (set<int>::const_iterator i = loclist.begin(); i != loclist.end(); ++ i)
   {
      // TODO: each slave may set a capacity limit, which could be more than 2 active trasactions.
      if (m_mSlaveList[*i].m_iActiveTrans <= 1)
      {
         idle = true;
         break;
      }
   }
   if (!idle)
      return -1;
   */

   vector< set<int> > avail;
   avail.resize(m_pTopology->m_uiLevel + 2);

   // find the topology of current replicas
   vector< vector<int> > locpath;
   set<string> usedIP;
   for (set<int>::iterator i = loclist.begin(); i != loclist.end(); ++ i)
   {
      map<int, SlaveNode>::iterator p = m_mSlaveList.find(*i);
      if (p == m_mSlaveList.end())
         continue;
      locpath.push_back(p->second.m_viPath);
      usedIP.insert(p->second.m_strIP);
   }

   // TODO: this should not be calcuated each time.

   for (map<int, SlaveNode>::iterator i = m_mSlaveList.begin(); i != m_mSlaveList.end(); ++ i)
   {
      // skip bad&lost slaves
      if (i->second.m_iStatus != SlaveStatus::NORMAL)
         continue;

      // only nodes with more than minimum availale disk space are chosen
      if (i->second.m_llAvailDiskSpace < (m_llSlaveMinDiskSpace + filesize))
         continue;

      // cannot replicate to a node already having the data
      if (loclist.find(i->first) != loclist.end())
         continue;

      // if a location restriction is applied to the file, only limited nodes can be chosen
      if ((NULL != restrict_loc) && (!restrict_loc->empty()))
      {

         int clusterId = i->second.m_viPath.back();
         if( std::find( restrict_loc->begin(), restrict_loc->end(), clusterId ) == restrict_loc->end() ) 
         {
            continue;
         }
      }

      // Calculate the distance from this slave node to the current replicas
      // We want maximize the distance to closest node.
      int level = m_pTopology->min_distance(i->second.m_viPath, locpath);

      // if users define a replication distance, then only nodes within rep_dist can be chosen
      if ((rep_dist >= 0) && (level > rep_dist))
         continue;

      // if level is 1, it is possible there is a replica on slave on same node (same IP)
      // we do not want to have more than 1 replica per node
      // this can happen is several slaves started per node, one slave per volume
      if ( level == 1 )
      {
        set<string>::iterator fs = usedIP.find(i->second.m_strIP);
        if (fs != usedIP.end())
          continue;
      }

      // level <= m_pTopology->m_uiLevel + 1.
      // We do not want to replicate on the same node (level == 0), even if they are different slaves.
      if (level > 0)
         avail[level].insert(i->first);
   }

   set<int>* candidate = NULL;
   // choose furthest node within replica distance
   for (int i = m_pTopology->m_uiLevel + 1; i > 0; -- i)
   {
      if (!avail[i].empty())
      {
         candidate = &avail[i];
         break;
      }
   }

   if (NULL == candidate)
      return SectorError::E_NODISK;

/*
   int lvl = 0;
   log().trace << "Available slaves" << std::endl;
   for ( vector <set <int> >::const_iterator vi = avail.begin(); vi != avail.end(); ++vi)
   {
      log().trace << "Level " << lvl << " ";
      for ( set< int >::const_iterator si = vi->begin(); si != vi->end(); ++si)
        log().trace << *si << ",";
      log().trace << std::endl;
      lvl++;
   }
  
   log().trace << "Candidate slaves ";
   for (set<int>::const_iterator cs = candidate->begin(); cs != candidate->end(); ++ cs)
     log().trace << *cs << ",";
   log().trace << std::endl;
*/
/*
   int64_t totalFreeSpaceOnCandidates = 0;
   for( set<int>::iterator j = candidate->begin(); j != candidate->end(); ++j )
   {
       totalFreeSpaceOnCandidates += m_mSlaveList[*j].m_llAvailDiskSpace;
   }
   int64_t avgFreeSpaceOnCandidates = totalFreeSpaceOnCandidates / candidate->size();

   for( set<int>::iterator c = candidate->begin(); c != candidate->end(); )
       if( m_mSlaveList[*c].m_llAvailDiskSpace < avgFreeSpaceOnCandidates )
       {
           set<int>::iterator tmp = c++;
           candidate->erase( tmp );
       }
       else
           ++c;
*/

   std::vector<int> slavesOrderedByFreeSpace( candidate->begin(), candidate->end() );
   std::sort( slavesOrderedByFreeSpace.begin(), slavesOrderedByFreeSpace.end(), SortByFreeSpace( m_mSlaveList ) );

   slavesOrderedByFreeSpace.resize( slavesOrderedByFreeSpace.size() / 2 + slavesOrderedByFreeSpace.size() % 2 );
   candidate->clear();
   candidate->insert( slavesOrderedByFreeSpace.begin(), slavesOrderedByFreeSpace.end() );

   // Choose a random node.
   timeval t;
   gettimeofday(&t, 0);
   srand(t.tv_usec);
   int r = int(candidate->size() * (double(rand()) / RAND_MAX));
   set<int>::iterator n = candidate->begin();
   for (int i = 0; i < r; ++ i)
      n ++;

   // If there is no other active transaction on this node, return.
   if( m_mSlaveList.find( *n ) == m_mSlaveList.end() )
      log().error << __PRETTY_FUNCTION__ << ": (1) about to add new slave to list " << *n << std::endl;

   sn = m_mSlaveList[*n];
   if (sn.m_iActiveTrans == 0)
   {
//      log().trace << "Choosen slave without active transactions " << sn.m_iNodeID << std::endl;
      return 1;
   }

   // Choose node with lowest number of active transactions.
   set<int>::iterator s = n;
   do
   {
      if (++s == candidate->end())
         s = candidate->begin();
      if( m_mSlaveList.find( *s ) == m_mSlaveList.end() )
         log().error << __PRETTY_FUNCTION__ << ": (2) about to add new slave to list " << *s << std::endl;
      if (m_mSlaveList[*s].m_iActiveTrans < sn.m_iActiveTrans)
      {
         sn = m_mSlaveList[*s];
         if (sn.m_iActiveTrans == 0)
            break;
      }
   } while (s != n);

//   log().trace << "Choosen slave with lowest no of active transactions " << sn.m_iNodeID << std::endl;

   return 1;
}
 
int SlaveManager::chooseIONode(set<int>& loclist, int mode, vector<SlaveNode>& sl, const SF_OPT& option, const int rep_dist, const vector<int>* restrict_loc)
{
   CGuardEx sg(m_SlaveLock);

   timeval t;
   gettimeofday(&t, 0);
   srand(t.tv_usec);

   sl.clear();

   if (m_mSlaveList.empty())
      return SectorError::E_NODISK;

   if (!loclist.empty())
   {
      SlaveNode sn;
      int rc = findNearestNode(loclist, option.m_strHintIP, sn);
      if( rc < 0 )
          return rc;

      sl.push_back(sn);

      // if this is a READ_ONLY operation, one node is enough
      if ((mode & SF_MODE::WRITE) == 0)
         return sl.size();

      // the first node will be the closest to the client; the client writes to that node only
      for (set<int>::iterator i = loclist.begin(); i != loclist.end(); i ++)
      {
         if (*i == sn.m_iNodeID)
            continue;

         if( m_mSlaveList.find( *i ) == m_mSlaveList.end() ) 
            log().error << __PRETTY_FUNCTION__ << ":  about to add new slave to list " << *i << std::endl;

         sl.push_back(m_mSlaveList[*i]);
      }
   }
   else
   {
      // no available nodes for READ_ONLY operation
      if ((mode & SF_MODE::WRITE) == 0)
         return 0;

      //TODO: optimize the node selection process; no need to scan all nodes

      set<int> avail;

      vector<int> path_limit;
      if (option.m_strCluster.c_str()[0] != '\0')
         Topology::parseTopo(option.m_strCluster.c_str(), path_limit);

      for (map<int, SlaveNode>::iterator i = m_mSlaveList.begin(); i != m_mSlaveList.end(); ++ i)
      {
         // skip bad & lost nodes
         if (i->second.m_iStatus != SlaveStatus::NORMAL) {
            continue;
         }

         // if client specifies a cluster ID, then only nodes on the cluster are chosen
         if (!path_limit.empty())
         {
            int clusterId = i->second.m_viPath.back();
            if( std::find( path_limit.begin(), path_limit.end(), clusterId ) == path_limit.end() ) {
               continue;
            }
         }

         // if there is location restriction on the file, check path as well
         if ((NULL != restrict_loc) && (!restrict_loc->empty()))
         {
            int clusterId = i->second.m_viPath.back();
            if( std::find( restrict_loc->begin(), restrict_loc->end(), clusterId ) == restrict_loc->end() ) {
               continue;
            }
         }

         // only nodes with more than minimum available disk space are chosen
         if (i->second.m_llAvailDiskSpace > (m_llSlaveMinDiskSpace + option.m_llReservedSize))
            avail.insert(i->first);
      }

      if (avail.empty())
         return SectorError::E_NODISK;

     
      SlaveNode sn;
      findNearestNode(avail, option.m_strHintIP, sn);

      sl.push_back(sn);

      // otherwise choose more nodes for immediate replica
      for (int i = 0; i < option.m_iReplicaNum - 1; ++ i)
      {
         set<int> locid;
         for (vector<SlaveNode>::iterator j = sl.begin(); j != sl.end(); ++ j)
            locid.insert(j->m_iNodeID);

         if (choosereplicanode_(locid, sn, option.m_llReservedSize, rep_dist, restrict_loc) <= 0)
            break;

         sl.push_back(sn);
      }
   }

   return sl.size();
}

int SlaveManager::chooseReplicaNode(set<Address, AddrComp>& loclist, SlaveNode& sn, const int64_t& filesize, const int rep_dist, const vector<int>* restrict_loc)
{
   set<int> locid;
   for (set<Address, AddrComp>::iterator i = loclist.begin(); i != loclist.end(); ++ i)
   {
      locid.insert(m_mAddrList[*i]);
   }

   return chooseReplicaNode(locid, sn, filesize, rep_dist, restrict_loc);
}

int SlaveManager::chooseIONode(set<Address, AddrComp>& loclist, int mode, vector<SlaveNode>& sl, const SF_OPT& option, const int rep_dist, const vector<int>* restrict_loc)
{
   set<int> locid;
   for (set<Address, AddrComp>::iterator i = loclist.begin(); i != loclist.end(); ++ i)
   {
      locid.insert(m_mAddrList[*i]);
   }

   return chooseIONode(locid, mode, sl, option, rep_dist, restrict_loc);
}

int SlaveManager::chooseSPENodes(const Address& /*client*/, vector<SlaveNode>& sl)
{
   for (map<int, SlaveNode>::iterator i = m_mSlaveList.begin(); i != m_mSlaveList.end(); ++ i)
   {
      // skip bad&lost slaves
      if (i->second.m_iStatus != SlaveStatus::NORMAL)
         continue;

      // only nodes with more than minimum available disk space are chosen
      if (i->second.m_llAvailDiskSpace <= m_llSlaveMinDiskSpace)
         continue;

      sl.push_back(i->second);

      //TODO:: add more creteria to choose nodes
   }

   return sl.size();
}

int SlaveManager::chooseLessReplicaNode(std::set<Address, AddrComp>& loclist, Address& addr)
{
   if (loclist.empty())
      return -1;

   int min_dist = 1024;
   int64_t min_avail_space = -1;


   // TODO (sergey)

   // Remove a node such that the rest has the max-min distance;
   // When the first rule ties, choose one with least available space.
   for (set<Address, AddrComp>::iterator i = loclist.begin(); i != loclist.end(); ++ i)
   {
      // TODO: optimize this by using ID instead of address.
      set<Address, AddrComp> tmp = loclist;
      tmp.erase(*i);
      int slave_id = m_mAddrList[*i];
      SlaveNode sn = m_mSlaveList[slave_id];
      if( m_mSlaveList.find( slave_id ) == m_mSlaveList.end() )
         log().error << __PRETTY_FUNCTION__ << ": about to add new slave to list " << slave_id << std::endl;
      int64_t availDiskSpace = sn.m_llAvailDiskSpace;

      int dist = m_pTopology->min_distance(*i, tmp);
      if (dist < min_dist)
      {
         addr = *i;
         min_dist = dist;
         min_avail_space = availDiskSpace;
      }
      else if (dist == min_dist)
      {
         if (availDiskSpace < min_avail_space)
         {
            addr = *i;
            min_avail_space = sn.m_llAvailDiskSpace;
         }
      }
   }

   return 0;
}

int SlaveManager::serializeTopo(char*& buf, int& size)
{
   CGuardEx sg(m_SlaveLock);

   buf = NULL;
   size = m_pTopology->getTopoDataSize();
   buf = new char[size];
   m_pTopology->serialize(buf, size);

   return size;
}

int SlaveManager::updateSlaveList(vector<Address>& sl, int64_t& last_update_time)
{
   CGuardEx sg(m_SlaveLock);

   if (last_update_time < m_llLastUpdateTime)
   {
      sl.clear();
      for (map<int, SlaveNode>::iterator i = m_mSlaveList.begin(); i != m_mSlaveList.end(); ++ i)
      {
         Address addr;
         addr.m_strIP = i->second.m_strIP;
         addr.m_iPort = i->second.m_iPort;
         sl.push_back(addr);
      }
   }

   last_update_time = CTimer::getTime();

   return sl.size();
}

int SlaveManager::updateSlaveInfo(const Address& addr, const char* info, const int& len)
{
   CGuardEx sg(m_SlaveLock);

   map<Address, int, AddrComp>::iterator a = m_mAddrList.find(addr);
   if (a == m_mAddrList.end())
      return -1;

   map<int, SlaveNode>::iterator s = m_mSlaveList.find(a->second);
   if (s == m_mSlaveList.end())
   {
      //THIS SHOULD NOT HAPPEN
      return -1;
   }

   if (s->second.m_iStatus == SlaveStatus::DOWN)
   {
      // "lost" slaves must be restarted, as files might have been changed
      return -1;
   }

   s->second.m_llLastUpdateTime = CTimer::getTime();
   s->second.m_llTimeStamp = CTimer::getTime();
   s->second.deserialize(info, len);

   if (s->second.m_iStatus == SlaveStatus::BAD)
      return -1;

   if (s->second.m_llAvailDiskSpace <= m_llSlaveMinDiskSpace)
      s->second.m_iStatus = SlaveStatus::DISKFULL;
   else
   {
      s->second.m_iStatus = SlaveStatus::NORMAL;
      s->second.m_bDiskLowWarning = false;
   }

   return 0;
}

int SlaveManager::updateSlaveTS(const Address& addr)
{
   CGuardEx sg(m_SlaveLock);

   map<Address, int, AddrComp>::iterator a = m_mAddrList.find(addr);
   if (a == m_mAddrList.end())
      return -1;

   map<int, SlaveNode>::iterator s = m_mSlaveList.find(a->second);
   if (s == m_mSlaveList.end())
   {
      //THIS SHOULD NOT HAPPEN
      return -1;
   }

   s->second.m_llLastUpdateTime = CTimer::getTime();

   return 0;
}

int SlaveManager::checkBadAndLost(map<int, SlaveNode>& bad, map<int, SlaveNode>& lost,
                                  map<int, SlaveNode>& retry, map<int, SlaveNode>& dead,
                                  const int64_t& timeout, const int64_t& retrytime)
{
   CGuardEx sg(m_SlaveLock);

   bad.clear();
   lost.clear();
   retry.clear();
   dead.clear();

   for (map<int, SlaveNode>::iterator i = m_mSlaveList.begin(); i != m_mSlaveList.end(); ++ i)
   {
      if (i->second.m_iStatus == SlaveStatus::DOWN)
      {
         // if the node is already marked down, try to restart or remove permanently
         if (CTimer::getTime() - i->second.m_llLastUpdateTime >= (uint64_t)retrytime)
         {
            dead[i->first] = i->second;
         }
         else
         {
            retry[i->first] = i->second;
         }

         continue;
      }

      // clear expired votes
      if (i->second.m_llLastVoteTime - CTimer::getTime() > 24LL * 60 * 3600 * 1000000)
      {
         i->second.m_sBadVote.clear();
         i->second.m_llLastVoteTime = CTimer::getTime();
      }

      // if received more than half votes, it is bad
      if (i->second.m_sBadVote.size() * 2 > m_mSlaveList.size())
      {
         bad[i->first] = i->second;
         i->second.m_iStatus = SlaveStatus::BAD;
      }

      // detect slave timeout
      if (CTimer::getTime() - i->second.m_llLastUpdateTime >= (uint64_t)timeout)
      {
         lost[i->first] = i->second;
         i->second.m_iStatus = SlaveStatus::DOWN;
      }
   }

   return 0;
}

int SlaveManager::serializeSlaveList(char*& buf, int& size)
{
   CGuardEx sg(m_SlaveLock);

   buf = new char [(4 + 4 + 64 + 4 + 4) * m_mSlaveList.size()];

   char* p = buf;
   for (map<int, SlaveNode>::iterator i = m_mSlaveList.begin(); i != m_mSlaveList.end(); ++ i)
   {
      *(int32_t*)p = i->first;
      p += 4;
      *(int32_t*)p = i->second.m_strIP.length() + 1;
      p += 4;
      strcpy(p, i->second.m_strIP.c_str());
      p += i->second.m_strIP.length() + 1;
      *(int32_t*)p = i->second.m_iPort;
      p += 4;
      *(int32_t*)p = i->second.m_iDataPort;
      p += 4;
   }

   size = p - buf;

   return m_mSlaveList.size();
}

int SlaveManager::deserializeSlaveList(int num, const char* buf, int /*size*/)
{
   const char* p = buf;
   for (int i = 0; i < num; ++ i)
   {
      SlaveNode sn;
      sn.m_iNodeID = *(int32_t*)p;
      p += 4;
      int32_t size = *(int32_t*)p;
      p += 4;
      sn.m_strIP = p;
      p += size;
      sn.m_iPort = *(int32_t*)p;
      p += 4;
      sn.m_iDataPort = *(int32_t*)p;
      p += 4;

      insert(sn);
   }

   updateClusterStat();

   return 0;
}

int SlaveManager::getSlaveID(const Address& addr)
{
   CGuardEx sg(m_SlaveLock);

   map<Address, int, AddrComp>::const_iterator i = m_mAddrList.find(addr);

   if (i == m_mAddrList.end())
      return -1;

   return i->second;
}

int SlaveManager::getSlaveAddr(const int& id, Address& addr)
{
   CGuardEx sg(m_SlaveLock);

   map<int, SlaveNode>::iterator i = m_mSlaveList.find(id);

   if (i == m_mSlaveList.end())
      return -1;

   addr.m_strIP = i->second.m_strIP;
   addr.m_iPort = i->second.m_iPort;

   return 0;
}

int SlaveManager::voteBadSlaves(const Address& voter, int num, const char* buf)
{
   CGuardEx sg(m_SlaveLock);

   int vid = m_mAddrList[voter];
   for (int i = 0; i < num; ++ i)
   {
      Address addr;
      addr.m_strIP = buf + i * 68;
      addr.m_iPort = *(int*)(buf + i * 68 + 64);

      int slave = m_mAddrList[addr];
      if( m_mSlaveList.find( slave ) == m_mSlaveList.end() )
         log().error << __PRETTY_FUNCTION__ << ": about to add new slave to list " << slave << std::endl;
      m_mSlaveList[slave].m_sBadVote.insert(vid);
   }

   return 0;
}

unsigned int SlaveManager::getNumberOfClusters()
{
   CGuardEx sg(m_SlaveLock);

   return m_Cluster.m_mSubCluster.size();
}

unsigned int SlaveManager::getNumberOfSlaves()
{
   CGuardEx sg(m_SlaveLock);

   return m_mSlaveList.size();
}

int SlaveManager::serializeClusterInfo(char*& buf, int& size)
{
   CGuardEx sg(m_SlaveLock);

   size = 4 + m_Cluster.m_mSubCluster.size() * 40;
   buf = new char[size];

   *(int32_t*)buf = m_Cluster.m_mSubCluster.size();

   char* p = buf + 4;
   for (map<int, Cluster>::iterator i = m_Cluster.m_mSubCluster.begin(); i != m_Cluster.m_mSubCluster.end(); ++ i)
   {
      *(int32_t*)p = i->second.m_iClusterID;
      *(int32_t*)(p + 4) = i->second.m_iTotalNodes;
      *(int64_t*)(p + 8) = i->second.m_llAvailDiskSpace;
      *(int64_t*)(p + 16) = i->second.m_llTotalFileSize;
      *(int64_t*)(p + 24) = i->second.m_llTotalInputData;
      *(int64_t*)(p + 32) = i->second.m_llTotalOutputData;

      p += 40;
   }

   return size;
}

int SlaveManager::serializeSlaveInfo(char*& buf, int& size)
{
   CGuardEx sg(m_SlaveLock);

   size = 4;
   for (map<int, SlaveNode>::iterator i = m_mSlaveList.begin(); i != m_mSlaveList.end(); ++ i)
   {
      size += i->second.m_strStoragePath.length() + 1;
   }
   size += m_mSlaveList.size() * 92;

   buf = new char[size];

   *(int32_t*)buf = m_mSlaveList.size();

   char* p = buf + 4;
   for (map<int, SlaveNode>::iterator i = m_mSlaveList.begin(); i != m_mSlaveList.end(); ++ i)
   {
      *(int32_t*)p = i->first;
      strcpy(p + 4, i->second.m_strIP.c_str());
      *(int32_t*)(p + 20) = i->second.m_iPort;
      int64_t avail_size = 0;
      if (i->second.m_llAvailDiskSpace > m_llSlaveMinDiskSpace)
         avail_size = i->second.m_llAvailDiskSpace - m_llSlaveMinDiskSpace;
      *(int64_t*)(p + 24) = avail_size;
      *(int64_t*)(p + 32) = i->second.m_llTotalFileSize;
      *(int64_t*)(p + 40) = i->second.m_llCurrMemUsed;
      *(int64_t*)(p + 48) = i->second.m_llCurrCPUUsed;
      *(int64_t*)(p + 56) = i->second.m_llTotalInputData;
      *(int64_t*)(p + 64) = i->second.m_llTotalOutputData;
      *(int64_t*)(p + 72) = i->second.m_llTimeStamp;
      *(int64_t*)(p + 80) = i->second.m_iStatus;
      *(int64_t*)(p + 84) = i->second.m_viPath[0];
      *(int64_t*)(p + 88) = i->second.m_strStoragePath.length() + 1;
      p+= 92;
      strcpy(p, i->second.m_strStoragePath.c_str());
      p+= i->second.m_strStoragePath.length() + 1;
   }

   return size;
}

uint64_t SlaveManager::getTotalDiskSpace()
{
   CGuardEx sg(m_SlaveLock);

   uint64_t size = 0;
   for (map<int, SlaveNode>::iterator i = m_mSlaveList.begin(); i != m_mSlaveList.end(); ++ i)
   {
      if ((i->second.m_iStatus == SlaveStatus::DOWN) || (i->second.m_iStatus == SlaveStatus::BAD))
         continue;

      if (i->second.m_llAvailDiskSpace > m_llSlaveMinDiskSpace)
         size += i->second.m_llAvailDiskSpace - m_llSlaveMinDiskSpace;
   }

   return size;
}

void SlaveManager::updateClusterStat()
{
   CGuardEx sg(m_SlaveLock);

   updateclusterstat_(m_Cluster);
}

void SlaveManager::updateclusterstat_(Cluster& c)
{
   if (c.m_mSubCluster.empty())
   {
      c.m_llAvailDiskSpace = 0;
      c.m_llTotalFileSize = 0;
      c.m_llTotalInputData = 0;
      c.m_llTotalOutputData = 0;
      c.m_mSysIndInput.clear();
      c.m_mSysIndOutput.clear();

      for (set<int>::iterator i = c.m_sNodes.begin(); i != c.m_sNodes.end(); ++ i)
      {
      if( m_mSlaveList.find( *i ) == m_mSlaveList.end() )
         log().error << __PRETTY_FUNCTION__ << ": about to add new slave to list " << *i << std::endl;
         SlaveNode* s = &m_mSlaveList[*i];

         if (s->m_iStatus == SlaveStatus::DOWN)
            continue;

         if (s->m_llAvailDiskSpace > m_llSlaveMinDiskSpace)
            c.m_llAvailDiskSpace += s->m_llAvailDiskSpace - m_llSlaveMinDiskSpace;
         c.m_llTotalFileSize += s->m_llTotalFileSize;
         updateclusterio_(c, s->m_mSysIndInput, c.m_mSysIndInput, c.m_llTotalInputData);
         updateclusterio_(c, s->m_mSysIndOutput, c.m_mSysIndOutput, c.m_llTotalOutputData);
      }
   }
   else
   {
      c.m_llAvailDiskSpace = 0;
      c.m_llTotalFileSize = 0;
      c.m_llTotalInputData = 0;
      c.m_llTotalOutputData = 0;
      c.m_mSysIndInput.clear();
      c.m_mSysIndOutput.clear();
	    
      for (map<int, Cluster>::iterator i = c.m_mSubCluster.begin(); i != c.m_mSubCluster.end(); ++ i)
      {
         updateclusterstat_(i->second);

         c.m_llAvailDiskSpace += i->second.m_llAvailDiskSpace;
         c.m_llTotalFileSize += i->second.m_llTotalFileSize;
         updateclusterio_(c, i->second.m_mSysIndInput, c.m_mSysIndInput, c.m_llTotalInputData);
         updateclusterio_(c, i->second.m_mSysIndOutput, c.m_mSysIndOutput, c.m_llTotalOutputData);
      }
   }
}

void SlaveManager::updateclusterio_(Cluster& c, map<string, int64_t>& data_in, map<string, int64_t>& data_out, int64_t& total)
{
   for (map<string, int64_t>::iterator p = data_in.begin(); p != data_in.end(); ++ p)
   {
      vector<int> path;
      m_pTopology->lookup(p->first.c_str(), path);
      if (m_pTopology->match(c.m_viPath, path) == c.m_viPath.size())
         continue;

      map<string, int64_t>::iterator n = data_out.find(p->first);
      if (n == data_out.end())
         data_out[p->first] = p->second;
      else
         n->second += p->second;

      total += p->second;
   }
}

int SlaveManager::getSlaveListByRack(map<int, Address>& sl, const string& topopath)
{
   CGuardEx sg(m_SlaveLock);

   vector<int> path;
   if (m_pTopology->parseTopo(topopath.c_str(), path) < 0)
      return -1;

   sl.clear();
   unsigned int len = path.size();

   for (map<int, SlaveNode>::iterator i = m_mSlaveList.begin(); i != m_mSlaveList.end(); ++ i)
   {
      if (len > i->second.m_viPath.size())
         continue;

      bool match = true;
      for (unsigned int p = 0; p < len; ++ p)
      {
         if (path[p] != i->second.m_viPath[p])
         {
            match = false;
            break;
         }
      }

      if (match)
      {
         Address addr;
         addr.m_strIP = i->second.m_strIP;
         addr.m_iPort = i->second.m_iPort;
         sl[i->first] = addr;
      }
   }

   return sl.size();
}

int SlaveManager::checkStorageBalance(map<int64_t, Address>& lowdisk, bool forceclear)
{
   CGuardEx sg(m_SlaveLock);

   if (m_mSlaveList.empty())
      return 0;

   lowdisk.clear();

   std::map<int, int64_t> totalAvailableDiskSpacePerCluster;
   std::map<int, int>     numSlavesPerCluster;
   std::map<int, int64_t> avgAvailableDiskSpacePerCluster;

   for (map<int, SlaveNode>::iterator i = m_mSlaveList.begin(); i != m_mSlaveList.end(); ++ i)
   {
      if( forceclear )
      {
         i->second.m_iStatus = SlaveStatus::NORMAL;
         i->second.m_bDiskLowWarning = false;
      }

      if (i->second.m_iStatus == SlaveStatus::NORMAL)
      {
         totalAvailableDiskSpacePerCluster[ i->second.m_viPath.back() ] += i->second.m_llAvailDiskSpace;
         ++numSlavesPerCluster[ i->second.m_viPath.back() ];
      }

   }
//   log().trace << "Slave space deficit caclulation - minimum disk space set to " << m_llSlaveMinDiskSpace << std::endl;
   for( std::map<int, int64_t>::iterator i = totalAvailableDiskSpacePerCluster.begin(); i != totalAvailableDiskSpacePerCluster.end(); ++i )
   {
      avgAvailableDiskSpacePerCluster[ i->first ] = i->second / numSlavesPerCluster[ i->first ];
//      log().trace << "Cluster " << i->first << " total available " << i->second << " no of slaves " <<
//         numSlavesPerCluster[ i->first ] << " avg available per cluster " 
//         << avgAvailableDiskSpacePerCluster[ i->first ] << std::endl;
   }

   //TODO: using "target" value as key may cause certain low disk node to be ignored.
   // such node may be included again in the next round of check.
   // a better method can be used in the future.

   for (map<int, SlaveNode>::iterator i = m_mSlaveList.begin(); i != m_mSlaveList.end(); ++ i)
   {
//      log().trace << "Slave " << i->first << " " << i->second.m_strIP << ":" << i->second.m_iPort 
//      << " status " << i->second.m_iStatus << " Space available " << i->second.m_llAvailDiskSpace 
//      << " space used " << i->second.m_llTotalFileSize
//      << " DiskLowWarning " << i->second.m_bDiskLowWarning   
//      << " cluster " << i->second.m_viPath.back() << std::endl;
      if ((i->second.m_llAvailDiskSpace <= m_llSlaveMinDiskSpace) && (!i->second.m_bDiskLowWarning))
      {

         int64_t target =  avgAvailableDiskSpacePerCluster[ i->second.m_viPath.back() ] - i->second.m_llAvailDiskSpace;
         if (target <= 0)
         {
           log().trace << "Average space on cluster below min disk space on slave - no space rebalancing " << std::endl;
         } else 
         {
         
           lowdisk[target].m_strIP = i->second.m_strIP;
           lowdisk[target].m_iPort = i->second.m_iPort;

           i->second.m_iStatus = SlaveStatus::DISKFULL;
           i->second.m_bDiskLowWarning = true;

           log().trace << "Storage balance for " << i->second.m_strIP << ":" << i->second.m_iPort 
             << " avgAvailableCluster: " << avgAvailableDiskSpacePerCluster[ i->second.m_viPath.back() ] 
             << " m_llSlaveMinDiskSpace: " << m_llSlaveMinDiskSpace
             << " m_llAvailDiskSpace: " << i->second.m_llAvailDiskSpace
             << " m_llTotalFileSize: " << i->second.m_llTotalFileSize
             << " target available space: " << target << std::endl;
         }
      }
   }

   return lowdisk.size();
}

int SlaveManager::findNearestNode(std::set<int>& loclist, const std::string& ip, SlaveNode& sn)
{
   if (loclist.empty())
      return SectorError::E_NODISK;

   // find nearest node, if equal distance, choose a random one
   int dist = -1;
   map<int, vector<int> > dist_vec;
   for (set<int>::iterator i = loclist.begin(); i != loclist.end(); ++ i)
   {
      if( m_mSlaveList.find( *i ) == m_mSlaveList.end() )
         log().error << __PRETTY_FUNCTION__ << ": (1) about to add new slave to list " << *i << std::endl;
      int d = m_pTopology->distance(ip.c_str(), m_mSlaveList[*i].m_strIP.c_str());
      dist_vec[d].push_back(*i);
      if ((d < dist) || (dist < 0))
         dist = d;
   }

   // if no slave node found, return 0
   if (dist < 0)
      return SectorError::E_NODISK;

   // chose nearest node first then least busy node, a random one if the first two conditions equal
   // TODO: this code can be slightly optimized

   int r = int(dist_vec[dist].size() * (double(rand()) / RAND_MAX));
   int n = dist_vec[dist][r];

      if( m_mSlaveList.find( n ) == m_mSlaveList.end() )
         log().error << __PRETTY_FUNCTION__ << ": (2) about to add new slave to list " << n << std::endl;
   sn = m_mSlaveList[n];

   // choose node with least active transactions; disabled as this is dangerous to get certan nodes starved
   // re-enabled by sergey
   if (sn.m_iActiveTrans == 0)
      return 0;

   // if the chosen node already serves other transactions, choose the next one with minimum number of transactions
   for (int i = r + 1, max = r + dist_vec[dist].size(); i < max; ++ i)
   {
      int index = i % dist_vec[dist].size();

      if( m_mSlaveList.find( dist_vec[dist][index] ) == m_mSlaveList.end() )
         log().error << __PRETTY_FUNCTION__ << ": about to add new slave to list " << dist_vec[dist][index] << std::endl;
      if (m_mSlaveList[dist_vec[dist][index]].m_iActiveTrans < sn.m_iActiveTrans)
      {
         sn = m_mSlaveList[dist_vec[dist][index]];
         if (sn.m_iActiveTrans == 0)
            break;
      }
   }

   return 0;
}

void SlaveManager::incActTrans(const int& slaveid)
{
   CGuardEx sg(m_SlaveLock);

   map<int, SlaveNode>::iterator i = m_mSlaveList.find(slaveid);
   if (i != m_mSlaveList.end())
      ++ i->second.m_iActiveTrans;
}

void SlaveManager::decActTrans(const int& slaveid)
{
   CGuardEx sg(m_SlaveLock);

   map<int, SlaveNode>::iterator i = m_mSlaveList.find(slaveid);
   if ((i != m_mSlaveList.end()) && (i->second.m_iActiveTrans > 0))
      -- i->second.m_iActiveTrans;
}

void SlaveManager::getListActTrans( map<int, int>& lats )
{
   lats.clear();
   CGuardEx sg(m_SlaveLock);
   for( map<int, SlaveNode>::iterator i = m_mSlaveList.begin(); i != m_mSlaveList.end(); ++i )
      lats.insert( std::make_pair( i->second.m_iNodeID, i->second.m_iActiveTrans ) );
}

void SlaveManager::getSlaveIPToClusterMap ( std::map<string, int>& IPToSlave )
{
  CGuardEx sg(m_SlaveLock);
  for ( std::map<int, SlaveNode>::iterator i = m_mSlaveList.begin(); i != m_mSlaveList.end(); ++i )
  {
      // need only unique IP 
      IPToSlave.insert( std::make_pair( i->second.m_strIP, i->second.m_viPath.back() ) );
  }
}
