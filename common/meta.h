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


#ifndef __SECTOR_METADATA_H__
#define __SECTOR_METADATA_H__

#include <string>
#include <vector>
#include <map>
#include <set>
#include <fstream>
#include <topology.h>
#include <sector.h>
#include <common.h>

enum MetaForm {DEFAULT, MEMORY, DISK};

class Metadata
{
public:
   Metadata();
   virtual ~Metadata();

   virtual void init(const std::string& path) = 0;
   virtual void clear() = 0;

   void setDefault(const int rep_num, const int rep_dist, bool allow_same_ip_replica, int pct_of_slaves_to_consider, bool check_replica_cluster);

public:	// list and lookup operations
   virtual int list(const std::string& path, std::vector<std::string>& filelist) = 0;
     // like previous but with flag to serialize replicas
   virtual int list(const std::string& path, std::vector<std::string>& filelist, const bool includeReplica) = 0;

      // Functionality:
      //    list all files in a directory recursively. used for sector_copy ONLY. ".nosplit" dirs only list its name, not content
      // Parameters:
      //    1) [in] path: file or dir name
      //    2) [out] filelist: list of files and dirs to be replicated or copied
      // Returned value:
      //    0 success, otherwise negative error number.

   virtual int list_r(const std::string& path, std::vector<std::string>& filelist) = 0;

      // Functionality:
      //    look up a specific file or directory in the metadata.
      // Parameters:
      //    1) [in] path: file or dir name
      //    2) [out] attr: SNode structure to store the information.
      // Returned value:
      //    If exist, 0 for a file, number of files or sub-dirs for a directory, or -1 on error.

   virtual int lookup(const std::string& path, SNode& attr) = 0;
   virtual int lookup(const std::string& path, std::set<Address, AddrComp>& addr) = 0;

public:	// update operations
   virtual int create(const SNode& node) = 0;
   virtual int move(const std::string& oldpath, const std::string& newpath, const std::string& newname = "") = 0;
   virtual int remove(const std::string& path, bool recursive = false) = 0;
   virtual int addReplica(const std::string& path, const int64_t& ts, const int64_t& size, const Address& addr) = 0;
   virtual int removeReplica(const std::string& path, const Address& addr) = 0;

      // Functionality:
      //    update the timestamp and size information of a file
      // Parameters:
      //    1) [in] path: file or dir name, full path
      //    2) [in] ts: timestamp
      //    3) [in] size: file size, when it is negative, ignore it and only update timestamp
      // Returned value:
      //    0 success, -1 error.

   virtual int update(const std::string& path, const int64_t& ts, const int64_t& size = -1) = 0;

public:	// lock/unlock
   virtual int lock(const std::string& path, int user, int mode);
   virtual int unlock(const std::string& path, int user, int mode);
   bool isWriteLocked( const std::string& path );

public:	// serialization
   // TODO: use memory buffer to store the serialized metadata instead of a file
   virtual int serialize(const std::string& path, const std::string& dstfile) = 0;
   virtual int deserialize(const std::string& path, const std::string& srcfile, const Address* addr) = 0;
   virtual int scan(const std::string& data_dir, const std::string& meta_dir) = 0;

public:	// medadata and file system operations

      // Functionality:
      //    merge a slave's index with the system file index.
      // Parameters:
      //    1) [in] path: merge into this director, usually "/"
      //    2) [in, out] branch: new metadata to be included; excluded/conflict metadata will be left, while others will be removed
      //    3) [in] replica: number of replicas
      // Returned value:
      //    0 on success, or -1 on error.

   virtual int merge(const std::string& path, Metadata* branch, const unsigned int& replica) = 0;

   virtual int substract(const std::string& path, const Address& addr) = 0;
   virtual int64_t getTotalDataSize(const std::string& path) = 0;
   virtual int64_t getTotalFileNum(const std::string& path) = 0;
   virtual int collectDataInfo(const std::string& path, std::vector<std::string>& result) = 0;
   virtual int checkReplica(const std::string& path, std::vector<std::string>& under, std::vector<std::string>& over,  const std::map< std::string, int> & IPToCluster) = 0;

   virtual int getSlaveMeta(Metadata* branch, const Address& addr) = 0;

public:
   static int parsePath(const std::string& path, std::vector<std::string>& result);
   static std::string revisePath(const std::string& path);

public:
      // Functionality:
      //    update per-file configuration, from replica.conf.
      // Parameters:
      //    1) [in] path: directory to be configured/updated, usually "/"
      //    2) [in] default_num: default replica factor
      //    3) [in] default_dist: default replication distance
      //    4) [in] rep_num: special replica factor
      //    5) [in] rep_dist: special replication distance
      //    3) [in] restricted_loc: special restricted locations
      // Returned value:
      //    0 on success, or -1 on error.

   virtual void refreshRepSetting(const std::string& path, int default_num, int default_dist, const std::map<std::string, std::pair<int,int> >& rep_num, const std::map<std::string, int>& rep_dist, const std::map<std::string, std::vector<int> >& restrict_loc) = 0;

protected:
   // TODO: The locking can be sharded.

   pthread_mutex_t m_FileLockProtection;

public:
   struct LockSet
   {
      std::set<int> m_sReadLock;
      std::set<int> m_sWriteLock;
   };

   std::map<std::string, LockSet> getLockList();

protected:
   std::map<std::string, LockSet> m_mLock;

private:
   static bool initLC();
   static bool m_pbLegalChar[256];
   static bool m_bInit;

protected:
   static int m_iDefaultRepNum;
   static int m_iDefaultRepDist;
   static bool m_bCheckReplicaOnSameIp;
   static int m_iPctSlavesToConsider;
   static int64_t m_iLastTotalDiskSpace;
   static time_t m_iLastTotalDiskSpaceTs;
   static int const m_iLastTotalDiskSpaceTimeout = 10;
   static bool m_bCheckReplicaCluster;
};

#endif
