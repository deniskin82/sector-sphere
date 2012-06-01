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


#ifndef __SECTOR_INDEX_H__
#define __SECTOR_INDEX_H__

#include <meta.h>
#include <osportable.h>

class Index: public Metadata
{
public:
   Index();
   virtual ~Index();

   virtual void init(const std::string& /*path*/) {}
   virtual void clear() {}

public:
   virtual int list(const std::string& path, std::vector<std::string>& filelist);
   virtual int list(const std::string& path, std::vector<std::string>& filelist, bool includeReplica);
   virtual int list_r(const std::string& path, std::vector<std::string>& filelist);
   virtual int lookup(const std::string& path, SNode& attr);
   virtual int lookup(const std::string& path, std::set<Address, AddrComp>& addr);

public:
   virtual int create(const SNode& node);
   virtual int move(const std::string& oldpath, const std::string& newpath, const std::string& newname = "");
   virtual int remove(const std::string& path, bool recursive = false);
   virtual int addReplica(const std::string& path, const int64_t& ts, const int64_t& size, const Address& addr);
   virtual int removeReplica(const std::string& path, const Address& addr);
   virtual int update(const std::string& path, const int64_t& ts, const int64_t& size = -1);

public:
   virtual int serialize(const std::string& path, const std::string& dstfile);
   virtual int deserialize(const std::string& path, const std::string& srcfile,  const Address* addr = NULL);
   virtual int scan(const std::string& data_dir, const std::string& meta_dir);

public:
   virtual int merge(const std::string& path, Metadata* branch, const unsigned int& replica);
   virtual int substract(const std::string& path, const Address& addr);

   virtual int64_t getTotalDataSize(const std::string& path);
   virtual int64_t getTotalFileNum(const std::string& path);
   virtual int collectDataInfo(const std::string& path, std::vector<std::string>& result);
   virtual int checkReplica(const std::string& path, std::vector<std::string>& under, std::vector<std::string>& over,  const std::map< std::string, int> & IPToCluster);
   virtual int getSlaveMeta(Metadata* branch, const Address& addr);

public:
   virtual void refreshRepSetting(const std::string& path, int default_num, int default_dist, const std::map<std::string, std::pair<int,int> >& rep_num, const std::map<std::string, int>& rep_dist, const std::map<std::string, std::vector<int> >& restrict_loc);

private:
   int serialize(std::ofstream& ofs, std::map<std::string, SNode>& currdir, int level);
   int deserialize(std::ifstream& ifs, std::map<std::string, SNode>& currdir, const Address* addr = NULL);
   int scan(const std::string& currdir, std::map<std::string, SNode>& metadata);
   int merge(std::map<std::string, SNode>& currdir, std::map<std::string, SNode>& branch, const unsigned int& replica);
   int substract(std::map<std::string, SNode>& currdir, const Address& addr);

   int64_t getTotalDataSize(const std::map<std::string, SNode>& currdir) const;
   int64_t getTotalFileNum(const std::map<std::string, SNode>& currdir) const;
   int collectDataInfo(const std::string& path, const std::map<std::string, SNode>& currdir, std::vector<std::string>& result) const;
   int checkReplica(const std::string& path, const std::map<std::string, SNode>& currdir, std::vector<std::string>& under, std::vector<std::string>& over, const std::map< std::string, int> & IPToCluster) const;
   int list_r(const std::map<std::string, SNode>& currdir, const std::string& path, std::vector<std::string>& filelist) const;
   int getSlaveMeta(const std::map<std::string, SNode>& currdir, const std::vector<std::string>& path, std::map<std::string, SNode>& target, const Address& addr) const;

   int refreshRepSetting(const std::string& path, std::map<std::string, SNode>& currdir, int default_num, int default_dist, const std::map<std::string, std::pair<int, int> >& rep_num, const std::map<std::string, int>& rep_dist, const std::map<std::string, std::vector<int> >& restrict_loc);

   int refreshRepSetting(const std::string& path, SNode& curnode, int default_num, int default_dist, const std::map<std::string, std::pair<int,int> >& rep_num, const std::map<std::string, int>& rep_dist, const std::map<std::string, std::vector<int> >& restrict_loc);

private:
   std::map<std::string, SNode> m_mDirectory;
   RWLock m_MetaLock;
};

#endif
