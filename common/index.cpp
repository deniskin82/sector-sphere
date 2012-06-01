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
   Yunhong Gu, last updated 03/16/2011
*****************************************************************************/

#include <algorithm>
#include <cstring>
#include <iostream>
#include <queue>
#include <set>
#include <sstream>
#include <stack>
#include <sys/stat.h>
#include <sys/types.h>
#include <time.h>

#include "common.h"
#include "index.h"
#include "sector.h"


using namespace std;

Index::Index()
{
}

Index::~Index()
{
}

int Index::list(const string& path, vector<string>& filelist, const bool includeReplica)
{
   RWGuard mg(m_MetaLock, RW_READ);

   vector<string> dir;
   if (parsePath(path, dir) < 0)
      return SectorError::E_INVALID;

   map<string, SNode>* currdir = &m_mDirectory;
   unsigned int depth = 1;
   for (vector<string>::const_iterator d = dir.begin(); d != dir.end(); ++ d)
   {
      map<string, SNode>::iterator s = currdir->find(*d);
      if (s == currdir->end())
         return SectorError::E_NOEXIST;

      if (!s->second.m_bIsDir)
      {
         if (depth != dir.size())
            return SectorError::E_NOEXIST;

         char* buf = NULL;       
         if (s->second.serialize(buf, includeReplica) >= 0)
            filelist.insert(filelist.end(), buf);
         delete [] buf;
         return 1;
      }

      currdir = &(s->second.m_mDirectory);
      depth ++;
   }

   filelist.clear();
   for (map<string, SNode>::const_iterator i = currdir->begin(); i != currdir->end(); ++ i)
   {
      char* buf = NULL;
      if (i->second.serialize(buf, includeReplica) >= 0)
         filelist.insert(filelist.end(), buf);
      delete [] buf;
   }

   return filelist.size();
}

int Index::list(const string& path, vector<string>& filelist){
  return list(path, filelist, true);
}

int Index::list_r(const string& path, vector<string>& filelist)
{
   RWGuard mg(m_MetaLock, RW_READ);

   vector<string> dir;
   if (parsePath(path, dir) < 0)
      return SectorError::E_INVALID;

   map<string, SNode>* currdir = &m_mDirectory;
   map<string, SNode>::iterator s;
   for (vector<string>::const_iterator d = dir.begin(); d != dir.end(); ++ d)
   {
      s = currdir->find(*d);
      if (s == currdir->end())
         return SectorError::E_NOEXIST;

      currdir = &(s->second.m_mDirectory);
   }

   // if this is root dir, list its content, but not itself
   if (dir.empty())
      return list_r(*currdir, path, filelist);

   if (s->second.m_bIsDir)
   {
      if (s->second.m_mDirectory.find(".nosplit") != s->second.m_mDirectory.end())
      {
         // nosplit dir, only dir name is returned 
         filelist.push_back(path);
         return 0;
      }

      if (s->second.m_mDirectory.empty())
      {
         filelist.push_back(path);
         return 0;
      }

      return list_r(*currdir, path, filelist);
   }
   else
   {
      filelist.push_back(path);
   }

   return 0;
}

int Index::lookup(const string& path, SNode& attr)
{
   RWGuard mg(m_MetaLock, RW_READ);

   vector<string> dir;
   if (parsePath(path, dir) < 0)
      return SectorError::E_INVALID;

   if (dir.empty())
   {
      // stat on the root directory "/"
      attr.m_strName = "/";
      attr.m_bIsDir = true;
      attr.m_llSize = 0;
      attr.m_llTimeStamp = 0;
      for (map<string, SNode>::iterator i = m_mDirectory.begin(); i != m_mDirectory.end(); ++ i)
      {
         attr.m_llSize += i->second.m_llSize;
         if (attr.m_llTimeStamp < i->second.m_llTimeStamp)
            attr.m_llTimeStamp = i->second.m_llTimeStamp;
      }
      return m_mDirectory.size();
   }

   map<string, SNode>* currdir = &m_mDirectory;
   map<string, SNode>::iterator s;
   for (vector<string>::iterator d = dir.begin(); d != dir.end(); ++ d)
   {
      s = currdir->find(*d);
      if (s == currdir->end())
         return -1;

      currdir = &(s->second.m_mDirectory);
   }

 //  attr = s->second;
   attr.m_strName = s->second.m_strName;
   attr.m_bIsDir = s->second.m_bIsDir;
   attr.m_sLocation = s->second.m_sLocation;
   attr.m_mDirectory.clear();
   attr.m_llTimeStamp = s->second.m_llTimeStamp;
   attr.m_llSize = s->second.m_llSize;
   attr.m_strChecksum = s->second.m_strChecksum;
   attr.m_iReplicaNum = s->second.m_iReplicaNum;
   attr.m_iMaxReplicaNum = s->second.m_iMaxReplicaNum;
   attr.m_iReplicaDist = s->second.m_iReplicaDist;
   attr.m_viRestrictedLoc = s->second.m_viRestrictedLoc;

   return s->second.m_mDirectory.size();
}

int Index::lookup(const string& path, set<Address, AddrComp>& addr)
{
   RWGuard mg(m_MetaLock, RW_READ);

   vector<string> dir;
   if (parsePath(path, dir) <= 0)
      return SectorError::E_INVALID;

   map<string, SNode>* currdir = &m_mDirectory;
   map<string, SNode>::iterator s;
   for (vector<string>::iterator d = dir.begin(); d != dir.end(); ++ d)
   {
      s = currdir->find(*d);
      if (s == currdir->end())
         return -1;

      currdir = &(s->second.m_mDirectory);
   }

   stack<SNode*> scanmap;
   scanmap.push(&(s->second));

   while (!scanmap.empty())
   {
      SNode* n = scanmap.top();
      scanmap.pop();

      if (n->m_bIsDir)
      {
         for (map<string, SNode>::iterator i = n->m_mDirectory.begin(); i != n->m_mDirectory.end(); ++ i)
            scanmap.push(&(i->second));
      }
      else
      {
         for (set<Address, AddrComp>::iterator i = n->m_sLocation.begin(); i != n->m_sLocation.end(); ++ i)
            addr.insert(*i);
      }
   }

   return addr.size();
}

int Index::create(const SNode& node)
{
   RWGuard mg(m_MetaLock, RW_WRITE);

   vector<string> dir;
   if (parsePath(node.m_strName.c_str(), dir) <= 0)
      return -3;

   if (dir.empty())
      return -1;

   bool found = true;

   map<string, SNode>* currdir = &m_mDirectory;
   map<string, SNode>::iterator s;
   string filename;
   for (vector<string>::iterator d = dir.begin(); d != dir.end(); ++ d)
   {
      s = currdir->find(*d);
      if (s == currdir->end())
      {
         SNode n;
         n.m_strName = *d;
         n.m_bIsDir = true;
         n.m_llTimeStamp = time(NULL);
         n.m_llSize = 0;
         (*currdir)[*d] = n;
         s = currdir->find(*d);

         filename = *d;

         found = false;
      }
      currdir = &(s->second.m_mDirectory);
   }

   // if already exist, return error
   if (found)
      return -1;

   // node initially contains full path name, revise it to file name only
   s->second = node;
   s->second.m_strName = filename;

   return 0;
}

int Index::move(const string& oldpath, const string& newpath, const string& newname)
{
   RWGuard mg(m_MetaLock, RW_WRITE);

   vector<string> olddir;
   if (parsePath(oldpath, olddir) <= 0)
      return -3;

   if (olddir.empty())
      return -1;

   vector<string> newdir;
   if (parsePath(newpath, newdir) < 0)
      return -3;

   map<string, SNode>* od = &m_mDirectory;
   map<string, SNode>::iterator os;
   for (vector<string>::iterator d = olddir.begin();;)
   {
      os = od->find(*d);
      if (os == od->end())
         return -1;

      if (++ d == olddir.end())
         break;

      od = &(os->second.m_mDirectory);
   }

   map<string, SNode>* nd = &m_mDirectory;
   map<string, SNode>::iterator ns;
   for (vector<string>::iterator d = newdir.begin(); d != newdir.end(); ++ d)
   {
      ns = nd->find(*d);
      if (ns == nd->end())
      {
         SNode n;
         n.m_strName = *d;
         n.m_bIsDir = true;
         n.m_llTimeStamp = 0;
         n.m_llSize = 0;
         (*nd)[*d] = n;
         ns = nd->find(*d);
      }

      nd = &(ns->second.m_mDirectory);
   }

   if (newname.length() == 0)
      (*nd)[os->first] = os->second;
   else
   {
      os->second.m_strName = newname;
      (*nd)[newname] = os->second;
   }

   od->erase(os->first);

   return 1;
}

int Index::remove(const string& path, bool recursive)
{
   RWGuard mg(m_MetaLock, RW_WRITE);

   vector<string> dir;
   if (parsePath(path, dir) <= 0)
      return SectorError::E_INVALID;

   if (dir.empty())
      return -1;

   map<string, SNode>* currdir = &m_mDirectory;
   map<string, SNode>::iterator s;
   for (vector<string>::iterator d = dir.begin(); ; )
   {
      s = currdir->find(*d);
      if (s == currdir->end())
         return -1;

      if (++ d == dir.end())
         break;

      currdir = &(s->second.m_mDirectory);
   }

   if (s->second.m_bIsDir)
   {
      if (recursive)
         currdir->erase(s);
      else
         return -1;
   }
   else
   {
      currdir->erase(s);
   }

   return 0;
}

int Index::addReplica(const string& path, const int64_t& ts, const int64_t& size, const Address& addr)
{
   RWGuard mg(m_MetaLock, RW_WRITE);

   vector<string> dir;
   parsePath(path.c_str(), dir);
   if (dir.empty())
      return -1;

   map<string, SNode>* currdir = &m_mDirectory;
   map<string, SNode>::iterator s;
   for (vector<string>::iterator d = dir.begin(); d != dir.end(); ++ d)
   {
      s = currdir->find(*d);
      if (s == currdir->end())
      {
         // file does not exist, return error
         return SectorError::E_NOEXIST;
      }
      currdir = &(s->second.m_mDirectory);
   }

   if ((s->second.m_llSize != size) || (s->second.m_llTimeStamp != ts))
      return -1;

   s->second.m_sLocation.insert(addr);

   return 0;
}

int Index::removeReplica(const string& path, const Address& addr)
{
   RWGuard mg(m_MetaLock, RW_WRITE);

   vector<string> dir;
   parsePath(path.c_str(), dir);
   if (dir.empty())
      return -1;

   map<string, SNode>* currdir = &m_mDirectory;
   map<string, SNode>::iterator s;
   for (vector<string>::iterator d = dir.begin(); d != dir.end(); ++ d)
   {
      s = currdir->find(*d);
      if (s == currdir->end())
      {
         // file does not exist, return error
         return SectorError::E_NOEXIST;
      }
      currdir = &(s->second.m_mDirectory);
   }

   // if this is a single file, remove the address from its location list
   if (!s->second.m_bIsDir)
   {
      s->second.m_sLocation.erase(addr);
      return 0;
   }

   // if this is a directory, remove the address from all files in the directory
   queue<SNode*> fq;
   fq.push(&s->second);
   while (!fq.empty())
   {
      SNode* p = fq.front();
      fq.pop();
      if (p->m_bIsDir)
      {
         for (map<string, SNode>::iterator i = p->m_mDirectory.begin(); i != p->m_mDirectory.end(); ++ i)
            fq.push(&i->second);
      }
      else
      {
         p->m_sLocation.erase(addr);
      }
   }

   return 0;
}

int Index::update(const string& path, const int64_t& ts, const int64_t& size)
{
   RWGuard mg(m_MetaLock, RW_WRITE);

   vector<string> dir;
   parsePath(path, dir);

   if (dir.empty())
      return 0;

   map<string, SNode>* currdir = &m_mDirectory;
   map<string, SNode>::iterator s;
   for (vector<string>::iterator d = dir.begin(); d != dir.end(); ++ d)
   {
      s = currdir->find(*d);
      if (s == currdir->end())
         return -1;

      currdir = &(s->second.m_mDirectory);
   }

   // sometime it may be necessary to update timestamp only. In this case size should be set to <0.
   if (size >= 0)
      s->second.m_llSize = size;

   s->second.m_llTimeStamp = ts;

   return 1;
}

int Index::serialize(const string& path, const string& dstfile)
{
   RWGuard mg(m_MetaLock, RW_READ);

   vector<string> dir;
   if (parsePath(path, dir) < 0)
      return SectorError::E_INVALID;

   map<string, SNode>* currdir = &m_mDirectory;
   map<string, SNode>::iterator s;
   for (vector<string>::const_iterator d = dir.begin(); d != dir.end(); ++ d)
   {
      s = currdir->find(*d);
      if (s == currdir->end())
         return -1;

      currdir = &(s->second.m_mDirectory);
   }

   ofstream ofs(dstfile.c_str());
   if (ofs.bad() || ofs.fail())
      return -1;

   serialize(ofs, *currdir, 1);
   bool rc = ofs.bad() || ofs.fail();
   ofs.close();
   return rc;
}

int Index::deserialize(const string& path, const string& srcfile,  const Address* addr)
{
   RWGuard mg(m_MetaLock, RW_WRITE);

   vector<string> dir;
   if (parsePath(path, dir) < 0)
      return SectorError::E_INVALID;

   map<string, SNode>* currdir = &m_mDirectory;
   map<string, SNode>::iterator s;
   for (vector<string>::iterator d = dir.begin(); d != dir.end(); ++ d)
   {
      s = currdir->find(*d);
      if (s == currdir->end())
         return -1;

      currdir = &(s->second.m_mDirectory);
   }

   ifstream ifs(srcfile.c_str());
   if (ifs.bad() || ifs.fail())
      return -1;

   deserialize(ifs, *currdir, addr);

   ifs.close();
   return 0;
}

int Index::scan(const string& datadir, const string& metadir)
{
   RWGuard mg(m_MetaLock, RW_WRITE);

   vector<string> dir;
   if (parsePath(metadir, dir) < 0)
      return SectorError::E_INVALID;

   map<string, SNode>* currdir = &m_mDirectory;
   map<string, SNode>::iterator s;
   for (vector<string>::iterator d = dir.begin(); d != dir.end(); ++ d)
   {
      s = currdir->find(*d);
      if (s == currdir->end())
         return -1;

      currdir = &(s->second.m_mDirectory);
   }

   scan(datadir, *currdir);

   return 0;
}

int Index::merge(const string& /*path*/, Metadata* meta, const unsigned int& replica)
{
   RWGuard mg(m_MetaLock, RW_WRITE);

   merge(m_mDirectory, ((Index*)meta)->m_mDirectory, replica);

   return 0;
}

int Index::substract(const string& path, const Address& addr)
{
   RWGuard mg(m_MetaLock, RW_WRITE);

   vector<string> dir;
   if (parsePath(path, dir) < 0)
      return SectorError::E_INVALID;

   map<string, SNode>* currdir = &m_mDirectory;
   map<string, SNode>::iterator s;
   for (vector<string>::iterator d = dir.begin(); d != dir.end(); ++ d)
   {
      s = currdir->find(*d);
      if (s == currdir->end())
         return -1;

      currdir = &(s->second.m_mDirectory);
   }

   substract(*currdir, addr);

   return 0;
}

int64_t Index::getTotalDataSize(const string& path)
{
   RWGuard mg(m_MetaLock, RW_READ);

   vector<string> dir;
   if (parsePath(path, dir) < 0)
      return SectorError::E_INVALID;

   map<string, SNode>* currdir = &m_mDirectory;
   map<string, SNode>::iterator s;
   for (vector<string>::const_iterator d = dir.begin(), end = dir.end(); d != end; ++ d)
   {
      s = currdir->find(*d);
      if (s == currdir->end())
         return -1;

      currdir = &(s->second.m_mDirectory);
   }

   return getTotalDataSize(*currdir);
}

int64_t Index::getTotalFileNum(const string& path)
{
   RWGuard mg(m_MetaLock, RW_READ);

   vector<string> dir;
   if (parsePath(path, dir) < 0)
      return SectorError::E_INVALID;

   map<string, SNode>* currdir = &m_mDirectory;
   map<string, SNode>::iterator s;
   for (vector<string>::const_iterator d = dir.begin(), end = dir.end(); d != end; ++ d)
   {
      s = currdir->find(*d);
      if (s == currdir->end())
         return -1;

      currdir = &(s->second.m_mDirectory);
   }

   return getTotalFileNum(*currdir);
}

int Index::collectDataInfo(const string& file, vector<string>& result)
{
   RWGuard mg(m_MetaLock, RW_READ);

   vector<string> dir;
   if (parsePath(file, dir) <= 0)
      return -3;

   map<string, SNode>* currdir = &m_mDirectory;
   map<string, SNode>* updir = NULL;
   map<string, SNode>::iterator s;
   for (vector<string>::const_iterator d = dir.begin(); d != dir.end(); ++ d)
   {
      s = currdir->find(*d);
      if (s == currdir->end())
         return -1;

      updir = currdir;
      currdir = &(s->second.m_mDirectory);
   }

   if (!s->second.m_bIsDir)
   {
      string idx = *dir.rbegin() + ".idx";
      int64_t rows = -1;
      map<string, SNode>::iterator i = updir->find(idx);
      if (i != updir->end())
         rows = i->second.m_llSize / 8 - 1;

      stringstream buf;
      buf << file << " " << s->second.m_llSize << " " << rows;

      for (set<Address, AddrComp>::iterator j = s->second.m_sLocation.begin(); j != s->second.m_sLocation.end(); ++ j)
         buf << " " << j->m_strIP << " " << j->m_iPort;

      result.push_back(buf.str());
   }

   return collectDataInfo(file, *currdir, result);
}

int Index::checkReplica(const string& path, vector<string>& under, vector<string>& over,
        const std::map< std::string, int> & IPToCluster)
{
   under.clear();
   over.clear();

   RWGuard mg(m_MetaLock, RW_READ);

   vector<string> dir;
   if (parsePath(path, dir) < 0)
      return SectorError::E_INVALID;

   map<string, SNode>* currdir = &m_mDirectory;
   map<string, SNode>::iterator s;
   for (vector<string>::const_iterator d = dir.begin(), end = dir.end(); d != end; ++ d)
   {
      s = currdir->find(*d);
      if (s == currdir->end())
         return -1;

      currdir = &(s->second.m_mDirectory);
   }

   return checkReplica(path, *currdir, under, over, IPToCluster );
}

int Index::getSlaveMeta(Metadata* branch, const Address& addr)
{
   RWGuard mg(m_MetaLock, RW_READ);

   vector<string> path;
   return getSlaveMeta(m_mDirectory, path, ((Index*)branch)->m_mDirectory, addr);
}

///////////////////////////////////////////////////////////////////////////////////////

int Index::serialize(ofstream& ofs, map<string, SNode>& currdir, int level)
{
   for (map<string, SNode>::iterator i = currdir.begin(), end = currdir.end(); i != end; ++ i)
   {
      char* buf = NULL;
      if (i->second.serialize(buf) >= 0)
         ofs << level << " " << buf << endl;
      delete [] buf;

      if (i->second.m_bIsDir)
         serialize(ofs, i->second.m_mDirectory, level + 1);
   }

   return 0;
}

int Index::deserialize(ifstream& ifs, map<string, SNode>& metadata, const Address* addr)
{
   vector<string> dirs;
   dirs.resize(1024);
   map<string, SNode>* currdir = &metadata;
   int currlevel = 1;

   while (!ifs.eof())
   {
      char tmp[4096];
      tmp[4095] = 0;
      char* buf = tmp;

      ifs.getline(buf, 4096);
      int len = strlen(buf);
      if ((len <= 0) || (len >= 4095))
         continue;

      for (int i = 0; i < len; ++ i)
      {
         if (buf[i] == ' ')
         {
            buf[i] = '\0';
            break;
         }
      }

      int level = atoi(buf);

      SNode sn;
      sn.deserialize(buf + strlen(buf) + 1);
      if ((!sn.m_bIsDir) && (NULL != addr))
      {
         sn.m_sLocation.clear();
         sn.m_sLocation.insert(*addr);
      }

      if (level == currlevel)
      {
         (*currdir)[sn.m_strName] = sn;
         dirs[level] = sn.m_strName;
      }
      else if (level == currlevel + 1)
      {
         map<string, SNode>::iterator s = currdir->find(dirs[currlevel]);
         currdir = &(s->second.m_mDirectory);
         currlevel = level;

         (*currdir)[sn.m_strName] = sn;
         dirs[level] = sn.m_strName;
      }
      else if (level < currlevel)
      {
         currdir = &metadata;

         for (int i = 1; i < level; ++ i)
         {
            map<string, SNode>::iterator s = currdir->find(dirs[i]);
            currdir = &(s->second.m_mDirectory);
         }
         currlevel = level;

         (*currdir)[sn.m_strName] = sn;
         dirs[level] = sn.m_strName;
      }
   }

   return 0;
}

int Index::scan(const string& currdir, map<string, SNode>& metadata)
{
   vector<SNode> filelist;
   if (LocalFS::list_dir(currdir, filelist) < 0)
      return -1;

   metadata.clear();

   for (vector<SNode>::iterator i = filelist.begin(); i != filelist.end(); ++ i)
   {
      // skip "." and ".."
      if (i->m_strName.empty() || (i->m_strName == ".") || (i->m_strName == ".."))
         continue;

      // check file name
      bool bad = false;
      for (char *p = (char*)i->m_strName.c_str(), *q = p + i->m_strName.length(); p != q; ++ p)
      {
         if ((*p == 10) || (*p == 13))
         {
            bad = true;
            break;
         }
      }
      if (bad)
         continue;

      // skip system file and directory
      if (i->m_bIsDir && (i->m_strName.c_str()[0] == '.'))
         continue;

      metadata[i->m_strName] = *i;
      map<string, SNode>::iterator mi = metadata.find(i->m_strName);

      if (mi->second.m_bIsDir)
         scan(currdir + mi->first + "/", mi->second.m_mDirectory);
   }

   return metadata.size();
}

int Index::merge(map<string, SNode>& currdir, map<string, SNode>& branch, const unsigned int& replica)
{
   vector<string> tbd;

   for (map<string, SNode>::iterator i = branch.begin(); i != branch.end(); ++ i)
   {
      map<string, SNode>::iterator s = currdir.find(i->first);

      if (s == currdir.end())
      {
         currdir[i->first] = i->second;
         tbd.push_back(i->first);
      }
      else
      {
         if (i->second.m_bIsDir && s->second.m_bIsDir)
         {
            // directories with same name

            merge(s->second.m_mDirectory, i->second.m_mDirectory, replica);

            // if all files have been successfully merged, remove the directory name
            if (i->second.m_mDirectory.empty())
               tbd.push_back(i->first);
         }
         else if (!(i->second.m_bIsDir) && !(s->second.m_bIsDir) 
                  && (i->second.m_llSize == s->second.m_llSize) 
                  && (i->second.m_llTimeStamp == s->second.m_llTimeStamp))
                  //&& (s->second.m_sLocation.size() < replica))
         {
            // files with same name, size, timestamp
            // and the number of replicas is below the threshold
            for (set<Address, AddrComp>::iterator a = i->second.m_sLocation.begin(); a != i->second.m_sLocation.end(); ++ a)
               s->second.m_sLocation.insert(*a);
            tbd.push_back(i->first);
         }
      }
   }

   for (vector<string>::iterator i = tbd.begin(); i != tbd.end(); ++ i)
      branch.erase(*i);

   return 0;
}

int Index::substract(map<string, SNode>& currdir, const Address& addr)
{
   vector<string> tbd;

   for (map<string, SNode>::iterator i = currdir.begin(); i != currdir.end(); ++ i)
   {
      if (!i->second.m_bIsDir)
      {
         i->second.m_sLocation.erase(addr);
         if (i->second.m_sLocation.empty())
            tbd.insert(tbd.end(), i->first);
      }
      else
         substract(i->second.m_mDirectory, addr);
   }

   for (vector<string>::iterator i = tbd.begin(); i != tbd.end(); ++ i)
      currdir.erase(*i);

   return 0;
}

int64_t Index::getTotalDataSize(const map<string, SNode>& currdir) const
{
   int64_t size = 0;

   for (map<string, SNode>::const_iterator i = currdir.begin(), end = currdir.end(); i != end; ++ i)
   {
      if (!i->second.m_bIsDir)
         size += i->second.m_llSize;
      else
         size += getTotalDataSize(i->second.m_mDirectory);
   }

   return size;
}

int64_t Index::getTotalFileNum(const map<string, SNode>& currdir) const
{
   int64_t num = 0;

   for (map<string, SNode>::const_iterator i = currdir.begin(); i != currdir.end(); ++ i)
   {
      if (!i->second.m_bIsDir)
         num ++;
      else
         num += getTotalFileNum(i->second.m_mDirectory);
   }

   return num;
}

int Index::collectDataInfo(const string& path, const map<string, SNode>& currdir, vector<string>& result) const
{
   for (map<string, SNode>::const_iterator i = currdir.begin(); i != currdir.end(); ++ i)
   {
      if (!i->second.m_bIsDir)
      {
         // skip system files
         if (i->first.c_str()[0] == '.')
           continue;

         // ignore index file
         int t = i->first.length();
         if ((t > 4) && (i->first.substr(t - 4, t) == ".idx"))
            continue;

         string idx = i->first + ".idx";
         int64_t rows = -1;
         map<string, SNode>::const_iterator j = currdir.find(idx);
         if (j != currdir.end())
            rows = j->second.m_llSize / 8 - 1;

         stringstream buf;
         buf << path + "/" + i->first << " " << i->second.m_llSize << " " << rows;

         for (set<Address, AddrComp>::iterator k = i->second.m_sLocation.begin(); k != i->second.m_sLocation.end(); ++ k)
            buf << " " << k->m_strIP << " " << k->m_iPort;

         result.push_back(buf.str());
      }
      else
         collectDataInfo((path + "/" + i->first).c_str(), i->second.m_mDirectory, result);
   }

   return result.size();
}


set<int> getClustersForPath( const std::string& path, const std::map<std::string, std::vector<int> >& restrictedLoc  )
{
   for (map<string, vector<int> >::const_iterator i = restrictedLoc.begin(); i != restrictedLoc.end(); ++ i)
      if (WildCard::contain(i->first, path))
         return set<int>( i->second.begin(), i->second.end() );

  return set<int>();
}

int Index::checkReplica(const string& path, const map<string, SNode>& currdir, vector<string>& under, 
                     vector<string>& over, const std::map< std::string, int> & IPToCluster) const
{
   for (map<string, SNode>::const_iterator i = currdir.begin(), end = currdir.end(); i != end; ++ i)
   {
      string abs_path = path;
      if (path == "/")
         abs_path += i->first;
      else
         abs_path += "/" + i->first;

      if (i->second.m_bIsDir)
      {
        checkReplica(abs_path, i->second.m_mDirectory, under, over, IPToCluster);
        continue;
      }
      unsigned int target_rep_num;
      unsigned int target_max_rep_num;
      map<string, SNode>::const_iterator ns = i->second.m_mDirectory.find(".nosplit");
      // if this is a directory and it contains a file called ".nosplit", the whole directory will be replicated together
      if (ns != i->second.m_mDirectory.end())
      {
        target_rep_num = ns->second.m_iReplicaNum;
        target_max_rep_num = ns->second.m_iMaxReplicaNum;
      }
      else
      {
        target_rep_num = i->second.m_iReplicaNum;
        target_max_rep_num = i->second.m_iMaxReplicaNum;
      }
      
      unsigned int curr_rep_num = i->second.m_sLocation.size();
      if (curr_rep_num > target_max_rep_num)
      {
        over.push_back(abs_path);
        continue;
      }
      if (curr_rep_num < target_rep_num)
      {
        under.push_back(abs_path);
        continue;
      }
      // now we left with curr_rep_num between target_rep_num and target_max_rep_num, and will be checking only
      //  for replicas on wrong cluster or same ip, which will be marked as underreplicated
      if ( m_bCheckReplicaCluster )
      {
        if( !i->second.m_viRestrictedLoc.empty() )
        {
           bool found = false;
           for( set<Address, AddrComp>::const_iterator loc =  i->second.m_sLocation.begin(); 
                                                       loc != i->second.m_sLocation.end(); ++loc )
           {
              map< std::string, int>::const_iterator clu = IPToCluster.find( loc->m_strIP );
              if ( clu == IPToCluster.end() )
              {
                under.push_back(abs_path);
                found = true;
                break;
              }

              vector< int >::const_iterator vs = find(i->second.m_viRestrictedLoc.begin(),
                                                      i->second.m_viRestrictedLoc.end(),
                                                      clu->second);
       
              if (vs ==  i->second.m_viRestrictedLoc.end())
              {
                under.push_back(abs_path);
                found = true;
                break;
              }
           }
           if ( found ) 
             continue;
        }
      }
      if( m_bCheckReplicaOnSameIp && i->second.m_sLocation.size() > 1 )
      { 
        std::string cur_ip;
        std::set<Address, AddrComp>::const_iterator cur  = i->second.m_sLocation.begin();
        std::set<Address, AddrComp>::const_iterator last = i->second.m_sLocation.end();
        for( ; cur != last; ++cur )
          if( cur->m_strIP == cur_ip )
          {
            under.push_back(abs_path);
            break;
          } else
            cur_ip = cur->m_strIP;
          
      }      
   }

   return 0;
}

int Index::list_r(const map<string, SNode>& currdir, const string& path, vector<string>& filelist) const
{
   for (map<string, SNode>::const_iterator i = currdir.begin(); i != currdir.end(); ++ i)
   {
      if (i->second.m_bIsDir)
      {
         // nosplit dir return name only
         if (i->second.m_mDirectory.find(".nosplit") != i->second.m_mDirectory.end())
         {
            filelist.insert(filelist.end(), path + "/" + i->second.m_strName);
         }
         else if (i->second.m_mDirectory.empty())
         {
            filelist.insert(filelist.end(), path + "/" + i->second.m_strName);
         }
         else
         {
            list_r(i->second.m_mDirectory, path + "/" + i->second.m_strName, filelist);
         }
      }
      else
      {
         filelist.insert(filelist.end(), path + "/" + i->second.m_strName);
      }
   }

   return filelist.size();
}

int Index::getSlaveMeta(const map<string, SNode>& currdir, const vector<string>& path, map<string, SNode>& target, const Address& addr) const
{
   for (map<string, SNode>::const_iterator i = currdir.begin(); i != currdir.end(); ++ i)
   {
      if (!i->second.m_bIsDir)
      {
         if (i->second.m_sLocation.find(addr) != i->second.m_sLocation.end())
         {
            map<string, SNode>* currdir = &target;
            for (vector<string>::const_iterator d = path.begin(); d != path.end(); ++ d)
            {
               map<string, SNode>::iterator s = currdir->find(*d);
               if (s == currdir->end())
               {
                  SNode n;
                  n.m_strName = *d;
                  n.m_bIsDir = true;
                  n.m_llTimeStamp = time(NULL);
                  n.m_llSize = 0;
                  (*currdir)[*d] = n;
                  s = currdir->find(*d);
               }

               currdir = &(s->second.m_mDirectory);
            }

            (*currdir)[i->first] = i->second;
         }
      }
      else
      {
         vector<string> new_path = path;
         new_path.push_back(i->first);
         getSlaveMeta(i->second.m_mDirectory, new_path, target, addr);
      }
   }

   return 0;
}

void Index::refreshRepSetting(const string& path, int default_num, int default_dist, const map<string, pair<int,int> >& rep_num, const map<string, int>& rep_dist, const map<string, vector<int> >& restrict_loc)
{
   RWGuard mg(m_MetaLock, RW_WRITE);

   vector<string> dir;
   if (parsePath(path, dir) < 0)
     return;
   
   map<string, SNode>* currdir = &m_mDirectory;
   map<string, SNode>::iterator s;
   SNode * curnode = NULL;
   for (vector<string>::iterator d = dir.begin(); d != dir.end(); ++ d)
   {
      s = currdir->find(*d);
      if (s == currdir->end())
         return;
      curnode = &(s->second);
      currdir = & (curnode->m_mDirectory);
      
   }
   if ( curnode != NULL && !curnode->m_bIsDir )
     refreshRepSetting(path, *curnode, default_num, default_dist, rep_num, rep_dist, restrict_loc);
   else
     refreshRepSetting(path, *currdir, default_num, default_dist, rep_num, rep_dist, restrict_loc);
}

int Index::refreshRepSetting(const string& path, map<string, SNode>& currdir, int default_num, int default_dist, const map<string, pair<int,int> >& rep_num, const map<string, int>& rep_dist, const map<string, vector<int> >& restrict_loc)
{
   //TODO: use wildcard match each level of dir, instead of contain()
   string slash = "/";
   if ( path == "/" ) 
     slash = "";

   for (map<string, SNode>::iterator i = currdir.begin(); i != currdir.end(); ++ i)
   {
      refreshRepSetting(path + slash + i->second.m_strName, i->second, default_num, default_dist, rep_num, rep_dist, restrict_loc);

   }
   return 0;
}

int Index::refreshRepSetting(const string& path, SNode & node, int default_num, int default_dist, const map<string, pair<int,int> >& rep_num, const map<string, int>& rep_dist, const map<string, vector<int> >& restrict_loc)
{
//  string abs_path = path;
//  if (path == "/")
//    abs_path += node.m_strName;
//  else
//    abs_path += "/" + node.m_strName;

  // set replication factor
  node.m_iReplicaNum = default_num;
  node.m_iMaxReplicaNum = default_num;
  for (map<string, std::pair<int,int> >::const_iterator rn = rep_num.begin(); rn != rep_num.end(); ++ rn)
  {
     if (WildCard::contain(rn->first, path))
     {
        node.m_iReplicaNum = rn->second.first;
        node.m_iMaxReplicaNum = rn->second.second;
        break;
     }
  }
  // set replication distance
  node.m_iReplicaDist = default_dist;
  for (map<string, int>::const_iterator rd = rep_dist.begin(); rd != rep_dist.end(); ++ rd)
  {
     if (WildCard::contain(rd->first, path))
     {
        node.m_iReplicaDist = rd->second;
        break;
      }
  }

  // set restricted location
  node.m_viRestrictedLoc.clear();
  for (map<string, vector<int> >::const_iterator rl = restrict_loc.begin(); rl != restrict_loc.end(); ++ rl)
  {
     if (WildCard::contain(rl->first, path))
     {
        node.m_viRestrictedLoc = rl->second;
        break;
     }
  }
  if (node.m_bIsDir)
    refreshRepSetting(path, node.m_mDirectory, default_num, default_dist, rep_num, rep_dist, restrict_loc);
  return 0;
}
