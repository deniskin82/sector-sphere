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
   Yunhong Gu, last updated 05/23/2011
*****************************************************************************/

#include <common.h>
#include <string.h>
#include <meta.h>
#include <iostream>
using namespace std;

bool Metadata::m_pbLegalChar[256];
bool Metadata::m_bInit = Metadata::initLC();
int Metadata::m_iDefaultRepNum = 1;
int Metadata::m_iDefaultRepDist = 65536;
bool Metadata::m_bCheckReplicaOnSameIp = false;
int Metadata::m_iPctSlavesToConsider = 50;
int64_t Metadata::m_iLastTotalDiskSpace = 0;
time_t Metadata:: m_iLastTotalDiskSpaceTs = 0;
bool Metadata::m_bCheckReplicaCluster = false;

Metadata::Metadata()
{
   CGuard::createMutex(m_FileLockProtection);
}

Metadata::~Metadata()
{
   CGuard::releaseMutex(m_FileLockProtection);
}

void Metadata::setDefault(const int rep_num, const int rep_dist, bool allow_same_ip_replica, int pct_of_slaves_to_consider, bool check_replica_cluster)
{
   m_iDefaultRepNum = rep_num;
   m_iDefaultRepDist = rep_dist;
   m_bCheckReplicaOnSameIp = allow_same_ip_replica;
   m_iPctSlavesToConsider = pct_of_slaves_to_consider;
   m_bCheckReplicaCluster = check_replica_cluster;
}

int Metadata::lock(const string& path, int user, int mode)
{
   CGuard mg(m_FileLockProtection);

   if (mode & SF_MODE::WRITE)
   {
      // Write is exclusive
      if (!m_mLock[path].m_sWriteLock.empty() || !m_mLock[path].m_sReadLock.empty())
         return -1;

      m_mLock[path].m_sWriteLock.insert(user);
      if (mode & SF_MODE::READ)
         m_mLock[path].m_sReadLock.insert(user);
   }
   else if (mode & SF_MODE::READ)
   {
      if (!m_mLock[path].m_sWriteLock.empty())
         return -1;
      m_mLock[path].m_sReadLock.insert(user);
   }

   return 0;
}

int Metadata::unlock(const string& path, int user, int mode)
{
   CGuard mg(m_FileLockProtection);

   map<string, LockSet>::iterator i = m_mLock.find(path);

   if (i == m_mLock.end())
      return -1;

   if (mode & SF_MODE::WRITE)
      i->second.m_sWriteLock.erase(user);

   if (mode & SF_MODE::READ)
      i->second.m_sReadLock.erase(user);;

   if (i->second.m_sReadLock.empty() && i->second.m_sWriteLock.empty())
      m_mLock.erase(i);

   return 0;
}

std::map<std::string, Metadata::LockSet> Metadata::getLockList() 
{ 
   CGuard mg( m_FileLockProtection );
   return m_mLock; 
}

bool Metadata::isWriteLocked( const std::string& path )
{
   CGuard mg( m_FileLockProtection );
   map<string, LockSet>::const_iterator file = m_mLock.find( path );
   if( file == m_mLock.end() )
      return false;

   return !file->second.m_sWriteLock.empty();
}


int Metadata::parsePath(const string& path, vector<string>& result)
{
   result.clear();

   char* token = new char[path.length() + 1];
   int tc = 0;
   char* p = (char*)path.c_str();

   for (int i = 0, n = path.length(); i <= n; ++ i, ++ p)
   {
      if ((*p == '/') || (*p == '\0'))
      {
         if (tc > 0)
         {
            token[tc] = '\0';
            if (strcmp(token, ".") == 0)
            {
               // ignore current directory segment
            }
            else if (strcmp(token, "..") == 0)
            {
               if (result.empty())
               {
                  // upper level directory, must exist after some real directory name
                  delete [] token;
                  return -1;
               }
               // pop up one level
               result.pop_back();
            }
            else
            {
               result.push_back(token);
            }
            tc = 0;
         }
      }
      else
      {
         // check legal characters
         if (!m_pbLegalChar[int(*p)])
         {
            delete [] token;
            return -1;
         }

         token[tc ++] = *p;
      }
   }

   delete [] token;
   return result.size();
}

string Metadata::revisePath(const string& path)
{
   // empty path is regarded as "/"
   //if (path.length() == 0)
   //   return path;

   vector<string> path_vec;
   parsePath(path, path_vec);

   string tmp = "/";
   for (vector<string>::const_iterator i = path_vec.begin(); i != path_vec.end(); ++ i)
   {
      if (tmp.size() > 1)
         tmp.append(1, '/');
      tmp.append(*i);
   }

   return tmp;
}

bool Metadata::initLC()
{
   for (int i = 0; i < 256; ++ i)
   {
      m_pbLegalChar[i] = false;
   }

   m_pbLegalChar[32] = true; // Space
   m_pbLegalChar[36] = true; // $
   m_pbLegalChar[39] = true; // ' 
   m_pbLegalChar[40] = true; // (
   m_pbLegalChar[41] = true; // )
   m_pbLegalChar[42] = true; // *
   m_pbLegalChar[43] = true; // +
   m_pbLegalChar[45] = true; // -
   m_pbLegalChar[46] = true; // .

   // 0 - 9
   for (int i = 48; i <= 57; ++ i)
   {
      m_pbLegalChar[i] = true;
   }

   m_pbLegalChar[61] = true; // =
   m_pbLegalChar[63] = true; // ?
   m_pbLegalChar[64] = true; // @

   // A - Z
   for (int i = 65; i <= 90; ++ i)
   {
      m_pbLegalChar[i] = true;
   }

   m_pbLegalChar[95] = true; // _

   // a - z
   for (int i = 97; i <= 122; ++ i)
   {
      m_pbLegalChar[i] = true;
   }

   m_pbLegalChar[126] = true; // ~

   return true;
}
