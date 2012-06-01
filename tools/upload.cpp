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
   Yunhong Gu, last updated 03/15/2011
*****************************************************************************/

#include <errno.h>
#include <fcntl.h>
#include <iostream>
#include <osportable.h>
#include <sector.h>
#include <stddef.h>
#include <stdio.h>
#include <stdlib.h>
#include <string>
#include <string.h>
#ifndef WIN32
   #include <sys/types.h>
   #include <sys/stat.h>
#endif

using namespace std;

void help()
{
   cerr << "usage: sector_upload <src file/dir> <dst dir> [-n num_of_replicas] [-a ip_address] [-c cluster_id] [--e(ncryption)] --smart" << endl;
}

int upload(const char* file, const char* dst, Sector& client, const int rep_num, const string& ip, const string& cid, const bool secure, const bool smart)
{
   //check if file already exists

   SNode s;
   if (LocalFS::stat(file, s) < 0)
   {
      cout << "cannot locate source file " << file << endl;
      return -1;
   }

   SNode attr;
   if (client.stat(dst, attr) >= 0)
   {
      if (attr.m_llSize == s.m_llSize && smart)
      {
         cout << "destination file " << dst << " exists on Sector FS. skip.\n";
         return 0;
      }
   }

   cout << "uploading " << file << " of " << s.m_llSize << " bytes" << endl;

   timeval t1, t2;
   gettimeofday(&t1, 0);

   SectorFile* f = client.createSectorFile();

   SF_OPT option;
   option.m_llReservedSize = s.m_llSize;
   if (option.m_llReservedSize <= 0)
      option.m_llReservedSize = 1;
   option.m_iReplicaNum = rep_num;
   option.m_strHintIP = ip;
   option.m_strCluster = cid;

   int mode = SF_MODE::WRITE | SF_MODE::TRUNC;
   if (secure)
      mode |= SF_MODE::SECURE;

   int r = f->open(dst, mode, &option);
   if (r < 0)
   {
      cerr << "unable to open file " << dst << endl;
      Utility::print_error(r);
      return -1;
   }

   int64_t result = f->upload(file);

   f->close();
   client.releaseSectorFile(f);

   if (result >= 0)
   {
      gettimeofday(&t2, 0);
      float throughput = s.m_llSize * 8.0 / 1000000.0 / ((t2.tv_sec - t1.tv_sec) + (t2.tv_usec - t1.tv_usec) / 1000000.0);

      cout << "Uploading accomplished! " << "AVG speed " << throughput << " Mb/s." << endl << endl ;
   }
   else
   {
      cout << "Uploading failed! Please retry. " << endl << endl;
      Utility::print_error(result);
      return result;
   }

   return 0;
}

int getFileList(const string& path, vector<string>& fl)
{
   fl.push_back(path);

   SNode s;
   if (LocalFS::stat(path, s) < 0)
      return -1;

   if (s.m_bIsDir)
   {
      // if there is a ".nosplit" file, must upload this file the first in the directory, subsequent files will be uploaded to the same node
      if (LocalFS::stat(path + "/.nosplit", s) > 0)
         fl.push_back(path + "/.nosplit");

      vector<SNode> curr_fl;
      if (LocalFS::list_dir(path, curr_fl) < 0)
         return -1;

      for (vector<SNode>::iterator i = curr_fl.begin(); i != curr_fl.end(); ++ i)
      {
         // skip "." and ".."
         if ((i->m_strName == ".") || (i->m_strName == ".."))
            continue;

         string subdir = path + "/" + i->m_strName;

         if (i->m_bIsDir)
            getFileList(subdir, fl);
         else
            fl.push_back(subdir);
      }
   }

   return fl.size();
}

int main(int argc, char** argv)
{
   if (argc < 3)
   {
      help();
      return -1;
   }

   CmdLineParser clp;
   if (clp.parse(argc, argv) < 0)
   {
      help();
      return -1;
   }

   if (clp.m_vParams.size() < 2)
   {
      help();
      return -1;
   }

   int replica_num = 1;
   string ip = "";
   string cluster = "";
   int parallel = 1;  // concurrent uploading multiple files

   bool encryption = false;
   bool smart = false;

   for (map<string, string>::const_iterator i = clp.m_mDFlags.begin(); i != clp.m_mDFlags.end(); ++ i)
   {
      if (i->first == "n")
         replica_num = atoi(i->second.c_str());
      else if (i->first == "a")
         ip = i->second;
      else if (i->first == "c")
         cluster = i->second;
      else if (i->first == "p")
         parallel = atoi(i->second.c_str());
      else
      {
         help();
         return -1;
      }
   }

   for (vector<string>::const_iterator i = clp.m_vSFlags.begin(); i != clp.m_vSFlags.end(); ++ i)
   {
      if (*i == "e")
         encryption = true;
      else if (*i == "smart" )
         smart = true;
      else
      {
         help();
         return -1;
      }
   }

   Sector client;
   if (Utility::login(client) < 0)
      return -1;

   string dstdir = *clp.m_vParams.rbegin();
   clp.m_vParams.pop_back();
   
   SNode attr;
   int r = client.stat(dstdir, attr);
   if ((r < 0) || (!attr.m_bIsDir))
   {
      cerr << "destination directory on Sector does not exist.\n";
      Utility::logout(client);
      return -1;
   }

   bool success = true;

   // upload multiple files/dirs
   for (vector<string>::const_iterator param = clp.m_vParams.begin(); param != clp.m_vParams.end(); ++ param)
   {
      string prefix = "";

      vector<string> fl;
      bool wc = WildCard::isWildCard(*param);
      if (!wc)
      {
         SNode s;
         if (LocalFS::stat(*param, s) < 0)
         {
            cerr << "ERROR: source file does not exist.\n";
            return -1;
         }

         if (s.m_bIsDir)
            prefix = *param;
         else
         {
            size_t pos = param->rfind('/');
            if (pos != string::npos)
               prefix = param->substr(0, pos);
         }

         getFileList(*param, fl);
      }
      else
      {
         size_t pos = param->rfind('/');
         if (pos != string::npos)
            prefix = param->substr(0, pos);

         string path = *param;
         string orig = path;
         size_t p = path.rfind('/');
         if (p == string::npos)
         {
            path = ".";
         }
         else
         {
            path = path.substr(0, p);
            orig = orig.substr(p + 1, orig.length() - p);
         }

         // as this is a wildcard, list all files in the current dir, choose those matched ones
         vector<SNode> curr_fl;
         if (LocalFS::list_dir(path, curr_fl) < 0)
            return -1;

         for (vector<SNode>::iterator s = curr_fl.begin(); s != curr_fl.end(); ++ s)
         {
            // skip "." and ".."
            if ((s->m_strName == ".") || (s->m_strName == ".."))
               continue;

            if (WildCard::match(orig, s->m_strName))
            {
               if (path == ".")
                  getFileList(s->m_strName, fl);
               else
                  getFileList(path + "/" + s->m_strName, fl);
            }
         }
      }

      // upload all files in the file list
      for (vector<string>::const_iterator i = fl.begin(); i != fl.end(); ++ i)
      {
         // process directory name change: /src/mydata -> /dst/mydata
         string dst = *i;
         if (prefix.length() > 0)
            dst.replace(0, prefix.length(), dstdir + "/");
         else
            dst = dstdir + "/" + dst;

         SNode s;
         if (LocalFS::stat(*i, s) < 0)
            continue;

         if (s.m_bIsDir)
            client.mkdir(dst);
         else
         {
   
            int result = upload(i->c_str(), dst.c_str(), client, replica_num, ip, cluster, encryption, smart);
            if ((result == SectorError::E_CONNECTION) ||
                (result == SectorError::E_BROKENPIPE))
            {
               // connection fail, retry once.
               result = upload(i->c_str(), dst.c_str(), client, replica_num, ip, cluster, encryption, smart);
            }
 
            if (result < 0)
            {
               // failed, remove the file in Sector.
               client.remove(dst);
               success = false;
               Utility::logout(client);
               return -1;
            }
         }
      }
   }

   Utility::logout(client);
   return success ? 0 : -1;
}
