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
   Yunhong Gu, last updated 03/17/2011
*****************************************************************************/

#include <sector.h>
#include <list>
#include <iostream>

using namespace std;

void help()
{
   cerr << "USAGE: sector_cp <src_file/dir> <dst_file/dir> [--sync]\n";
}

int getRepFileList(Sector& client, const string& path, map<string, int>& fl)
{
   SNode attr;
   if (client.stat(path.c_str(), attr) < 0)
      return -1;

   if (attr.m_bIsDir)
   {
      vector<SNode> subdir;
      client.list(path, subdir);

      for (vector<SNode>::iterator i = subdir.begin(); i != subdir.end(); ++ i)
      {
         if (i->m_bIsDir)
            getRepFileList(client, path + "/" + i->m_strName, fl);
         else if (i->m_iReplicaNum > int(i->m_sLocation.size()))
            fl[path + "/" + i->m_strName] = i->m_sLocation.size();
      }
   }

   return fl.size();
}

int getCopyFileList(Sector& client, const string& path, list<string>& fl)
{
   SNode attr;
   if (client.stat(path.c_str(), attr) < 0)
      return -1;

   fl.push_back(path);

   if (attr.m_bIsDir)
   {
      vector<SNode> subdir;
      client.list(path, subdir);

      for (vector<SNode>::iterator i = subdir.begin(); i != subdir.end(); ++ i)
      {
         if (i->m_bIsDir)
            getCopyFileList(client, path + "/" + i->m_strName, fl);
         else
            fl.push_back(path + "/" + i->m_strName);
      }
   }

   return fl.size();
}

int main(int argc, char** argv)
{
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

   bool synchronize = false;

   for (vector<string>::const_iterator i = clp.m_vSFlags.begin(); i != clp.m_vSFlags.end(); ++ i)
   {
      if (*i == "sync")
         synchronize = true;
      else
      {
         help();
         return -1;
      }
   }

   string src = clp.m_vParams[0];
   string dst = clp.m_vParams[1];

   if (WildCard::isWildCard(dst))
   {
      cerr << "destination file/directory cannot be wildcards.\n";
      return -1;
   }

   Sector client;
   if (Utility::login(client) < 0)
      return -1;

   int result = 0;

   if (src == dst)
   {
      // when src == dst, src will be replicated
      SNode attr;
      if ((result = client.stat(src, attr)) < 0)
      {
         Utility::print_error(result);
         Utility::logout(client);
         return -1;
      }

      map<string, int> original_status;

      if (!attr.m_bIsDir)
      {
         if (attr.m_iReplicaNum <= int(attr.m_sLocation.size()))
         {
            cerr << "The file already reached its maximum replication number.\n";
            Utility::logout(client);
            return -1;
         }

         original_status[src] = attr.m_sLocation.size();
      }
      else if (synchronize)
      {
         getRepFileList(client, src, original_status);
      }

      if ((result = client.copy(src, dst)) < 0)
      {
         Utility::print_error(result);
         Utility::logout(client);
         return -1;
      }

      if (synchronize)
      {
          int interval = 1; //second

         // wait for replication to be completed
         while (!original_status.empty())
         {
            vector<string> tbr;
            tbr.clear();
            for (map<string, int>::iterator i = original_status.begin(); i != original_status.end(); ++ i)
            {
               if (client.stat(i->first, attr) < 0)
                  tbr.push_back(i->first);
               else if (int(attr.m_sLocation.size()) > i->second)
                  tbr.push_back(i->first);
            }

            for (vector<string>::iterator i = tbr.begin(); i != tbr.end(); ++ i)
               original_status.erase(*i);

            //TODO: check available disk space and available slave > replicanum

            if (original_status.empty())
               break;

            #ifndef WIN32
               sleep(interval);
            #else
               Sleep(interval * 1000);
            #endif
            if (interval < 16)
               interval *= 2;
         }
      }
   }
   else
   {
      vector<string> filelist;

      string path = src;
      bool wc = WildCard::isWildCard(path);

      if (!wc)
      {
         filelist.push_back(src);
      }
      else
      {
         string orig = path;
         size_t p = path.rfind('/');
         if (p == string::npos)
            path = "/";
         else
         {
            path = path.substr(0, p);
            orig = orig.substr(p + 1, orig.length() - p);
         }

         vector<SNode> filelist;
         if ((result = client.list(path, filelist)) < 0)
            Utility::print_error(result);

         vector<string> filtered;
         for (vector<SNode>::iterator i = filelist.begin(); i != filelist.end(); ++ i)
         {
            if (WildCard::match(orig, i->m_strName))
               filtered.push_back(path + "/" + i->m_strName);
         }
      }

      bool dst_exist = true;
      SNode attr;
      if (client.stat(dst, attr) < 0)
         dst_exist = false;

      if (filelist.empty())
      {
         Utility::logout(client);
         return 0;
      }

      if (filelist.size() > 1)
      {
         if (!dst_exist)
         {
            cerr << "destination directory does not exist.\n";
            Utility::logout(client);
            return 0;
         }
      }

      list<string> new_files;

      for (vector<string>::iterator i = filelist.begin(); i != filelist.end(); ++ i)
      {
         if ((result = client.copy(*i, dst)) < 0)
            Utility::print_error(result);
         else if (synchronize)
            getCopyFileList(client, *i, new_files);
      }

      if (synchronize)
      {
         string rep;
         if ((new_files.size() == 1) && (!dst_exist))
            rep = src;
         else if (src.rfind('/') != string::npos)
            rep = src.substr(0, src.rfind('/'));
         else
            rep = "";
         int rep_len = rep.length();

         for (list<string>::iterator i = new_files.begin(); i != new_files.end(); ++ i)
         {
            if (rep_len > 0)
               i->replace(0, rep_len, dst);
            else
               *i = dst + "/" + *i;
         }

         int interval = 1; //second

         // wait for copy to be completed
         while (!new_files.empty())
         {
            for (list<string>::iterator i = new_files.begin(); i != new_files.end();)
            {
               if (client.stat(*i, attr) >= 0)
               {
                  list<string>::iterator j = i;
                  ++ i;
                  new_files.erase(j);
               }
               else
               {
                  ++ i;
               }
            }

            //TODO: check available disk space

            if (new_files.empty())
               break;

            #ifndef WIN32
               sleep(interval);
            #else
               Sleep(interval * 1000);
            #endif
            if (interval < 16)
               interval *= 2;
         }
      }
   }

   Utility::logout(client);

   return 0;
}
