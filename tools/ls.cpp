/****************************************************************************
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
   Yunhong Gu, last updated 01/12/2010
*****************************************************************************/

#include <iostream>
#include <iomanip>
#include <sector.h>

using namespace std;

int main(int argc, char** argv)
{
   if (argc < 2 || argc > 3)
   {
      cerr << "USAGE: ls [-r|-a] <dir>\n";
      return -1;
   }

   Sector client;
   if (Utility::login(client) < 0)
      return -2;

   //TODO: check original name first, continue to test wildcard if failed
   //file name may contain real * and ?

   //TODO: parse multiple level of wild card directories, such as */*.cpp

   string path;
   int r_level = 0;
   if (argc == 2) 
   {
     path = argv[1];
   }
      else
   {
     path = argv[2];
     string par = argv[1];
     if (par == "-r")
     {
       r_level=1;
     }
     else
     {
       if (par == "-a")
       {
         r_level=2;
       }
       else
       {
         cerr << "USAGE: ls [-r|-a] <dir>\n";
         return -1;
       }
     }
   }
   string orig = path;
   bool wc = WildCard::isWildCard(path);
   if (wc)
   {
      size_t p = path.rfind('/');
      if (p == string::npos)
         path = "/";
      else
      {
         path = path.substr(0, p);
         orig = orig.substr(p + 1, orig.length() - p);
      }
   }

   SNode attr;
   int result = client.stat(path, attr);
   if (result < 0)
   {
      Utility::print_error(result);
      Utility::logout(client);
      return 1;
   }

   vector<SNode> filelist;

   if (attr.m_bIsDir)
   {
      if ((result = client.list(path, filelist)) < 0)
      {
         Utility::print_error(result);
         Utility::logout(client);
         return -3;
      }
   }
   else
   {
      // if not a dir, list the file itself
      filelist.push_back(attr);
   }

   // show directory first
   for (vector<SNode>::iterator i = filelist.begin(); i != filelist.end(); ++ i)
   {
      if (wc && !WildCard::match(orig, i->m_strName))
         continue;

      if (!i->m_bIsDir)
         continue;

      time_t t = i->m_llTimeStamp;
      char buf[64];
      #ifndef WIN32
         ctime_r(&t, buf);
      #else
         ctime_s(buf, 64, &t);
      #endif
      for (char* p = buf; *p != '\n'; ++ p)
         cout << *p;
      if (r_level > 0)
         cout << "        ";
      cout << setiosflags(ios::right) << setw(16) << "<dir>" << "          ";

      setiosflags(ios::left);
      cout << i->m_strName << endl;
   }

   // then show regular files
   int fileCnt = 0;
   int64_t totalSize = 0;
   int64_t totalReplicaSize = 0;
   for (vector<SNode>::iterator i = filelist.begin(); i != filelist.end(); ++ i)
   {
      if (wc && !WildCard::match(orig, i->m_strName))
         continue;

      if (i->m_bIsDir)
         continue;
      fileCnt++;
      totalSize = totalSize + i->m_llSize;
      totalReplicaSize = totalReplicaSize + i->m_llSize * i->m_sLocation.size();
      time_t t = i->m_llTimeStamp;
      char buf[64];
      #ifndef WIN32
         ctime_r(&t, buf);
      #else
         ctime_s(buf, 64, &t);
      #endif
      for (char* p = buf; *p != '\n'; ++ p)
         cout << *p;
      if (r_level > 0) 
        cout << setiosflags(ios::right) << setw(4) << i->m_sLocation.size() << setw(4) << i->m_iReplicaNum 
             << setw(4) << i->m_iMaxReplicaNum;

      cout << setiosflags(ios::right) << setw(16) << i->m_llSize << " bytes    ";

      setiosflags(ios::left);
      cout << i->m_strName;
      if (r_level == 2)
      {
        cout << "  {";
        int first=0;
        for (set<Address, AddrComp>::iterator it = i->m_sLocation.begin(); it != i->m_sLocation.end(); ++it)
        {
          if (first++) cout << ",";
          cout << it->m_strIP<<':'<< it->m_iPort;
        }
        cout<<"}";
      } 
      cout << endl;
   }
   if (r_level > 0)
    cout << "Total Files: " << fileCnt << " Size: " << totalSize << " bytes, Replica size: " <<
      totalReplicaSize << " bytes" << endl;
   Utility::logout(client);

   return 0;
}
