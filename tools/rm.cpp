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

#include <iostream>
#include <sector.h>

using namespace std;

void help()
{
   cerr << "USAGE: rm <dir> [--f]\n";
   cerr << "use -f to force to remove recursively.\n";
}

bool isRecursive(const string& path)
{
   cout << "Directory " << path << " is not empty. Force to remove? Y/N: ";
   char input;
   cin >> input;

   return (input == 'Y') || (input == 'y');
}

int main(int argc, char** argv)
{
   CmdLineParser clp;
   clp.parse(argc, argv);

   if ((clp.m_vParams.size() != 1))
   {
      help();
      return 0;
   }

   bool recursive = false;
   if (!clp.m_vSFlags.empty())
   {
      for (vector<string>::iterator i = clp.m_vSFlags.begin(); i != clp.m_vSFlags.end(); ++ i)
      {
         if (*i == "f")
            recursive = true;
         else
            cerr << "unknown flag " << *i << " ignored.\n";
      }
   }

   Sector client;
   if (Utility::login(client) < 0)
      return -1;

   string path = *clp.m_vParams.begin();
   bool wc = WildCard::isWildCard(path);

   if (!wc)
   {
      int result = client.remove(path);

      if (result == SectorError::E_NOEMPTY)
      {
         if (recursive || isRecursive(path))
            client.rmr(path);
      }
      else if (result < 0)
      {
         Utility::print_error(result);
      }
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

      int result = 0;

      vector<SNode> filelist;
      if ((result = client.list(path, filelist)) < 0)
         Utility::print_error(result);

      bool recursive = false;

      vector<string> filtered;
      for (vector<SNode>::iterator i = filelist.begin(); i != filelist.end(); ++ i)
      {
         if (WildCard::match(orig, i->m_strName))
         {
            if (recursive)
               client.rmr(path + "/" + i->m_strName);
            else
            {
               int result = client.remove(path + "/" + i->m_strName);

               if (result == SectorError::E_NOEMPTY)
               {
                  if (recursive || isRecursive(path + "/" + i->m_strName))
                     client.rmr(path + "/" + i->m_strName);
               }
               else if (result < 0)
               {
                  Utility::print_error(result);
               }
            }
         }
      }
   }

   Utility::logout(client);

   return 0;
}
