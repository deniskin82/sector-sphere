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
   Yunhong Gu, last updated 02/08/2011
*****************************************************************************/

#include <cstdlib>
#include <cstring>
#include <fstream>
#include <iostream>

#include "master.h"
#include "sector.h"

using namespace std;
using namespace sector;

void help()
{
   cout << "stop_all [-s slaves_list] [--f(orce)]" << endl;
}


int main(int argc, char** argv)
{
   string sector_home;
   if (ConfLocation::locate(sector_home) < 0)
   {
      cerr << "no Sector information located; nothing to stop.\n";
      return -1;
   }

   string slaves_list = sector_home + "/conf/slaves.list";
   bool force = false;

   CmdLineParser clp;
   clp.parse(argc, argv);
   for (map<string, string>::const_iterator i = clp.m_mDFlags.begin(); i != clp.m_mDFlags.end(); ++ i)
   {
      if (i->first == "s")
         slaves_list = i->second;
      else
      {
         help();
         return 0;
      }
   }
   for (vector<string>::const_iterator i = clp.m_vSFlags.begin(); i != clp.m_vSFlags.end(); ++ i)
   {
      if ((*i == "f") || (*i == "force"))
         force = true;
      else
      {
         help();
         return 0;
      }
   }

   cout << "This will stop this master and all slave nodes by brutal forces. If you need a graceful shutdown, use ./tools/sector_shutdown.\n";

   if (!force)
   {
      cout << "Do you want to continue? Y/N:";
      char answer;
      cin >> answer;
      if ((answer != 'Y') && (answer != 'y'))
      {
         cout << "aborted.\n";
         return -1;
      }
   }

   system("killall -9 start_master");
   cout << "master node stopped\n";

   set<SlaveStartInfo, SSIComp> ssi;
   if (Master::loadSlaveStartInfo(slaves_list, ssi) < 0)
   {
      cerr << "unable to load slave information from " << slaves_list << endl;
      return -1;
   }

   for (set<SlaveStartInfo, SSIComp>::iterator i = ssi.begin(); i != ssi.end(); ++ i)
   {
      cout << "stop slave at " << i->m_strAddr << endl;
      system((string("ssh -o StrictHostKeychecking=no ") + i->m_strAddr + " killall -9 start_slave &").c_str());
   }

   return 0;
}
