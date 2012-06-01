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
   cout << "start_all [-s slaves.list] [-l slave_screen_log_output]" << endl;
}

int main(int argc, char** argv)
{
   string sector_home;
   if (ConfLocation::locate(sector_home) < 0)
   {
      cerr << "no Sector information located; nothing to start.\n";
      help();
      return -1;
   }

   CmdLineParser clp;
   clp.parse(argc, argv);

   string slaves_list = sector_home + "/conf/slaves.list";
   string slave_screen_log = "/dev/null";

   for (map<string, string>::const_iterator i = clp.m_mDFlags.begin(); i != clp.m_mDFlags.end(); ++ i)
   {
      if (i->first == "s")
         slaves_list = i->second;
      else if (i->first == "l")
         slave_screen_log = i->second;
      else
      {
         help();
         return 0;
      }
   }

   // starting master
   string cmd = string("nohup " + sector_home + "/master/start_master > /dev/null &");
   system(cmd.c_str());
   cout << "start master ...\n";

   set<SlaveStartInfo, SSIComp> ssi;
   if (Master::loadSlaveStartInfo(slaves_list, ssi) < 0)
   {
      cerr << "unable to load slave information from " << slaves_list << endl;
      return -1;
   }

   int count = 0;

   for (set<SlaveStartInfo, SSIComp>::iterator i = ssi.begin(); i != ssi.end(); ++ i)
   {
      if (++ count == 64)
      {
         // wait a while to avoid too many incoming slaves crashing the master
         // TODO: check number of active slaves so far
         #ifndef WIN32
         sleep(1);
         #else
         Sleep(1000);
         #endif
         count = 0;
      }

      Master::startSlave(i->m_strAddr, i->m_strBase, i->m_strOption, slave_screen_log);
   }

   return 0;
}
