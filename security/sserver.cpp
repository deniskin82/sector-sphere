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

#include <sector.h>
#include <security.h>
#include <filesrc.h>
#include <iostream>

using namespace std;
using namespace sector;

int main(int argc, char** argv)
{
   cout << SectorVersionString << endl;

   SServer ss;

   int port = 5000;
   if (argc == 2)
      port = atoi(argv[1]);

   string sector_home;
   if (ConfLocation::locate(sector_home) < 0)
   {
      cerr << "cannot locate security server configurations.\n";
      return -1;
   }

   if (ss.init(port, (sector_home + "/conf/security_node.cert").c_str(), (sector_home + "/conf/security_node.key").c_str()) < 0)
   {
      cerr << "failed to initialize security server at port " << port << endl;
      cerr << "Secuirty server failed to start. Please fix the problem.\n";
      return -1;
   }

   // using the simple user accounts stored in files
   // other driver can be used to connect to LDAP, database, or system accounts
   FileSrc* src = new FileSrc;

   if (src->init(sector_home.c_str()) < 0)
   {
      cerr << "Unable to locate any security source.\n";
      return -1;
   }

   ss.setSecuritySource(src);

   cout << "Sector Security server running at port " << port << endl << endl;
   cout << "The server is started successfully; there is no further output from this program. Please do not shutdown the security server; otherwise no client may be able to login. If the server is down for any reason, you can restart it without restarting the masters and the slaves.\n";

   ss.run();

   cerr << "Security server was interrupted.\n";

   ss.close();
   delete src;

   return 0;
}
