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
   Yunhong Gu, last updated 01/12/2010
*****************************************************************************/

#include <sector.h>
#include <iostream>

using namespace std;

int main(int argc, char** argv)
{
   if (argc > 2)
   {
      cerr << "USAGE: sector_fsck [path] \n";
      return -1;
   }

   Sector client;
   if (Utility::login(client) < 0)
      return 0;

   string path = "/";
   if (argc == 2)
      path = argv[1];

   //SysStat sys;
   int r = client.fsck(path);
   if (r >= 0)
      sys.print();
   else
      cerr << "Error happened, failed to retrieve any system information.\n";

   Utility::logout(client);

   return r;
}
