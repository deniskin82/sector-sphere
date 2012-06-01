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
   Yunhong Gu, last updated 10/14/2010
*****************************************************************************/

#include <iostream>

#include "master.h"

using namespace std;
using namespace sector;

int main(int argc, char** argv)
{
   cout << SectorVersionString << endl;

   Master m;

   if (m.init() < 0)
      return -1;

   if (argc == 3)
   {
      if (m.join(argv[1], atoi(argv[2])) < 0)
         return -1;
   }

   // TODO: move master.conf parsing here, and add set... API to class Master


   cout << "Sector master is successfully running now. check the master log at $DATA_DIRECTORY/.log for more details.\n";
   cout << "There is no further screen output from this program.\n";

   m.run();

   sleep(1);

   return 0;
}
