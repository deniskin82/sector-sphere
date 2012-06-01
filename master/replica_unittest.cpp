/*****************************************************************************
Copyright 2011 VeryCloud LLC

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
   bdl62, last updated 04/21/2011
*****************************************************************************/

#include <cassert>
#include <iostream>
#include <string>

#include "replica.h"

using namespace std;
using namespace sector;

int test1()
{
   const string file1 = "test1";
   const string file2 = "test2";

   ReplicaMgmt rm;

   ReplicaJob job;
   job.m_strSource = file1;
   job.m_iPriority = COPY;  // priority 0.
   rm.insert(job);
   assert(rm.getTotalNum() == 1);

   ReplicaMgmt::iterator iter = rm.begin();
   assert(iter->m_strSource == file1);

   job.m_strSource = file2;
   job.m_iPriority = BACKGROUND;  // priority 1.
   rm.insert(job);
   assert(rm.getTotalNum() == 2);

   ReplicaMgmt::iterator tmp = iter;
   ++iter;
   rm.erase(tmp);
   assert(rm.getTotalNum() == 1);

   assert(iter->m_strSource == file2);

   rm.clear();

   return 0;
}

int main()
{
   test1();

   return 0;
}
