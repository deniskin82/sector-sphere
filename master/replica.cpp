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


#include <cassert>
#include <iostream>

#include "common.h"
#include "replica.h"

using namespace std;
using namespace sector;

ReplicaJob::ReplicaJob():
m_strSource(""),
m_strDest(""),
m_iPriority(BACKGROUND),
m_llTimeStamp(CTimer::getTime()),
m_llSize(0),
m_bForceReplicate(false)
{
};

ReplicaIterator::ReplicaIterator():
m_pMgmtInstance(NULL)
{
}

ReplicaIterator& ReplicaIterator::operator=(const ReplicaIterator& iter)
{
   if (&iter == this)
      return *this;

   m_pMgmtInstance = iter.m_pMgmtInstance;
   m_iPriority = iter.m_iPriority;
   m_ListIter = iter.m_ListIter;

   return *this;
}

bool ReplicaIterator::operator!=(const ReplicaIterator& iter) const
{
   return !(*this == iter);
}

bool ReplicaIterator::operator==(const ReplicaIterator& iter) const
{
   if (!m_pMgmtInstance || !iter.m_pMgmtInstance)
      return false;

   if (m_pMgmtInstance != iter.m_pMgmtInstance)
      return false;

   if (m_pMgmtInstance->getTotalNum() == 0)
      return true;

   return (m_iPriority == iter.m_iPriority) &&
          (m_ListIter == iter.m_ListIter);
}

ReplicaIterator& ReplicaIterator::operator++()
{
   if (!m_pMgmtInstance)
      return *this;

   // Move to next list iterator.
   if (m_ListIter != m_pMgmtInstance->m_MultiJobList[m_iPriority].end())
      m_ListIter ++;

   // When reaching end, start from the beginning of next non-empty list.
   if (m_ListIter == m_pMgmtInstance->m_MultiJobList[m_iPriority].end())
   {
      // find next non-empty job list.
      for (int i = m_iPriority + 1; i < MAX_PRIORITY; ++ i)
      {
         if (!m_pMgmtInstance->m_MultiJobList[i].empty())
         {
            m_iPriority = i;
            m_ListIter = m_pMgmtInstance->m_MultiJobList[m_iPriority].begin();
            return *this;
         }
      }
   }
   else
   {
      return *this;
   }

   // Return end() is no more items available.
   // We will not move the pointer further.
   m_iPriority = MAX_PRIORITY - 1;
   m_ListIter = m_pMgmtInstance->m_MultiJobList[m_iPriority].end();
   return *this;
}

ReplicaJob& ReplicaIterator::operator*()
{
   return *m_ListIter;
}

ReplicaJob* ReplicaIterator::operator->()
{
   return &*m_ListIter;
}


ReplicaMgmt::ReplicaMgmt():
m_llTotalFileSize(0),
m_iTotalJob(0)
{
   m_MultiJobList.resize(MAX_PRIORITY);
}

ReplicaMgmt::~ReplicaMgmt()
{
}

int ReplicaMgmt::insert(const ReplicaJob& rep)
{
   assert(rep.m_iPriority < MAX_PRIORITY);

   m_MultiJobList[rep.m_iPriority].push_back(rep);
   m_llTotalFileSize += rep.m_llSize;
   m_iTotalJob ++;
   return 0;
}

int ReplicaMgmt::erase(const ReplicaMgmt::iterator& iter)
{
   if (iter.m_pMgmtInstance != this)
      return -1;

   if (iter.m_ListIter == m_MultiJobList[iter.m_iPriority].end())
      return -1;

   m_iTotalJob --;
   m_llTotalFileSize += iter.m_ListIter->m_llSize;
   m_MultiJobList[iter.m_iPriority].erase(iter.m_ListIter);
   return 0;
}

int ReplicaMgmt::getTotalNum() const
{
   return m_iTotalJob;
}

int64_t ReplicaMgmt::getTotalSize() const
{
   return m_llTotalFileSize;
}

ReplicaMgmt::iterator ReplicaMgmt::begin()
{
   // This is the first value in the system.
   // If empty, return begin() of the last list, which equals to the end().
   iterator iter;
   int pri = 0;
   for (; pri < MAX_PRIORITY - 1; ++ pri)
   {
      if (!m_MultiJobList[pri].empty())
         break;
   }
   iter.m_pMgmtInstance = this;
   iter.m_iPriority = pri;
   iter.m_ListIter = m_MultiJobList[pri].begin();
   return iter;
}

ReplicaMgmt::ReplicaMgmt::iterator ReplicaMgmt::end()
{
   iterator iter;
   iter.m_pMgmtInstance = this;
   iter.m_iPriority = MAX_PRIORITY - 1;
   iter.m_ListIter = m_MultiJobList[iter.m_iPriority].end();
   return iter;
}
