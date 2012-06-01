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


#include <sphere.h>
#include <cstring>

int SOutput::resizeResBuf(const int& newsize)
{
   if (newsize < m_iResSize)
      return -1;

   char* tmp = NULL;

   try
   {
      tmp = new char[newsize];
   }
   catch (...)
   {
      return -1;
   }

   memcpy(tmp, m_pcResult, m_iResSize);
   delete [] m_pcResult;
   m_pcResult = tmp;

   m_iBufSize = newsize;

   return newsize;
}

int SOutput::resizeIdxBuf(const int& newsize)
{
   if (newsize < (m_iRows + 1) * 8)
      return -1;

   int64_t* tmp1 = NULL;
   int* tmp2 = NULL;

   try
   {
      tmp1 = new int64_t[newsize];
      tmp2 = new int[newsize];
   }
   catch (...)
   {
      return -1;
   }

   memcpy(tmp1, m_pllIndex, (m_iRows + 1) * 8);
   delete [] m_pllIndex;
   m_pllIndex = tmp1;

   memcpy(tmp2, m_piBucketID, m_iRows * sizeof(int));
   delete [] m_piBucketID;
   m_piBucketID = tmp2;

   m_iIndSize = newsize;

   return newsize;
}

