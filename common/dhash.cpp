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


#include <string.h>
#include "dhash.h"

DHash::DHash():
m_im(32)
{
}

DHash::DHash(const int m):
m_im(m)
{
}

DHash::~DHash()
{
}

unsigned int DHash::hash(const char* str)
{
   unsigned char res[SHA_DIGEST_LENGTH];

   SHA1((const unsigned char*)str, strlen(str), res);

   return *(unsigned int*)(res + SHA_DIGEST_LENGTH - 4);
}

unsigned int DHash::hash(const char* str, int m)
{
   unsigned char res[SHA_DIGEST_LENGTH];

   SHA1((const unsigned char*)str, strlen(str), res);

   if (m >= 32)
      return *(unsigned int*)(res + SHA_DIGEST_LENGTH - 4);

   unsigned int mask = 1;
   mask = (mask << m) - 1;

   return (*(unsigned int*)(res + SHA_DIGEST_LENGTH - 4)) & mask;
}
