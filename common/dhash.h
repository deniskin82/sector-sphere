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

#ifndef __DHASH_H__
#define __DHASH_H__

#include <openssl/sha.h>
#include <math.h>
#include <string>

class DHash
{
public:
   DHash();
   DHash(const int m);
   ~DHash();

   unsigned int hash(const char* str);
   static unsigned int hash(const char* str, int m);

private:
   unsigned int m_im;
};

#endif
