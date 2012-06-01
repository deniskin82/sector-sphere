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
   Yunhong Gu, last updated 05/19/2010
*****************************************************************************/

#include <message.h>

namespace
{

struct util
{
   encode(Address, ...);
 
};

}

#define MSG_CP_T 111

struct Msg_cp: public SectorMsg
{
   MsgCp(): m_iType(MSG_CP) {}

   Address xxx
   string filename
};


#endif
