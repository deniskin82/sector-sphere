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
   Yunhong Gu, last updated 05/18/2010
*****************************************************************************/

#ifndef __SECTOR_MESSAGE_H__
#define __SECTOR_MESSAGE_H__

#ifndef WIN32
   #include <sys/types.h>
#else
   #include <windows.h>
   #include <udt.h>
#endif

struct GMP_API SectorMsg: public CUserMessage
{
public:
   SectorMsg() {m_iDataLength = m_iHdrSize;}
   virtual ~SectorMsg() {}

   int32_t getType() const;
   void setType(const int32_t& type);
   int32_t getKey() const;
   void setKey(const int32_t& key);
   int32_t getSignature() const;
   void setSignature(const int32_t& signature);

   char* getData() const;
   void setData(const int& offset, const char* data, const int& len);

public:
   static const int m_iHdrSize = 12;

public:
   virtual int serialize() { return 0; }
   virtual int deserialize() { return 0; }
};


struct SectorMsg
{
   int m_iType;

   SectorMsg(): m_iType(0) {}
   virtual ~SectorMsg() {}
   int serialize(char* buffer, int& size) = 0;
   int deserialize(const char* buffer, int size) = 0;
};

struct UserLoginRequest: public SectorMsg
{
};

struct UserLoginResponse: public SectorMsg
{
};

struct SlaveJoinRequest: public SectorMsg
{
};

struct SlaveJoinResponse: public SectorMsg
{
};


#define MSG_CP_T 111

struct Msg_cp: public SectorMsg
{
   MsgCp(): m_iType(MSG_CP) {}

   Address xxx
   string filename
};


#endif
