*****************************************************************************
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
   bdl62, last updated 03/21/2011
*****************************************************************************/

#ifndef __SECTOR_STAT_H__
#define __SECTOR_STAT_H__

// This file define the statistics structure for Sector master, slave, and client.

namespace sector
{

class MasterStat
{
public:
   MasterStat();
   ~MasterStat();

public:
   int64_t m_llTotalReq;
   int64_t m_llTotalReject;
   int64_t m_llTotalError;   

public:
   serialize();
   deserialize()
};

class SlaveStat
{
public:
   SlaveStat();
   ~SlaveStat();

public:
   int64_t m_llStartTime;
   int64_t m_llTimeStamp;

   int64_t m_llDataSize;
   int64_t m_llAvailSize;
   int64_t m_llCurrMemUsed;
   int64_t m_llCurrCPUUsed;

   int64_t m_llTotalInputData;
   int64_t m_llTotalOutputData;
   std::map<std::string, int64_t> m_mSysIndInput;
   std::map<std::string, int64_t> m_mSysIndOutput;
   std::map<std::string, int64_t> m_mCliIndInput;
   std::map<std::string, int64_t> m_mCliIndOutput;

   int64_t m_llTotalReq;
   int64_t m_llTotalReject;
   int64_t m_llTotalError;

public:
   void init();
   void refresh();
   void updateIO(const std::string& ip, const int64_t& size, const int& type);
   int serializeIOStat(char*& buf, int& size);

private:
   CMutex m_StatLock;

public: // io statistics type
   static const int SYS_IN = 1;
   static const int SYS_OUT = 2;
   static const int CLI_IN = 3;
   static const int CLI_OUT = 4;
};

class ClientStat
{
};

}  // namespace sector.

#endif
