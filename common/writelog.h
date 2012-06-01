/*****************************************************************************
Copyright 2010 The Board of Trustees of the University of Illinois. 

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
   Yunhong Gu [gu@lac.uic.edu], last updated 07/25/2010
*****************************************************************************/

#ifndef __SECTOR_WRITE_LOG__
#define __SECTOR_WRITE_LOG__

#include <udt.h>
#include <vector>

struct WriteEntry
{
    int64_t m_llID;		// A unique ID to identify each entry, as offset and size can be the same
    int64_t m_llOffset;
    int64_t m_llSize;
};

class WriteLog
{
public:
    WriteLog();

public:
    int insert(const int64_t& offset, const int64_t& size, const int64_t& id = 0);
    void clear();

    int serialize(char*& buf, int& size);
    int deserialize(const char* buf, int size);

    bool compare(const WriteLog& log);

    int64_t getCurrTotalSize();

public:
    std::vector<WriteEntry> m_vListOfWrites;

    int64_t m_llTotalSize;
};

#endif
