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

#include <writelog.h>

using namespace std;

WriteLog::WriteLog():
m_llTotalSize(0)
{
}

int WriteLog::insert(const int64_t& offset, const int64_t& size, const int64_t& id)
{
    WriteEntry entry;
    entry.m_llID = id;
    entry.m_llOffset = offset;
    entry.m_llSize = size;
    m_vListOfWrites.push_back(entry);

    m_llTotalSize += size;

    return 0;
}

void WriteLog::clear()
{
    m_vListOfWrites.clear();
    m_llTotalSize = 0;
}

int WriteLog::serialize(char*& buf, int& size)
{
    if (m_vListOfWrites.empty())
    {
       buf = NULL;
       size = 0;
       return 0;
    }

    size = m_vListOfWrites.size() * sizeof(int64_t) * 2;
    buf =  new char [size];
    int64_t* p = (int64_t*)buf;

    for (vector<WriteEntry>::iterator i = m_vListOfWrites.begin(); i != m_vListOfWrites.end(); ++ i)
    {
        *p++ = i->m_llOffset;
        *p++ = i->m_llSize;
    }

    return 0;
}

int WriteLog::deserialize(const char* buf, int size)
{
    int num = size / sizeof(int64_t) / 2;
    m_vListOfWrites.resize(num);

    int64_t* p = (int64_t*)buf;

    for (vector<WriteEntry>::iterator i = m_vListOfWrites.begin(); i != m_vListOfWrites.end(); ++ i)
    {
        i->m_llOffset = *p++;
        i->m_llSize = *p++;
    }

    return num;
}

bool WriteLog::compare(const WriteLog& log)
{
    if (m_vListOfWrites.size() != log.m_vListOfWrites.size())
        return false;

    for (vector<WriteEntry>::const_iterator i = m_vListOfWrites.begin(), j = log.m_vListOfWrites.begin(); i != m_vListOfWrites.end(); ++ i, ++ j)
    {
        if ((i->m_llOffset != j->m_llOffset) || (i->m_llSize != j->m_llSize))
            return false;
    }

    return true;
}

int64_t WriteLog::getCurrTotalSize()
{
    return m_llTotalSize; 
}
