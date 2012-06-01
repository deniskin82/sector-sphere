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
   Yunhong Gu, last updated 10/18/2010
*****************************************************************************/


#include <sector.h>
#include <sstream>

using namespace std;

SF_OPT::SF_OPT():
m_strHintIP(),
m_strCluster(),
m_llReservedSize(0),
m_iReplicaNum(1),
m_iPriority(1)
{
}

void SF_OPT::serialize(string& buf) const
{
   stringstream ss(stringstream::out);

   if (m_strHintIP.c_str()[0] == '\0')
      ss << "NULL ";
   else
      ss << m_strHintIP << " ";

   if (m_strCluster.c_str()[0] == '\0')
      ss << "NULL ";
   else
      ss << m_strCluster << " ";

   ss << m_llReservedSize << " "
      << m_iReplicaNum << " "
      << m_iPriority << endl;

   buf = ss.str();
}

void SF_OPT::deserialize(const string& buf)
{
   stringstream ss(buf, stringstream::in);
   ss >> m_strHintIP
      >> m_strCluster
      >> m_llReservedSize
      >> m_iReplicaNum
      >> m_iPriority;

   if (m_strHintIP == "NULL")
      m_strHintIP = "";
   if (m_strCluster == "NULL")
      m_strCluster = "";
}

