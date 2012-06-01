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
   Yunhong Gu, last updated 04/24/2011
*****************************************************************************/


#ifndef __SECURITY_SOURCE_FILE_H__
#define __SECURITY_SOURCE_FILE_H__

#include <security.h>

namespace sector
{

class FileSrc: public SSource
{
public:
   FileSrc();
   virtual ~FileSrc();

   virtual int init(const void* param);

   virtual bool matchMasterACL(const char* ip);
   virtual bool matchSlaveACL(const char* ip);
   virtual int retrieveUser(const char* name, const char* password, const char* ip, User& user);

   virtual bool isUpdated();
   virtual int refresh();

private:
   bool match(const std::vector<IPRange>& acl, const char* ip);
   int loadACL(std::vector<IPRange>& acl, const std::string& path);
   int loadUsers(std::map<std::string, User>& users, const std::string& path);

private:
   int parseIPRange(IPRange& ipr, const char* ip);
   int parseUser(User& user, const char* name, const char* ufile);

private:
   std::vector<IPRange> m_vMasterACL;
   std::vector<IPRange> m_vSlaveACL;
   std::map<std::string, User> m_mUsers;

   std::string m_strConfLoc;
   int64_t m_llLastUpdateTime;
};

}  // namespace sector

#endif
