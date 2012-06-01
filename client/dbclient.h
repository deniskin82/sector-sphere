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


#ifndef __SPACE_DB_CLIENT_H__
#define __SPACE_DB_CLIENT_H__

#include "client.h"
#include <string>

//TABLE: key = string
//TBALE: column = char*

class Cursor
{
};

class DBClient
{
friend class Client;

private:
   DBClient();
   ~DBClient();
   const DBClient& operator=(const DBClient&) {return *this;}

public:
   int open(const std::string& name);
   void close();

   int addAttribute(const std::string& column);
   int deleteAttribute(const std::string& column);

   int update(const std::string& key, const std::string& attr, const char* val, const int& size);
   int delete(const std::string& key, const std::string& attr);
   int delete(const std::string& key);
   int delete(const std::string& key1, const std::string& key2);

   int commit();

   int lookup(const std::string& key, const std::string& attr, char* val, int& size);
   int lookup(const std::string& key, const std::vector<std::string>& attrs, std::vector<char*>& vals, std::vector<int>& sizes);
   int select(const std::set<string> columns, FUNC* cond = NULL);

private:
   std::string m_strName;
   Column m_Key;
   std::map<std::string, Column> m_sAttributes;

   int64_t m_iRows;

   std::map<std::string, std::string> m_mDataLoc;

private:
   pthread_mutex_t m_TableLock;

private:
   Client* m_pClient;           // client instance
   int m_iID;                   // sector file id, for client internal use
};

#endif
