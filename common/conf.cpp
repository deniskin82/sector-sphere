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
   Yunhong Gu, last updated 03/27/2011
*****************************************************************************/

#ifndef WIN32
   #include <sys/socket.h>
   #include <arpa/inet.h>
   #include <unistd.h>
#endif
#include <osportable.h>
#include <string>
#include <cstring>
#include <cstdlib>
#include <sys/types.h>
#include <sys/stat.h>
#include <sector.h>
#include <conf.h>
#include <iostream>

using namespace std;


int ConfLocation::locate(string& loc)
{
   // search for configuration files from 1) $SECTOR_HOME, 2) ../, or 3) /opt/sector

   loc = "";

   char* system_env = getenv("SECTOR_HOME");
   if (NULL != system_env)
      loc = system_env;

   SNode s;
   if (LocalFS::stat(loc + "/conf", s) == 0)
      return 0;

   if (LocalFS::stat("../conf", s) == 0)
   {
      loc = "../";
   }
   else if (LocalFS::stat("/opt/sector/conf", s) == 0)
   {
      loc = "/opt/sector";
   }
   else
   {
      cerr << "cannot locate Sector configurations from either $SECTOR_HOME, ../, or /opt/sector.";
      return -1;
   }

   return 0;   
}

int ConfParser::init(const string& path)
{
   m_ConfFile.open(path.c_str(), ios::in);

   if (m_ConfFile.bad() || m_ConfFile.fail())
   {
      cerr << "unable to locate or open the configuration file: " << path << endl;
      return -1;
   }

   while (!m_ConfFile.eof())
   {
      string buf = "";
      getline(m_ConfFile, buf);

      if ('\0' == buf.c_str()[0])
         continue;

      //skip comments
      if ('#' == buf.c_str()[0])
         continue;

      //TODO: skip lines with all blanks and tabs
      std::string::size_type pos = buf.find_last_not_of( ' ' );
      if( pos != std::string::npos )
      {
          buf.resize( pos + 1 );
          m_vstrLines.insert(m_vstrLines.end(), buf);
      }
   }

   m_ConfFile.close();

   m_ptrLine = m_vstrLines.begin();
   m_iLineCount = 1;

   return 0;
}

void ConfParser::close()
{
}

int ConfParser::getNextParam(Param& param)
{
   //param format
   // NAME
   // < tab >value1
   // < tab >value2
   // < tab >...
   // < tab >valuen

   if (m_ptrLine == m_vstrLines.end())
      return -1;

   param.m_strName = "";
   param.m_vstrValue.clear();

   while (m_ptrLine != m_vstrLines.end())
   {
      char buf[1024];
      strncpy(buf, m_ptrLine->c_str(), 1024);

      // no blanks or tabs in front of name line
      if ((' ' == buf[0]) || ('\t' == buf[0]))
      {
         cerr << "Configuration file parsing error at line " << m_iLineCount << ": " << buf << endl;
         return -1;
      }

      char* str = buf;
      string token = "";

      if (NULL == (str = getToken(str, token)))
      {
         m_ptrLine ++;
         m_iLineCount ++;
         continue;
      }
      param.m_strName = token;

      // scan param values
      m_ptrLine ++;
      m_iLineCount ++;
      while (m_ptrLine != m_vstrLines.end())
      {
         strncpy(buf, m_ptrLine->c_str(), 1024);

         if (('\0' == *buf) || ('\t' != *buf && ' ' != *buf))
            break;

         str = buf;
         if (NULL == (str = getToken(str, token)))
         {
            //TODO: line count is incorrect, doesn't include # and blank lines
            cerr << "Configuration file parsing error at line " << m_iLineCount << ": " << buf << endl;
            return -1;
         }

         param.m_vstrValue.insert(param.m_vstrValue.end(), token);

         m_ptrLine ++;
         m_iLineCount ++;
      }

      return param.m_vstrValue.size();
   }

   return -1;
}

char* ConfParser::getToken(char* str, string& token)
{
   // remove blanks spaces at the end
   for (int i = strlen(str); i >= 0; -- i)
   {
      if ((str[i] == ' ') || (str[i] == '\t'))
         str[i] = '\0';
      else
         break;
   }

   char* p = str;

   // skip blank spaces
   while ((' ' == *p) || ('\t' == *p))
      ++ p;

   // nothing here...
   if ('\0' == *p)
      return NULL;

   token = p;
   return p + strlen(p);

   // The code below is not used for now.

   token = "";
   while ((' ' != *p) && ('\t' != *p) && ('\0' != *p))
   {
      token.append(1, *p);
      ++ p;
   }

   return p;
}

bool WildCard::isWildCard(const string& path)
{
   if (path.find('*') != string::npos)
      return true;

   if (path.find('?') != string::npos)
      return true;

   return false;
}

bool WildCard::match(const string& card, const string& path)
{
   // this wildcard match code is a modified version based on the one from this website:
   // http://xoomer.virgilio.it/acantato/dev/wildcard/wildmatch.html

   char* pat = (char*)card.c_str();
   char* str = (char*)path.c_str();
   char* p;
   char* s;
   bool star = false;

LoopStart:
   for (s = str, p = pat; *s; ++s, ++p) 
   {
      switch (*p) 
      {
      case '?':
         break;

      case '*':
         star = true;
         str = s;
         pat = p;
         do {++ pat;} while (*pat == '*');
         if (!*pat) return true;
         goto LoopStart;

      default:
         if (*s != *p)
            goto StarCheck;
      }
   } 

   while (*p == '*') ++p;
   return (!*p);

StarCheck:
   if (!star) return false;
   ++ str;
   goto LoopStart;
}

bool WildCard::contain(const string& card, const string& path)
{
   unsigned int lc = card.length();
   unsigned int lp = path.length();

   if (lc > lp)
      return false;

   const char* p = card.c_str();
   const char* q = path.c_str();
   unsigned int i = 0;
   unsigned int j = 0;

   while ((i < lc) && (j < lp))
   {
      switch (p[i])
      {
      case '*':
         while (p[i] == '*')
            ++ i;

         if (i >= lc)
            return true;

         while ((j < lp) && (q[j] != p[i]))
            ++ j;

         if (j >= lp)
            return false;

         break;

      case '?':
         break;

      default:
         if (p[i] != q[j])
            return false;
      }

      ++ i;
      ++ j;
   }

   if (i != lc)
      return false;

   if ((j != lp) && (q[j] != '/'))
      return false;

   return true;
}

int CmdLineParser::parse(int argc, char** argv)
{
   m_vSFlags.clear();
   m_mDFlags.clear();
   m_vParams.clear();

   bool dash = false;
   string key;
   for (int i = 1; i < argc; ++ i)
   {
      if (argv[i][0] == '-')
      {
         if ((strlen(argv[i]) >= 2) && (argv[i][1] == '-'))
         {
            // --f
            m_vSFlags.push_back(argv[i] + 2);
            dash = false;
         }
         else
         {
            // -f [val]
            dash = true;
            key = argv[i] + 1;
            m_mDFlags[key] = "";
         }
      }
      else
      {
         if (!dash)
         {
            // param
            m_vParams.push_back(argv[i]);
         }
         else
         {
            // -f val
            m_mDFlags[key] = argv[i];
            dash = false;
         }
      }
   }

   return 0;
}
