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

#ifndef __SPHERE_PROGRAM_ROBOT_H__
#define __SPHERE_PROGRAM_ROBOT_H__

#include <string>

class PRobot
{
public:
   PRobot();

public:
   void setCmd(const std::string& name);
   void setParam(const std::string& param);
   void setCmdFlag(const bool& local);
   void setOutput(const std::string& output);

public:
   int generate();
   int compile();

private:
   std::string m_strSrc;
   std::string m_strCmd;
   std::string m_strParam;
   bool m_bLocal;
   std::string m_strOutput;
};

#endif
