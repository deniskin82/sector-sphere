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

#include <fstream>
#include <iostream>
#include <stdlib.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <probot.h>

using namespace std;

PRobot::PRobot():
m_strSrc(),
m_strCmd(),
m_strParam(),
m_bLocal(false),
m_strOutput()
{
}

void PRobot::setCmd(const string& name)
{
   m_strSrc = name;
   m_strCmd = name;
}

void PRobot::setParam(const string& param)
{
   m_strParam = "";
   for (char* p = (char*)param.c_str(); *p != '\0'; ++ p)
   {
      if (*p == '"')
         m_strParam.push_back('\\');
      m_strParam.push_back(*p);
   }
}

void PRobot::setCmdFlag(const bool& local)
{
   m_bLocal = local;
}

void PRobot::setOutput(const string& output)
{
   m_strOutput = output;
}

int PRobot::generate()
{
   fstream cpp;
   cpp.open((m_strSrc + ".cpp").c_str(), ios::in | ios::out | ios::trunc);

   cpp << "#include <iostream>" << endl;
   cpp << "#include <fstream>" << endl;
   cpp << "#include <cstring>" << endl;
   cpp << "#include <cstdlib>" << endl;
   cpp << "#include <sphere.h>" << endl;
   cpp << endl;
   cpp << "using namespace std;" << endl;
   cpp << endl;
   cpp << "extern \"C\"" << endl;
   cpp << "{" << endl;
   cpp << endl;

   cpp << "int " << m_strCmd << "(const SInput* input, SOutput* output, SFile* file)" << endl;
   cpp << "{" << endl;
   cpp << "   string ifile = file->m_strHomeDir + input->m_pcUnit;" << endl;
   cpp << "   string ofile = ifile + \".result\";" << endl;
   cpp << endl;

   // Python: .py
   // Perl: .pl

   // system((string("") + FUNC + " " + ifile + " " + PARAM + " > " + ofile).c_str());
   cpp << "   system((string(\"\") + ";
   if (m_bLocal)
      cpp << "file->m_strLibDir + \"/\" + ";
   cpp << "\"";
   cpp << m_strCmd;
   cpp << "\" + ";
   cpp << "\" ";
   cpp << m_strParam;
   cpp << "\" + ";
   cpp << " \" \" + ifile + \" \" + ";
   cpp << " \" > \" + ofile).c_str());" << endl;

   cpp << endl;
   if (m_strOutput.length() == 0)
   {
      cpp << "   ifstream dat(ofile.c_str());" << endl;
      cpp << "   dat.seekg(0, ios::end);" << endl;
      cpp << "   int size = dat.tellg();" << endl;
      cpp << "   dat.seekg(0);" << endl;
      cpp << endl;
      cpp << "   output->m_iRows = 1;" << endl;
      cpp << "   output->m_pllIndex[0] = 0;" << endl;
      cpp << "   output->m_pllIndex[1] = size + 1;" << endl;
      cpp << "   dat.read(output->m_pcResult, size);" << endl;
      cpp << "   output->m_pcResult[size] = '\\0';" << endl;
      cpp << "   dat.close();" << endl;
      cpp << "   unlink(ofile.c_str());" << endl;
   }
   else
   {
      cpp << "   output->m_iRows = 0;" << endl;
      cpp << endl;
      cpp << "   string sfile;" << endl;
      cpp << "   for (int i = 1, n = strlen(input->m_pcUnit); i < n; ++ i)" << endl;
      cpp << "   {" << endl;
      cpp << "      if (input->m_pcUnit[i] == '/')" << endl;
      cpp << "         sfile.push_back('.');" << endl;
      cpp << "      else" << endl;
      cpp << "         sfile.push_back(input->m_pcUnit[i]);" << endl;
      cpp << "   }" << endl;
      cpp << "   sfile = string(" << "\"" << m_strOutput << "\")" << " + \"/\" + sfile;" << endl;
      cpp << endl;
      cpp << "   system((\"mkdir -p \" + file->m_strHomeDir + " << "\"" << m_strOutput << "\"" << ").c_str());" << endl;
      cpp << "   system((\"mv \" + ofile + \" \" + file->m_strHomeDir + sfile).c_str());" << endl;
      cpp << endl;
      cpp << "   file->m_sstrFiles.insert(sfile);" << endl;
   }
   cpp << endl;

   cpp << "   return 0;" << endl;
   cpp << "}" << endl;
   cpp << endl;
   cpp << "}" << endl;

   cpp.close();

   return 0;
}

int PRobot::compile()
{
   string loc;
   char* system_env = getenv("SECTOR_HOME");
   if (NULL != system_env)
      loc = system_env;

   struct stat t;
   if (stat((loc + "/include/sphere.h").c_str(), &t) == 0)
   {
   }
   else if (stat("../include/sphere.h", &t) == 0)
   {
      loc = "../";
   }
   else if (stat("/opt/sector/sphere.h", &t) == 0)
   {
      loc = "/opt/sector";
   }
   else
   {
      cerr << "cannot locate Sector and UDT header files from either $SECTOR_HOME, ../, or /opt/sector.";
      return -1;
   }

   string CCFLAGS = string("-I") + loc + string("/include -I") + loc + string("/udt");
   system(("g++ " + CCFLAGS + " -shared -fPIC -O3 -o " + m_strSrc + ".so -lstdc++ " + m_strSrc + ".cpp").c_str());

   system(("mv " + m_strSrc + ".* /tmp").c_str());

   return 0;
}
