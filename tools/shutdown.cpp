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
   Yunhong Gu, last updated 01/12/2010
*****************************************************************************/


#include <iostream>
#include <sector.h>

using namespace std;

void help()
{
   cout << "USAGE: sector_shutdown -a | -i <slave id> | -d <slave IP:port> | -r <rack topo path>\n";
}

int main(int argc, char** argv)
{
   if ((argc != 2) && (argc != 3))
   {
      help();
      return -1;
   }

   Sector client;

   Session s;
   s.loadInfo("../conf/client.conf");

   int result = client.init();
   if (result < 0)  
   {
      Utility::print_error(result);
      return -1;
   }

   string passwd = s.m_ClientConf.m_strPassword;
   if (s.m_ClientConf.m_strUserName != "root")
   {
      cout << "please input root password:";
      cin >> passwd;
   }

   bool master_conn = false;
   for (set<Address, AddrComp>::const_iterator i = s.m_ClientConf.m_sMasterAddr.begin(); i != s.m_ClientConf.m_sMasterAddr.end(); ++ i)
   {
      result = client.login(i->m_strIP, i->m_iPort, "root", passwd, s.m_ClientConf.m_strCertificate.c_str());
      if (result < 0)
      {
         cerr << "trying to connect " << i->m_strIP << " " << i->m_iPort << endl;
         Utility::print_error(result);
      }
      else
      {
         master_conn = true;
         break;
      }
   }

   CmdLineParser clp;
   if (clp.parse(argc, argv) < 0)
   {
      help();
      return -1;
   }

   string type = clp.m_mDFlags.begin()->first;
   string param = clp.m_mDFlags.begin()->second;

   result = -1;
   if (type == "a")
      result = client.shutdown(1);
   else if (type == "i")
      result = client.shutdown(2, param);
   else if (type == "d")
      result = client.shutdown(3, param);
   else if (type == "r")
      result = client.shutdown(4, param);
   else
   {
      help();
   }

   if (result < 0)
      Utility::print_error(result);
   else if (result >= 0)
      cout << "shutdown is successful. If you only shut down part of the system, run sector_sysinfo to check\n";

   client.logout();
   client.close();

   return result;
}
