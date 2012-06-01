#include <iostream>
#include <client.h>

using namespace std;

void help()
{
   cout << "USAGE: send_dbg_cmd -i <slave id> | -d <addr ip:port> -c <cmd code>\n";
   cout << "code 9901: simulate slave disk full\n";
   cout << "code 9902: simulate slave network down\n";
}

int main(int argc, char** argv)
{
   CmdLineParser clp;
   if (clp.parse(argc, argv) <= 0)
   {
      help();
      return -1;
   }

   int32_t id = 0;
   string addr;
   int32_t code = 0;
   for (map<string, string>::iterator i = clp.m_mDFlags.begin(); i != clp.m_mDFlags.end(); ++ i)
   {
      if (i->first == "i")
         id = atoi(i->second.c_str());
      else if (i->first == "d")
         addr = i->second;
      else if (i->first == "c")
         code = atoi(i->second.c_str());
      else
      {
         help();
         return -1;
      }
   }

   if (((id == 0) && (addr.c_str()[0] == 0))|| (code == 0))
   {
      help();
      return -1;
   }

   Session s;
   s.loadInfo("../conf/client.conf");

   Client c;
   if (c.init(s.m_ClientConf.m_strMasterIP, s.m_ClientConf.m_iMasterPort) < 0)
      return -1;

   string passwd = s.m_ClientConf.m_strPassword;
   if (s.m_ClientConf.m_strUserName != "root")
   {
      cout << "please input root password:";
      cin >> passwd;
   }

   if (c.login("root", passwd, s.m_ClientConf.m_strCertificate.c_str()) < 0)
      return -1;

   int r = 0;

   if (id != 0) 
      r = c.sendDebugCode(id, code);
   else
      r = c.sendDebugCode(addr, code);

   if (r < 0)
      cout << "ERROR: " << r << " " << SectorError::getErrorMsg(r) << endl;

   c.logout();
   c.close();

   return r;
}
