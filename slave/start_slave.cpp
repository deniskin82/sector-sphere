#include <slave.h>
#include <iostream>

using namespace std;

void help()
{
   cout << "start_slave [base] [-mh master_host_ip] [-mp master_port] [-h local_storage_path] [-ds max_data_size] [-log log_level]" << endl;
}

int main(int argc, char** argv)
{
   cout << SectorVersionString << endl;

   sector::SlaveConf global_conf;

   CmdLineParser clp;
   clp.parse(argc, argv);

   for (map<string, string>::const_iterator i = clp.m_mDFlags.begin(); i != clp.m_mDFlags.end(); ++ i)
   {
      if (i->first == "mh")
         global_conf.m_strMasterHost = i->second;
      else if (i->first == "mp")
         global_conf.m_iMasterPort = atoi(i->second.c_str());
      else if (i->first == "h")
      {
         global_conf.m_strHomeDir = i->second;
      }
      else if (i->first == "ds")
         global_conf.m_llMaxDataSize = atoll(i->second.c_str()) * 1024 * 1024;
      else if (i->first == "log")
         global_conf.m_iLogLevel = atoi(i->second.c_str());
      else
      {
         cout << "warning: unrecognized flag " << i->first << endl;
         help();
      }
   }

   for (vector<string>::const_iterator i = clp.m_vSFlags.begin(); i != clp.m_vSFlags.end(); ++ i)
   {
      if (*i == "v")
         global_conf.m_bVerbose = true;
      else
      {
         cout << "warning: unrecognized flag " << *i << endl;
         help();
      }
   }

   string base = "";
   if (clp.m_vParams.size() == 1)
      base = clp.m_vParams.front();
   else if (clp.m_vParams.size() > 1)
      cout << "warning: wrong parameters ignored.\n";

   // TODO: move slave.conf parsing here, remove conf parsing from inside class Slave.


   sector::Slave s;

   if (s.init(&base, &global_conf) < 0)
   {
      cout << "error: failed to initialize the slave. check slave configurations.\n";
      return-1;
   }

   if (s.connect() < 0)
   {
      cout << "error: failed to connect to the master, or the connection request is rejected.\n";
      return -1;
   }

   s.run();

   s.close();

   return 0;
}
