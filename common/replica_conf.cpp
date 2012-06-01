#include <cstdlib>
#include <sstream>
#include <iostream>
#include <iomanip>
#include "conf.h"
#include "meta.h"
#include "osportable.h"
#include "replica_conf.h"
#include "sector.h"

using namespace std;

namespace 
{
   std::string     pathToConfigFile( "/opt/sector/conf/replica.conf" );
   int64_t         configFileTimestamp = 0;
   ReplicaConfData configData;

    int parseItem(const string& input, string& path, int& val)
    {
       val = -1;

       //format: path num
       stringstream ssinput(input);
       ssinput >> path >> val;
       return val;
    }


    int parseItem(const string& input, string& path, int& val1, int& val2 )
    {
       val1 = val2 = -1;

       //format: path num
       stringstream ssinput(input);
       ssinput >> path >> val1;
       val2 = val1;
       ssinput >> val2;
       return val1;
    }


    void parseItem(const string& input, string& path, string& val)
    {
       //format: path val
       stringstream ssinput(input);
       ssinput >> path >> val;
    }
}


void ReplicaConfig::setPath( const std::string& path )
{
   pathToConfigFile = path;
}


const ReplicaConfData& ReplicaConfig::getCached()
{
   return configData;
}


// Returns:
//   True - configuration file was read
//   False - configuration file was not read, either because an error occurred, or because the config file was previously
//           read and has not changed since then.
bool ReplicaConfig::readConfigFile()
{
   SNode s;
   if( LocalFS::stat(pathToConfigFile, s) < 0 || s.m_llTimeStamp == configFileTimestamp )
      return false;

   configFileTimestamp = s.m_llTimeStamp;

   ConfParser parser;
   Param param;

   if (0 != parser.init(pathToConfigFile))
      return false;

   while (parser.getNextParam(param) >= 0)
   {
      if ("REPLICATION_NUMBER" == param.m_strName)
      {
         for (vector<string>::iterator i = param.m_vstrValue.begin(); i != param.m_vstrValue.end(); ++ i)
         {
            string path;
            int num1, num2;
            if (parseItem(*i, path, num1, num2) >= 0)
            {
               string rp = Metadata::revisePath(path);
               if (rp.length() > 0)
               {                  
                  configData.m_mReplicaNum[rp] = pair<int,int>(num1,num2);
               }
            }
         }
      }
      else if ("REPLICATION_DISTANCE" == param.m_strName)
      {
         for (vector<string>::iterator i = param.m_vstrValue.begin(); i != param.m_vstrValue.end(); ++ i)
         {
            string path;
            int dist;
            if (parseItem(*i, path, dist) >= 0)
            {
               string rp = Metadata::revisePath(path);
               if (rp.length() > 0)
                  configData.m_mReplicaDist[rp] = dist;
            }
         }
      }
      else if ("REPLICATION_LOCATION" == param.m_strName)
      {
         for (vector<string>::iterator i = param.m_vstrValue.begin(); i != param.m_vstrValue.end(); ++ i)
         {
            string path;
            string loc;
            parseItem(*i, path, loc);
            string rp = Metadata::revisePath(path);
            vector<int> topo;
            Topology::parseTopo(loc.c_str(), topo);
            if ((rp.length() > 0) && !topo.empty())
               configData.m_mRestrictedLoc[rp] = topo;
         }
      }
      else if ("REPLICATION_MAX_TRANS" == param.m_strName)
      {
         if( !param.m_vstrValue.empty() )
             configData.m_iReplicationMaxTrans = atoi(param.m_vstrValue[0].c_str());
         else
             cerr << "no value specified for REPLICATION_MAX_TRANS" << endl;
      }
      else if ("CHECK_REPLICA_ON_SAME_IP" == param.m_strName)
      {
         if( !param.m_vstrValue.empty() )
             configData.m_bCheckReplicaOnSameIp = (param.m_vstrValue[0] == "TRUE" );
         else
             cerr << "no value specified for CHECK_REPLICA_ON_SAME_IP" << endl;
      }
      else if ("PCT_SLAVES_TO_CONSIDER" == param.m_strName)
      {
         if( !param.m_vstrValue.empty() )
             configData.m_iPctSlavesToConsider = atoi(param.m_vstrValue[0].c_str());
         else
             cerr << "no value specified for PCT_SLAVES_TO_CONSIDER" << endl;
      }
      else if ("REPLICATION_START_DELAY" == param.m_strName)
      {
         if( !param.m_vstrValue.empty() )
             configData.m_iReplicationStartDelay = atoi(param.m_vstrValue[0].c_str());
         else
             cerr << "no value specified for REPLICATION_START_DELAY" << endl;
      }
      else if ("REPLICATION_FULL_SCAN_DELAY"  == param.m_strName)
      {
         if( !param.m_vstrValue.empty() )
             configData.m_iReplicationFullScanDelay = atoi(param.m_vstrValue[0].c_str());
         else
             cerr << "no value specified for REPLICATION_FULL_SCAN_DELAY" << endl;
         if (configData.m_iReplicationFullScanDelay < 60)
            configData.m_iReplicationFullScanDelay = 60;
      }
      else if ("DISK_BALANCE_AGGRESSIVENESS"  == param.m_strName)
      {
         if( !param.m_vstrValue.empty() )
             configData.m_iDiskBalanceAggressiveness = atoi(param.m_vstrValue[0].c_str());
         else
             cerr << "no value specified for DISK_BALANCE_AGGRESSIVENESS" << endl;
         configData.m_iDiskBalanceAggressiveness = std::max( 0, configData.m_iDiskBalanceAggressiveness );
         configData.m_iDiskBalanceAggressiveness = std::min( 100, configData.m_iDiskBalanceAggressiveness );
      }
      else if ("REPLICATE_ON_TRANSACTION_CLOSE"  == param.m_strName)
      {
         if( !param.m_vstrValue.empty() )
             configData.m_bReplicateOnTransactionClose = (param.m_vstrValue[0] == "TRUE");
         else
             cerr << "no value specified for REPLICATE_ON_TRANSACTION_CLOSE" << endl;
      }
      else if ("CHECK_REPLICA_CLUSTER"  == param.m_strName)
      {
         if( !param.m_vstrValue.empty() )
             configData.m_bCheckReplicaCluster = (param.m_vstrValue[0] == "TRUE");
         else
             cerr << "no value specified for CHECK_REPLCIA_CLUSTER" << endl;
      }
      else
      {
         cerr << "unrecognized replica.conf parameter: " << param.m_strName << endl;
      }
   }

   parser.close();
   return true;
}


ReplicaConfData::ReplicaConfData() :
   m_iReplicationStartDelay(10*60),        // 10 min
   m_iReplicationFullScanDelay(10*60),     // 10 min
   m_iReplicationMaxTrans(),               // 0 - no of slaves
   m_iDiskBalanceAggressiveness(25),       // percent
   m_bReplicateOnTransactionClose(),
   m_bCheckReplicaOnSameIp(),
   m_iPctSlavesToConsider(50),
   m_bCheckReplicaCluster()
{
}


std::string ReplicaConfData::toString() const
{
   std::stringstream buf;

   buf << "Replication configuration:\n";
   buf << "REPLICATION_MAX_TRANS " << m_iReplicationMaxTrans << std::endl;
   buf << "REPLICATION_START_DELAY " << m_iReplicationStartDelay << std::endl;
   buf << "REPLICATION_FULL_SCAN_DELAY " << m_iReplicationFullScanDelay << std::endl;
   buf << "DISK_BALANCE_AGGRESSIVENESS " << m_iDiskBalanceAggressiveness << std::endl;
   buf << "REPLICATE_ON_TRANSACTION_CLOSE " <<  m_bReplicateOnTransactionClose << std::endl;
   buf << "CHECK_REPLICA_ON_SAME_IP " << m_bCheckReplicaOnSameIp << std::endl;
   buf << "PCT_SLAVES_TO_CONSIDER " << m_iPctSlavesToConsider << std::endl;
   buf << "CHECK_REPLICA_CLUSTER " << m_bCheckReplicaCluster << std::endl;
   buf << "Number of replicas:\n"; 
   for( std::map<std::string, pair<int,int> >::const_iterator i = m_mReplicaNum.begin(); i != m_mReplicaNum.end(); ++i )
      buf << i->first << " => " << i->second.first << " " << i->second.second << '\n';

   buf << "Replication distance:\n";
   for( std::map<std::string, int>::const_iterator i = m_mReplicaDist.begin(); i != m_mReplicaDist.end(); ++i )
      buf << i->first << " => " << i->second << '\n';

   buf << "Restricted locations:\n";
   for( std::map<std::string, std::vector<int> >::const_iterator i = m_mRestrictedLoc.begin(); i != m_mRestrictedLoc.end(); ++i )
   {
      buf << i->first << " => [";
      for( size_t j = 0; j < i->second.size(); ++j )
        buf << ' ' << i->second[j];
      buf << "]\n";
   }

   buf << "End of replication configuration\n";
   return buf.str();
}


int ReplicaConfData::getReplicaNum(const std::string& path, int default_val) const
{
   for (map<string, std::pair<int,int> >::const_iterator i = m_mReplicaNum.begin(); i != m_mReplicaNum.end(); ++ i)
      if (WildCard::contain(i->first, path))
         return i->second.first;

   return default_val;
}

int ReplicaConfData::getMaxReplicaNum(const std::string& path, int default_val) const
{
   for (map<string, std::pair<int,int> >::const_iterator i = m_mReplicaNum.begin(); i != m_mReplicaNum.end(); ++ i)
      if (WildCard::contain(i->first, path))
         return i->second.second;

   return default_val;
}

int ReplicaConfData::getReplicaDist(const std::string& path, int default_val) const
{
   for (map<string, int>::const_iterator i = m_mReplicaDist.begin(); i != m_mReplicaDist.end(); ++ i)
      if (WildCard::contain(i->first, path))
         return i->second;
 
   return default_val;
}


void ReplicaConfData::getRestrictedLoc(const std::string& path, vector<int>& loc) const
{
   loc.clear();

   for (map<string, vector<int> >::const_iterator i = m_mRestrictedLoc.begin(); i != m_mRestrictedLoc.end(); ++ i)
      if (WildCard::contain(i->first, path))
         loc = i->second;
}

