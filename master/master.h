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
   Yunhong Gu, last updated 02/08/2011
*****************************************************************************/

#ifndef __SECTOR_MASTER_H__
#define __SECTOR_MASTER_H__

#include <vector>

#include "gmp.h"
#include "index.h"
#include "log.h"
#include "osportable.h"
#include "replica.h"
#include "routing.h"
#include "sector.h"
#include "slavemgmt.h"
#include "threadpool.h"
#include "topology.h"
#include "transaction.h"
#include "user.h"

class Topology;

namespace sector
{

class SSLTransport;

class MasterConf
{
public:
   MasterConf();
   int init(const std::string& path);
   std::string toString();

public:
   int m_iServerPort;                   // server port
   std::string m_strSecServIP;          // security server IP
   int m_iSecServPort;                  // security server port
   int m_iMaxActiveUser;                // maximum active user
   std::string m_strHomeDir;            // data directory
   int m_iReplicaNum;                   // min number of replicas of each file
   int m_iMaxReplicaNum;                // max number of replicas of each file
   int m_iReplicaDist;			// replication distance
   MetaForm m_MetaType;                 // form of metadata
   int m_iSlaveTimeOut;                 // slave timeout threshold
   int m_iSlaveRetryTime;               // time to reload a lost slave
   int64_t m_llSlaveMinDiskSpace;       // minimum available disk space allowed on each slave
   int m_iClientTimeOut;                // client timeout threshold
   int m_iLogLevel;                     // level of logs, higher = more verbose, 0 = no log
   int m_iProcessThreads;               // Number of processing threads.
   std::vector<std::string> m_vWriteOncePath; // WriteOnce protected pathes
};


class SlaveStartInfo
{
friend class Master;

public:
   std::string m_strIP;			// IP address, numerical
   std::string m_strAddr;		// username@hostname/ip
   std::string m_strBase;		// $SECTOR_HOME location on the slave
   std::string m_strStoragePath;	// local storage path
   std::string m_strOption;		// slave options

private:
   static bool skip(const char* line);
   static int parse(char* line, std::string& addr, std::string& base, std::string& param);
   static int parse(char* line, std::string& key, std::string& val);
   static std::string getIP(const char* name);
};

struct SSIComp
{
   // Slave is differentiated by IP, storage path, and base
   bool operator()(const SlaveStartInfo& s1, const SlaveStartInfo& s2)
   {
      int c1 = strcmp(s1.m_strIP.c_str(), s2.m_strIP.c_str());
      if (c1 == 0)
      {
         int c2 = 0;
         if (!s1.m_strStoragePath.empty() && !s2.m_strStoragePath.empty())
            c2 = strcmp(s1.m_strStoragePath.c_str(), s2.m_strStoragePath.c_str());
         if (c2 == 0)
         {
            return (strcmp(s1.m_strBase.c_str(), s2.m_strBase.c_str()) > 0);
         }

         return c2 > 0;
      }
      
      return c1 > 0;
   }
};

class Master
{
public:
   Master();
   ~Master();

public:
   int init();
   int join(const char* ip, const int& port);
   int run();
   int stop();

private:
#ifndef WIN32
   static void* utility(void* s);
#else
   static DWORD WINAPI utility(void* s);
#endif

   ThreadJobQueue m_ServiceJobQueue;			// job queue for service thread pool
   struct ServiceJobParam
   {
      std::string ip;
      int port;
      SSLTransport* ssl;
   };
#ifndef WIN32
   static void* service(void* s);
   static void* serviceEx(void* p);
#else
   static DWORD WINAPI service(void* s);
   static DWORD WINAPI serviceEx(void* p);
#endif
   int processSlaveJoin(SSLTransport& s, SSLTransport& secconn, const std::string& ip);
   int processUserJoin(SSLTransport& s, SSLTransport& secconn, const std::string& ip);
   int processMasterJoin(SSLTransport& s, SSLTransport& secconn, const std::string& ip);

   ThreadJobQueue m_ProcessJobQueue;
   struct ProcessJobParam
   {
      std::string ip;
      int port;
      User* user;
      int key;
      int id;
      SectorMsg* msg;
   };
#ifndef WIN32
   static void* process(void* s);
   static void* processEx(void* p);
#else
   static DWORD WINAPI process(void* s);
   static DWORD WINAPI processEx(void* p);
#endif
   int processSysCmd(const std::string& ip, const int port,  const User* user, const int32_t key, int id, SectorMsg* msg);
   int processFSCmd(const std::string& ip, const int port,  const User* user, const int32_t key, int id, SectorMsg* msg);
   int processDCCmd(const std::string& ip, const int port,  const User* user, const int32_t key, int id, SectorMsg* msg);
   int processDBCmd(const std::string& ip, const int port,  const User* user, const int32_t key, int id, SectorMsg* msg);
   int processMCmd(const std::string& ip, const int port,  const User* user, const int32_t key, int id, SectorMsg* msg);
   int sync(const char* fileinfo, const int& size, const int& type);
   int processSyncCmd(const std::string& ip, const int port,  const User* user, const int32_t key, int id, SectorMsg* msg);

private:
   int removeSlave(const int& id, const Address& addr);

private:
   inline void reject(const std::string& ip, const int port, int id, int32_t code);
   inline void logUserActivity(const User* user, const char* cmd, const char* file, const int res, const char* info, const int level);

private: // replication
#ifndef WIN32
   static void* replica(void* s);
#else
   static DWORD WINAPI replica(void* p);
#endif

   CMutex m_ReplicaLock;
   CCond m_ReplicaCond;

   ReplicaMgmt m_ReplicaMgmt;				// list of files to be replicated
   std::set<std::string> m_sstrOnReplicate;		// list of files currently being replicated

   int createReplica(const ReplicaJob& job);
   int removeReplica(const std::string& filename, const Address& addr);

   int processWriteResults(const std::string& filename, std::map<int, std::string> results);

   int chooseDataToMove(std::vector<std::string>& path, const Address& addr, const int64_t& target_size);

private:
   
   CMutex m_DfLock;
   time_t m_iDfTs;
   time_t m_iDfTimeout;
   bool m_bDfBeingEvaluated;
   int64_t m_iDfUsedSpace;
   int64_t m_iDfAvailSpace;

   CGMP m_GMP;						// GMP messenger

   std::string m_strSectorHome;				// $SECTOR_HOME directory, for code and configuration file location
   MasterConf m_SysConfig;				// master configuration
   std::string m_strHomeDir;				// home data directory, for system metadata

   SectorLog m_SectorLog;				// sector log

   Metadata* m_pMetadata;                               // metadata

   int m_iMaxActiveUser;				// maximum number of active users allowed
   UserManager m_UserManager;				// user management

   SlaveManager m_SlaveManager;				// slave management
   std::vector<Address> m_vSlaveList;			// list of slave addresses
   int64_t m_llLastUpdateTime;				// last update time for the slave list;

   TransManager m_TransManager;				// transaction management

   enum Status {INIT, RUNNING, STOPPED} m_Status;	// system status

   char* m_pcTopoData;					// serialized topology data
   int m_iTopoDataSize;					// size of the topology data
   Topology* m_pTopology;				// Slave topology.

   Routing m_Routing;					// master routing module
   uint32_t m_iRouterKey;				// identification for this master

private:
   std::set<SlaveStartInfo, SSIComp> m_sSlaveStartInfo;	// information on how to start a slave

public:
   static int loadSlaveStartInfo(const std::string& file, std::set<SlaveStartInfo, SSIComp>& ssi);

private:
   int64_t m_llStartTime;
   int serializeSysStat(char*& buf, int& size);
   #ifdef DEBUG
   int processDebugCmd(const std::string& ip, const int port,  const User* user, const int32_t key, int id, SectorMsg* msg);
   #endif

public:
   static void startSlave(const std::string& addr, const std::string& base, const std::string& option, const std::string& log = "");
};

} // namespace sector

#endif
