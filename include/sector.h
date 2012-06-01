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
   Yunhong Gu, last updated 05/18/2011
*****************************************************************************/


#ifndef __SECTOR_H__
#define __SECTOR_H__

#include <map>
#include <set>
#include <string>
#include <vector>
#include <deque>
#include <ctime>
#include <udt.h>


#ifndef WIN32
   #include <stdint.h>

   #define SECTOR_API
#else
   #ifdef SECTOR_EXPORTS
      #define SECTOR_API __declspec(dllexport)
   #else
      #define SECTOR_API __declspec(dllimport)
   #endif
   #pragma warning( disable: 4251 )
#endif


// Sector version. The digit version is for compatibility check, so it is the earliest compatible version number
// FORMAT 3-digit Major 3-digit Minor 3-digit revision
const int32_t SectorVersion = 2006031;
const std::string SectorVersionString = "Sector SVN version 869 build Fri Apr 13 10:59:06 CDT 2012";


struct Address
{
   std::string m_strIP;
   unsigned short int m_iPort;
};

struct AddrComp
{
   bool operator()(const Address& a1, const Address& a2) const
   {
      if (a1.m_strIP == a2.m_strIP)
         return a1.m_iPort < a2.m_iPort;
      return a1.m_strIP < a2.m_strIP;
   }
};

class SECTOR_API SNode
{
public:
   SNode();
   ~SNode();

public:
   std::string m_strName;
   bool m_bIsDir;
   std::set<Address, AddrComp> m_sLocation;
   std::map<std::string, SNode> m_mDirectory;
   int64_t m_llTimeStamp;
   int64_t m_llSize;
   std::string m_strChecksum;
   int m_iReplicaNum;
   int m_iMaxReplicaNum;
   int m_iReplicaDist;
   std::vector<int> m_viRestrictedLoc;

public:
   int serialize(char*& buf) const;
   int deserialize(const char* buf);
   int serialize(char*& buf, const bool includeReplica) const;

};

class SECTOR_API SysStat
{
public:
   int64_t m_llStartTime;

   int64_t m_llAvailDiskSpace;
   int64_t m_llTotalFileSize;
   int64_t m_llTotalFileNum;
   int64_t m_llUnderReplicated;

   int64_t m_llTotalSlaves;

   struct MasterStat
   {
      int32_t m_iID;
      std::string m_strIP;
      int32_t m_iPort;
   };
   std::vector<MasterStat> m_vMasterList;

   struct SlaveStat
   {
      int32_t m_iID;
      std::string m_strIP;
      int32_t m_iPort;
      int64_t m_llTimeStamp;
      int64_t m_llAvailDiskSpace;
      int64_t m_llTotalFileSize;
      int64_t m_llCurrMemUsed;
      int64_t m_llCurrCPUUsed;
      int64_t m_llTotalInputData;
      int64_t m_llTotalOutputData;
      std::string m_strDataDir;
      int32_t m_iClusterID;
      int32_t m_iStatus;
   };
   std::vector<SlaveStat> m_vSlaveList;

   struct ClusterStat
   {
      int m_iClusterID;
      int m_iTotalNodes;
      int64_t m_llAvailDiskSpace;
      int64_t m_llTotalFileSize;
      int64_t m_llTotalInputData;
      int64_t m_llTotalOutputData;
   };
   std::vector<ClusterStat> m_vCluster;

public:
   //void print(string&);
};

class SectorFile;
class SphereProcess;

class SECTOR_API Sector
{
public:
   int init();
   int login(const std::string& server, const int& port,
             const std::string& username, const std::string& password, const char* cert = NULL);
   int logout();
   int close();

   int list(const std::string& path, std::vector<SNode>& attr);
   int list(const std::string& path, std::vector<SNode>& attr, const bool includeReplica);
   int stat(const std::string& path, SNode& attr);
   int mkdir(const std::string& path);
   int move(const std::string& oldpath, const std::string& newpath);
   int remove(const std::string& path);
   int rmr(const std::string& path);
   int copy(const std::string& src, const std::string& dst);
   int utime(const std::string& path, const int64_t& ts);

   int sysinfo(SysStat& sys);
   int debuginfo(std::string& dbg);
   int df(int64_t& availableSize, int64_t& totalSize);
   int shutdown(const int& type, const std::string& param = "");
   int fsck(const std::string& path);

public: // file and data processing handlers
   SectorFile* createSectorFile();
   SphereProcess* createSphereProcess();
   int releaseSectorFile(SectorFile* sf);
   int releaseSphereProcess(SphereProcess* sp);

public: //config
   int configLog(const char* log_path, bool screen, int level);
   int setMaxCacheSize(const int64_t& ms);

public:
   int m_iID;
};

// file open mode
struct SECTOR_API SF_MODE
{
   static const int READ = 1;                   // read only
   static const int WRITE = 2;                  // write only
   static const int RW = 3;                     // read and write
   static const int TRUNC = 4;                  // trunc the file upon opening
   static const int APPEND = 8;                 // move the write offset to the end of the file upon opening
   static const int SECURE = 16;                // encrypted file transfer
   static const int HiRELIABLE = 32;            // replicate data writting at real time (otherwise periodically)
};

//file IO position base
struct SECTOR_API SF_POS
{
   static const int BEG = 1;
   static const int CUR = 2;
   static const int END = 3;
};

//extra file open options
struct SECTOR_API SF_OPT
{
   SF_OPT();

   std::string m_strHintIP;
   std::string m_strCluster;
   int64_t m_llReservedSize;
   int m_iReplicaNum;
   int m_iPriority;

   void serialize(std::string& buf) const;
   void deserialize(const std::string& buf);
};

class SECTOR_API SectorFile
{
friend class Sector;

private:
   SectorFile() {}
   ~SectorFile() {}
   const SectorFile& operator=(const SectorFile&) {return *this;}

public:
   int open(const std::string& filename, int mode = SF_MODE::READ, const SF_OPT* option = NULL);
   int64_t read(char* buf, const int64_t& offset, const int64_t& size, const int64_t& prefetch = 0);
   int64_t write(const char* buf, const int64_t& offset, const int64_t& size, const int64_t& buffer = 0);
   int64_t read(char* buf, const int64_t& size);
   int64_t write(const char* buf, const int64_t& size);
   int64_t download(const char* localpath, const bool& cont = false);
   int64_t upload(const char* localpath, const bool& cont = false);
   int flush();
   int close();

   int64_t seekp(int64_t off, int pos = SF_POS::BEG);
   int64_t seekg(int64_t off, int pos = SF_POS::BEG);
   int64_t tellp();
   int64_t tellg();
   bool eof();

public:
   int m_iID;
};

class SECTOR_API SphereStream
{
public:
   SphereStream();
   ~SphereStream();

public:
   int init(const std::vector<std::string>& files);
   int init(const int& num);
   void setOutputPath(const std::string& path, const std::string& name);
   void setOutputLoc(const unsigned int& bucket, const Address& addr);

public:
   std::string m_strPath;               // path for output files
   std::string m_strName;               // name prefix for output files

   std::vector<std::string> m_vFiles;   // list of files
   std::vector<int64_t> m_vSize;        // size per file
   std::vector<int64_t> m_vRecNum;      // number of record per file

   std::vector< std::set<Address, AddrComp> > m_vLocation;      // locations for each file
   int32_t* m_piLocID;                  // for output bucket

   int m_iFileNum;                      // number of files
   int64_t m_llSize;                    // total data size
   int64_t m_llRecNum;                  // total number of records
   int64_t m_llStart;                   // start point (record)
   int64_t m_llEnd;                     // end point (record), -1 means the last record

   std::vector<std::string> m_vOrigInput;                       // original input files or dirs, need SphereStream::prepareInput to further process

   int m_iStatus;                       // 0: uninitialized, 1: initialized, -1: bad
};

class SECTOR_API SphereResult
{
public:
   SphereResult();
   ~SphereResult();

public:
   int m_iResID;                // result ID

   int m_iStatus;               // If this DS is processed successfully (> 0, number of rows).
                                // If not (<0), m_pcData may contain error msg

   char* m_pcData;              // result data
   int m_iDataLen;              // result data length
   int64_t* m_pllIndex;         // result data index
   int m_iIndexLen;             // result data index length

   std::string m_strOrigFile;   // original input file
   int64_t m_llOrigStartRec;    // first record of the original input file
   int64_t m_llOrigEndRec;      // last record of the original input file
};

class SECTOR_API SphereProcess
{
friend class Sector;

private:
   SphereProcess() {}
   ~SphereProcess() {}
   const SphereProcess& operator=(const SphereProcess&) {return *this;}

public:
   int close();

   int loadOperator(const char* library);

   int run(const SphereStream& input, SphereStream& output, const std::string& op, const int& rows,
           const char* param = NULL, const int& size = 0, const int& type = 0);
   int run_mr(const SphereStream& input, SphereStream& output, const std::string& mr, const int& rows,
              const char* param = NULL, const int& size = 0);

   int read(SphereResult*& res, const bool& inorder = false, const bool& wait = true);
   int checkProgress();
   int checkMapProgress();
   int checkReduceProgress();
   int waitForCompletion();

   void setMinUnitSize(int size);
   void setMaxUnitSize(int size);
   void setProcNumPerNode(int num);
   void setDataMoveAttr(bool move);

public:
   int m_iID;
};

// ERROR codes
class SECTOR_API SectorError
{
public:
   static const int E_OK = 0;                   // normal/correct/no error
   static const int E_UNKNOWN = -1;             // unknown error
   static const int E_PERMISSION = -1001;       // no permission for IO
   static const int E_EXIST = -1002;            // file/dir already exist
   static const int E_NOEXIST = -1003;          // file/dir not found
   static const int E_BUSY = -1004;             // file busy
   static const int E_LOCALFILE = -1005;        // local file failure
   static const int E_NOEMPTY = -1006;          // directory is not empty (for rmdir)
   static const int E_NOTDIR = -1007;           // directory does not exist or not a directory
   static const int E_FILENOTOPEN = -1008;	// the file is not open() yet for IO
   static const int E_NOREPLICA = -1009;        // all replicas have been lost
   static const int E_NOTFILE = -1010;          // not a regular file
   static const int E_SECURITY = -2000;         // security check failed
   static const int E_NOCERT = -2001;           // no certificate found
   static const int E_ACCOUNT = -2002;          // account does not exist
   static const int E_PASSWORD = -2003;         // incorrect password
   static const int E_ACL = -2004;              // visit from unallowd IP address
   static const int E_INITCTX = -2005;          // failed to initialize CTX
   static const int E_NOSECSERV = -2006;        // security server is down or cannot connect to it
   static const int E_EXPIRED = - 2007;         // connection time out due to no activity
   static const int E_AUTHORITY = - 2008;	// no authority to run the commands, e.g., only root can run it
   static const int E_ADDR = -2009;             // invalid network address
   static const int E_GMP = -2010;              // unable to initialize gmp
   static const int E_DATACHN = -2011;          // unable to initialize data channel
   static const int E_CERTREFUSE = -2012;       // unable to retrieve master certificate
   static const int E_MASTER = -2013;           // all masters have been lost
   static const int E_CONNECTION = -3000;       // unable to connect
   static const int E_BROKENPIPE = -3001;       // data connection lost
   static const int E_TIMEOUT = -3002;          // message recv timeout
   static const int E_RESOURCE = -4000;         // no available resources
   static const int E_NODISK = -4001;           // no enough disk
   static const int E_SYSBUSY = -4002;          // server is too busy to respond in time
   static const int E_VERSION = -5000;          // incompatible version between client and servers
   static const int E_INVALID = -6000;          // invalid parameter
   static const int E_SUPPORT = -6001;          // operation not supported
   static const int E_CANCELED = -6002;         // operation was canceled
   static const int E_BUCKETFAIL = -7001;       // bucket failure
   static const int E_NOPROCESS = -7002;	// no sphere process is running
   static const int E_MISSINGINPUT = -7003;     // at least one input file cannot be located
   static const int E_NOINDEX = -7004;          // missing index file
   static const int E_ALLSPEFAIL = -7005;       // All SPE has failed
   static const int E_NOBUCKET = -7006;         // cannot locate any bucket
   static const int E_UDFLIBFAIL = -7007;       // UDF library failure; incompatible to the slave system

public: // internal error
   static const int E_ROUTING = -101;           // incorrect master node to handle the request
   static const int E_REPSLAVE = -102;          // slave is already in the system; conflict
   static const int E_INVPARAM = -103;		// invalid parameters
   static const int E_SPEREAD = -104;		// SPE cannot read input data for processing
   static const int E_SPEWRITE = -105;		// SPE failed to deliver results
   static const int E_SPEUDF = -106;		// SPE cannot locate and/or open UDF libraries
   static const int E_SPEPROC = - 107;		// UDF processing error

public:
   static int init();
   static std::string getErrorMsg(int ecode);

private:
   static std::map<int, std::string> s_mErrorMsg;
};


class SECTOR_API ConfLocation
{
public:
   static int locate(std::string& loc);
};

struct SECTOR_API Param
{
   std::string m_strName;
   std::vector<std::string> m_vstrValue;
};

class SECTOR_API ClientConf
{
public:
   ClientConf();
   int init(const std::string& path);

   struct DirCacheLifetimeSpec 
   {
      DirCacheLifetimeSpec( const std::string& path, time_t duration ) :
          m_sPathMask( path ), m_seconds( duration ) {}

      std::string m_sPathMask;
      time_t      m_seconds;
   };

   typedef std::deque<DirCacheLifetimeSpec> CacheLifetimes;

public:
   std::string m_strUserName;
   std::string m_strPassword;
   std::set<Address, AddrComp> m_sMasterAddr;
   std::string m_strCertificate;
   int64_t m_llMaxCacheSize;
   int m_iFuseReadAheadBlock;
   int64_t m_llMaxWriteCacheSize;
   std::string m_strLog;
   int m_iLogLevel;
   CacheLifetimes m_pathCache;

public:
   //void print(string&);
};

class SECTOR_API WildCard
{
public:
   static bool isWildCard(const std::string& path);
   static bool match(const std::string& card, const std::string& path);
   static bool contain(const std::string& card, const std::string& path);
};

class SECTOR_API Session
{
public:
   int loadInfo(const char* conf = NULL);

public:
   ClientConf m_ClientConf;
};

class CmdLineParser
{
public:
   int parse(int argc, char** argv);

public:
   std::vector<std::string> m_vSFlags;           // --f
   std::map<std::string, std::string> m_mDFlags; // -f val
   std::vector<std::string> m_vParams;           // parameter
};

class Utility
{
public:
   static void print_error(int code);
   static int login(Sector& client);
   static int logout(Sector& client);
};

#endif
