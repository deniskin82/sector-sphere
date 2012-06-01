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
   Yunhong Gu, last updated 07/06/2010
*****************************************************************************/

#ifndef __SPHERE_CLIENT_H__
#define __SPHERE_CLIENT_H__

#include "client.h"

namespace sector
{

class DCClient
{
friend class Client;

private:
   DCClient();
   ~DCClient();
   const DCClient& operator=(const DCClient&) {return *this;}

public:
   int close();

   int loadOperator(const char* library);

   int run(const SphereStream& input, SphereStream& output, const std::string& op, const int& rows, const char* param = NULL, const int& size = 0, const int& type = 0);
   int run_mr(const SphereStream& input, SphereStream& output, const std::string& mr, const int& rows, const char* param = NULL, const int& size = 0);

   // rows:
   // 	n (n > 0): n rows per time
   //	0: no rows, one file per time
   //	-1: all rows in each segment

   int read(SphereResult*& res, const bool inorder = false, const bool wait = true);
   int checkProgress();
   int checkMapProgress();
   int checkReduceProgress();
   int waitForCompletion();

   // TODO: support callback APIs for result handling.

   inline void setMinUnitSize(int size) {m_iMinUnitSize = size;}
   inline void setMaxUnitSize(int size) {m_iMaxUnitSize = size;}
   inline void setProcNumPerNode(int num) {m_iCore = num;}
   inline void setDataMoveAttr(bool move) {m_bDataMove = move;}

private:
   int m_iProcType;				// 0: sphere 1: mapreduce

   std::string m_strOperator;			// name of the UDF
   char* m_pcParam;				// parameter
   int m_iParamSize;				// size of the parameter
   SphereStream* m_pInput;			// input stream
   SphereStream* m_pOutput;			// output stream
   int m_iRows;					// number of rows per UDF processing
   int m_iOutputType;				// type of output: -1, loca; 0, none; N, buckets 
   char* m_pOutputLoc;				// output location for buckets
   int m_iSPENum;				// number of SPEs

   struct DS
   {
      int m_iID;				// DS ID
      std::string m_strDataFile;		// input data file
      int64_t m_llOffset;			// input data offset
      int64_t m_llSize;				// input data size
      int m_iSPEID;				// processing SPE
      std::set<Address, AddrComp>* m_pLoc;	// locations of DS

      int m_iStatus;                            // 0: not started yet; 1: in progress; 2: done, result ready; 3: result read; -1: failed
      int m_iRetryNum;				// number of retry due to failure
      SphereResult* m_pResult;			// processing result
   };
   std::map<int, DS*> m_mpDS;
   pthread_mutex_t m_DSLock;

   struct SPE
   {
      int32_t m_iID;				// SPE ID
      std::string m_strIP;			// SPE IP
      int m_iPort;				// SPE GMP port
      int m_iDataPort;				// SPE data port
      DS* m_pDS;				// current processing DS
      int m_iStatus;				// -1: abandond; 0: uninitialized; 1: ready; 2; running
      int m_iProgress;				// 0 - 100 (%)
      int64_t m_StartTime;			// SPE start time
      int64_t m_LastUpdateTime;			// SPE last update time
      int m_iSession;				// SPE session ID for data channel

      std::map<int, std::list<int> > m_mDSQueue;	// job queue
   };
   std::map<int, SPE> m_mSPE;

   struct BUCKET
   {
      int32_t m_iID;				// bucket ID
      std::string m_strIP;			// slave IP address
      int m_iPort;				// slave GMP port
      int m_iDataPort;				// slave Data port
      int m_iShufflerPort;			// Shuffer GMP port
      int m_iSession;				// Shuffler session ID
      int m_iProgress;				// bucket progress, 0 or 100 
      int64_t m_LastUpdateTime;			// last update time
   };
   std::map<int, BUCKET> m_mBucket;		// list of all buckets

   int m_iProgress;				// progress, 0..100
   double m_dRunningProgress;			// progress of running but incomplete jobs
   int m_iAvgRunTime;				// average running time, in seconds
   int m_iTotalDS;				// total number of data segments
   int m_iTotalSPE;				// total number of SPEs
   int m_iAvailRes;				// number of availale result to be read
   bool m_bBucketHealth;			// if all bucket nodes are alive; unable to recover from bucket failure

   pthread_mutex_t m_ResLock;
   pthread_cond_t m_ResCond;

   int m_iMinUnitSize;				// minimum data segment size
   int m_iMaxUnitSize;				// maximum data segment size, must be smaller than physical memory
   int m_iCore;					// number of processing instances on each node
   bool m_bDataMove;				// if source data is allowed to move for Sphere process

   struct OP
   {
      std::string m_strLibrary;			// UDF name
      std::string m_strLibPath;			// path of the dynamic library that contains the UDF
      int m_iSize;				// size of the library
      std::set<std::string> m_sUploaded;	// slave address that the op has been uploaded to; to avoid uploading a library multiple times
   };
   std::map<std::string, OP> m_mOP;
   int loadOperator(const std::string& ip, const int port, const int dataport, const int session);

private: // inputs and outputs
   int dataInfo(const std::vector<std::string>& files, std::vector<std::string>& info);
   int prepareInput();
   int prepareSPE(const char* spenodes);
   int segmentData();
   int prepareSPEJobQueue();
   int prepareOutput(const char* spenodes);
   int postProcessOutput();

private: // running and schedulling
#ifndef WIN32
   static void* run(void*);
#else
   static DWORD WINAPI run(LPVOID);
#endif

   pthread_mutex_t m_RunLock;
   bool m_bOpened;

   int start();
   int checkSPE();
   int startSPE(SPE& s, DS* d);
   int connectSPE(SPE& s);
   int checkBucket();
   int readResult(SPE* s);

private:
   Client* m_pClient;				// pointer to the sector client
   int m_iID;					// unique instance id
};

}  // namespace sector

#endif
