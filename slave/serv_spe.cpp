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
   Yunhong Gu, last updated 01/16/2011
*****************************************************************************/

#ifndef WIN32
   #include <dlfcn.h>
#endif
#include <algorithm>

#include "slave.h"
#include "sphere.h"

#ifdef WIN32
   #define snprintf sprintf_s
#endif

using namespace std;
using namespace sector;

SPEResult::~SPEResult()
{
   for (vector<int64_t*>::iterator i = m_vIndex.begin(); i != m_vIndex.end(); ++ i)
      delete [] *i;
   for (vector<char*>::iterator i = m_vData.begin(); i != m_vData.end(); ++ i)
      delete [] *i;
}

void SPEResult::init(const int& n)
{
   if (n < 1)
     m_iBucketNum = 1;
   else
     m_iBucketNum = n;

   m_vIndex.resize(m_iBucketNum);
   m_vIndexLen.resize(m_iBucketNum);
   m_vIndexPhyLen.resize(m_iBucketNum);
   m_vData.resize(m_iBucketNum);
   m_vDataLen.resize(m_iBucketNum);
   m_vDataPhyLen.resize(m_iBucketNum);

   for (vector<int32_t>::iterator i = m_vIndexLen.begin(); i != m_vIndexLen.end(); ++ i)
      *i = 0;
   for (vector<int32_t>::iterator i = m_vDataLen.begin(); i != m_vDataLen.end(); ++ i)
      *i = 0;
   for (vector<int32_t>::iterator i = m_vIndexPhyLen.begin(); i != m_vIndexPhyLen.end(); ++ i)
      *i = 0;
   for (vector<int32_t>::iterator i = m_vDataPhyLen.begin(); i != m_vDataPhyLen.end(); ++ i)
      *i = 0;
   for (vector<int64_t*>::iterator i = m_vIndex.begin(); i != m_vIndex.end(); ++ i)
      *i = NULL;
   for (vector<char*>::iterator i = m_vData.begin(); i != m_vData.end(); ++ i)
      *i = NULL;

   m_llTotalDataSize = 0;
}

void SPEResult::addData(const int& bucketid, const char* data, const int64_t& len)
{
   if ((bucketid >= m_iBucketNum) || (bucketid < 0) || (len <= 0))
      return;

   // dynamically increase index buffer size
   if (m_vIndexLen[bucketid] >= m_vIndexPhyLen[bucketid])
   {
      int64_t* tmp = new int64_t[m_vIndexPhyLen[bucketid] + 256];

      if (NULL != m_vIndex[bucketid])
      {
         memcpy((char*)tmp, (char*)m_vIndex[bucketid], m_vIndexLen[bucketid] * 8);
         delete [] m_vIndex[bucketid];
      }
      else
      {
         tmp[0] = 0;
         m_vIndexLen[bucketid] = 1;
      }
      m_vIndex[bucketid] = tmp;
      m_vIndexPhyLen[bucketid] += 256;
   }

   m_vIndex[bucketid][m_vIndexLen[bucketid]] = m_vIndex[bucketid][m_vIndexLen[bucketid] - 1] + len;
   m_vIndexLen[bucketid] ++;

   // dynamically increase data buffer size
   if (m_vDataLen[bucketid] + len > m_vDataPhyLen[bucketid])
   {
      int inc_size = (len / 65536 + 1) * 65536;
      char* tmp = new char[m_vDataPhyLen[bucketid] + inc_size];

      if (NULL != m_vData[bucketid])
      {
         memcpy((char*)tmp, (char*)m_vData[bucketid], m_vDataLen[bucketid]);
         delete [] m_vData[bucketid];
      }
      m_vData[bucketid] = tmp;
      m_vDataPhyLen[bucketid] += inc_size;
   }

   memcpy(m_vData[bucketid] + m_vDataLen[bucketid], data, len);
   m_vDataLen[bucketid] += len;
   m_llTotalDataSize += len;
}

void SPEResult::clear()
{
   for (vector<int32_t>::iterator i = m_vIndexLen.begin(); i != m_vIndexLen.end(); ++ i)
   {
      if (*i > 0)
         *i = 1;
   }
   for (vector<int32_t>::iterator i = m_vDataLen.begin(); i != m_vDataLen.end(); ++ i)
      *i = 0;

   m_llTotalDataSize = 0;
}

SPEDestination::SPEDestination():
m_piSArray(NULL),
m_piRArray(NULL),
m_iLocNum(0),
m_pcOutputLoc(NULL),
m_piLocID(NULL)
{
}

SPEDestination::~SPEDestination()
{
   delete [] m_piSArray;
   delete [] m_piRArray;
   delete [] m_pcOutputLoc;
   delete [] m_piLocID;
}

void SPEDestination::init(const int& buckets)
{
   if (buckets > 0)
   {
      m_piSArray = new int[buckets];
      m_piRArray = new int[buckets];
      for (int i = 0; i < buckets; ++ i)
         m_piSArray[i] = m_piRArray[i] = 0;
   }
   else
   {
      m_piSArray = new int[1];
      m_piRArray = new int[1];
      m_piSArray[0] = m_piRArray[0] = 0;
   }
}

void SPEDestination::reset(const int& buckets)
{
   if (buckets > 0)
   {
      for (int i = 0; i < buckets; ++ i)
         m_piSArray[i] = m_piRArray[i] = 0;
   }
   else
      m_piSArray[0] = m_piRArray[0] = 0;
}

#ifndef WIN32
void* Slave::SPEHandler(void* p)
#else
DWORD WINAPI Slave::SPEHandler(LPVOID p)
#endif
{
   Slave* self = ((Param4*)p)->serv_instance;
   const string ip = ((Param4*)p)->client_ip;
   const int ctrlport = ((Param4*)p)->client_ctrl_port;
   const int dataport = ((Param4*)p)->client_data_port;
   const int speid = ((Param4*)p)->speid;
   const int transid = ((Param4*)p)->transid;
   const int key = ((Param4*)p)->key;
   const string function = ((Param4*)p)->function;
   const int rows = ((Param4*)p)->rows;
   const char* param = ((Param4*)p)->param;
   const int psize = ((Param4*)p)->psize;
   const int type = ((Param4*)p)->type;
   const string master_ip = ((Param4*)p)->master_ip;
   const int master_port = ((Param4*)p)->master_port;
   delete (Param4*)p;

   SectorMsg msg;
   bool init_success = true;

   self->m_SectorLog << LogStart(LogLevel::LEVEL_3) << "SPE starts " << ip << " " << dataport << LogEnd();

   if (self->m_DataChn.connect(ip, dataport) < 0)
   {
      self->m_SectorLog << LogStart(LogLevel::LEVEL_2) << "failed to connect to spe client " << ip << ":" << ctrlport << " " << function << LogEnd();
      init_success = false;
   }

   self->m_SectorLog << LogStart(LogLevel::LEVEL_3) << "connected." << LogEnd();

   // read outupt parameters
   int buckets = 0;
   if (self->m_DataChn.recv4(ip, dataport, transid, buckets) < 0)
      init_success = false;

   SPEDestination dest;
   if (buckets > 0)
   {
      if (self->m_DataChn.recv4(ip, dataport, transid, dest.m_iLocNum) < 0)
         init_success = false;
      int len = dest.m_iLocNum * 80;
      if (self->m_DataChn.recv(ip, dataport, transid, dest.m_pcOutputLoc, len) < 0)
         init_success = false;
      len = buckets * 4;
      if (self->m_DataChn.recv(ip, dataport, transid, (char*&)dest.m_piLocID, len) < 0)
         init_success = false;
   }
   else if (buckets < 0)
   {
      int32_t len = 0;
      if (self->m_DataChn.recv(ip, dataport, transid, dest.m_pcOutputLoc, len) < 0)
         init_success = false;
      dest.m_strLocalFile = dest.m_pcOutputLoc;
   }
   dest.init(buckets);


   // initialize processing function
   self->acceptLibrary(key, ip, dataport, transid);
   SPHERE_PROCESS process = NULL;
   MR_MAP map = NULL;
   MR_PARTITION partition = NULL;
   void* lh = NULL;
   self->openLibrary(key, function, lh);
   if (NULL == lh)
   {
      self->m_SectorLog << LogStart(LogLevel::LEVEL_2) << "failed to open SPE library " << ip << ":" << ctrlport << " " << function << LogEnd();
      init_success = false;
   }

   if (type == 0)
   {
      if (self->getSphereFunc(lh, function, process) < 0)
         init_success = false;
   }
   else if (type == 1)
   {
      if (self->getMapFunc(lh, function, map, partition) < 0)
         init_success = false;
   }
   else
   {
      init_success = false;
   }

   timeval t1, t2, t3, t4;
   gettimeofday(&t1, 0);

   msg.setType(1); // success, return result
   msg.setData(0, (char*)&(speid), 4);

   SPEResult result;
   result.init(buckets);

   // processing...
   while (init_success)
   {
      char* dataseg = NULL;
      int size = 0;
      if (self->m_DataChn.recv(ip, dataport, transid, dataseg, size) < 0)
         break;

      // client request to close this SPE
      if (size < 20)
         break;

      // read data segment parameters
      int64_t offset = *(int64_t*)(dataseg);
      int64_t totalrows = *(int64_t*)(dataseg + 8);
      int32_t dsid = *(int32_t*)(dataseg + 16);
      string datafile = dataseg + 20;
      sprintf(dest.m_pcLocalFileID, ".%d", dsid);
      delete [] dataseg;

      self->m_SectorLog << LogStart(LogLevel::LEVEL_3) << "new job " << datafile << " " << offset << " " << totalrows << LogEnd();

      int64_t* index = NULL;
      if ((totalrows > 0) && (rows != 0))
         index = new int64_t[totalrows + 1];
      char* block = NULL;
      int unitrows = (rows != -1) ? rows : totalrows;
      int progress = 0;

      // read data
      if (0 != rows)
      {
         size = 0;
         if (self->SPEReadData(datafile, offset, size, index, totalrows, block) <= 0)
         {
            delete [] index;
            delete [] block;

            progress = SectorError::E_SPEREAD;
            msg.setData(4, (char*)&progress, 4);
            msg.m_iDataLength = SectorMsg::m_iHdrSize + 8;
            int id = 0;
            self->m_GMP.sendto(ip.c_str(), ctrlport, id, &msg);

            continue;
         }
      }
      else
      {
         // store file name in "process" parameter
         block = new char[datafile.length() + 1];
         strcpy(block, datafile.c_str());
         size = datafile.length() + 1;
         totalrows = 0;
      }

      SInput input;
      input.m_pcUnit = NULL;
      input.m_pcParam = (char*)param;
      input.m_iPSize = psize;
      SOutput output;
      output.m_iBufSize = (size < 64000000) ? 64000000 : size;
      output.m_pcResult = new char[output.m_iBufSize];
      output.m_iIndSize = (totalrows < 640000) ? 640000 : totalrows + 2;
      output.m_pllIndex = new int64_t[output.m_iIndSize];
      output.m_piBucketID = new int[output.m_iIndSize];
      SFile file;
      file.m_strHomeDir = self->m_strHomeDir;
      char path[64];
      sprintf(path, "%d", key);
      file.m_strLibDir = self->m_strHomeDir + ".sphere/" + path + "/";
      file.m_strTempDir = self->m_strHomeDir + ".tmp/";
      file.m_iSlaveID = self->m_iSlaveID;
      file.m_pInMemoryObjects = &self->m_InMemoryObjects;

      result.clear();
      gettimeofday(&t3, 0);

      int deliverystatus = 0;
      int processstatus = 0;

      // process data segments
      for (int i = 0; i < totalrows; i += unitrows)
      {
         if (unitrows > totalrows - i)
            unitrows = totalrows - i;

         input.m_pcUnit = block + index[i] - index[0];
         input.m_iRows = unitrows;
         input.m_pllIndex = index + i;
         output.m_iResSize = 0;
         output.m_iRows = 0;
         output.m_strError = "";

         processstatus = self->processData(input, output, file, result, buckets, process, map, partition);
         if (processstatus < 0)
         {
            progress = SectorError::E_SPEPROC;
            break;
         }

         timeval t;
         gettimeofday(&t, NULL);
         unsigned int seed = t.tv_sec * 1000000 + t.tv_usec;
         srand(seed);
         int ds_thresh = 32000000 * ((rand() % 7) + 1);
         if ((result.m_llTotalDataSize >= ds_thresh) && (buckets != 0))
            deliverystatus = self->deliverResult(buckets, result, dest);

         if (deliverystatus < 0)
         {
            progress = SectorError::E_SPEWRITE;
            break;
         }

         gettimeofday(&t4, 0);
         if (t4.tv_sec - t3.tv_sec > 1)
         {
            progress = i * 100 / totalrows;
            msg.setData(4, (char*)&progress, 4);
            msg.m_iDataLength = SectorMsg::m_iHdrSize + 8;
            int id = 0;
            self->m_GMP.sendto(ip.c_str(), ctrlport, id, &msg);

            t3 = t4;
         }
      }

      // process files
      if (0 == unitrows)
      {
         SNode s;
         LocalFS::stat(self->m_strHomeDir + datafile, s);
         int64_t filesize = s.m_llSize;

         input.m_pcUnit = block;
         input.m_iRows = -1;
         input.m_pllIndex = NULL;
         output.m_llOffset = 0;

         for (int i = 0; (i == 0) || (output.m_llOffset > 0); ++ i)
         {
            // re-initialize output everytime UDF is called, except for offset
            output.m_iResSize = 0;
            output.m_iRows = 0;
            output.m_strError = "";

            processstatus = self->processData(input, output, file, result, buckets, process, map, partition);
            if (processstatus < 0)
            {
               progress = SectorError::E_SPEPROC;
               break;
            }

            timeval t;
            gettimeofday(&t, NULL);
            unsigned int seed = t.tv_sec * 1000000 + t.tv_usec;
            srand(seed);
            int ds_thresh = 32000000 * ((rand() % 7) + 1);
            if ((result.m_llTotalDataSize >= ds_thresh) && (buckets != 0))
               deliverystatus = self->deliverResult(buckets, result, dest);

            if (deliverystatus < 0)
            {
               progress = SectorError::E_SPEWRITE;
               break;
            }

            if (output.m_llOffset > 0)
            {
               progress = output.m_llOffset * 100LL / filesize;
               msg.setData(4, (char*)&progress, 4);
               msg.m_iDataLength = SectorMsg::m_iHdrSize + 8;
               int id = 0;
               self->m_GMP.sendto(ip.c_str(), ctrlport, id, &msg);
            }
         }
      }

      // if buckets = 0, send back to clients, otherwise deliver to local or network locations
      if ((buckets != 0) && (progress >= 0))
         deliverystatus = self->deliverResult(buckets, result, dest);

      if (deliverystatus < 0)
         progress = SectorError::E_SPEWRITE;
      else
         progress = 100;

      self->m_SectorLog << LogStart(LogLevel::LEVEL_3) << "SPE completed " << progress << " " << ip << " " << ctrlport << LogEnd();

      msg.setData(4, (char*)&progress, 4);

      if (100 == progress)
      {
         msg.m_iDataLength = SectorMsg::m_iHdrSize + 8;
         int id = 0;
         self->m_GMP.sendto(ip.c_str(), ctrlport, id, &msg);

         self->sendResultToClient(buckets, dest.m_piSArray, dest.m_piRArray, result, ip, dataport, transid);
         dest.reset(buckets);

         // report new files
         vector<string> filelist;
         for (set<string>::iterator i = file.m_sstrFiles.begin(); i != file.m_sstrFiles.end(); ++ i)
            filelist.push_back(*i);
         self->report(master_ip, master_port, transid, filelist, +FileChangeType::FILE_UPDATE_NEW);
         self->reportMO(master_ip, master_port, transid);
      }
      else
      {
         msg.setData(8, (char*)&processstatus, 4);
         msg.m_iDataLength = SectorMsg::m_iHdrSize + 12;
         if (output.m_strError.length() > 0)
            msg.setData(12, output.m_strError.c_str(), output.m_strError.length() + 1);
         else if (deliverystatus < 0)
         {
            string tmp = "System Error: data transfer to buckets failed.";
            msg.setData(12, tmp.c_str(), tmp.length() + 1);
         }

         int id = 0;
         self->m_GMP.sendto(ip.c_str(), ctrlport, id, &msg);
      }

      delete [] index;
      delete [] block;
      delete [] output.m_pcResult;
      delete [] output.m_pllIndex;
      delete [] output.m_piBucketID;
      index = NULL;
      block = NULL;
   }

   gettimeofday(&t2, 0);
   int duration = t2.tv_sec - t1.tv_sec;
   self->m_SectorLog << LogStart(LogLevel::LEVEL_3) << "comp server closed " << ip << " " << ctrlport << " " << duration << LogEnd();

   delete [] param;

   vector<Address> bad;

   if (init_success)
   {
      self->closeLibrary(lh);

      multimap<int64_t, Address> sndspd;
      for (int i = 0; i < dest.m_iLocNum; ++ i)
      {
         Address addr;
         addr.m_strIP = dest.m_pcOutputLoc + i * 80;
         addr.m_iPort = *(int32_t*)(dest.m_pcOutputLoc + i * 80 + 64);
         int dataport = *(int32_t*)(dest.m_pcOutputLoc + i * 80 + 68);
         int64_t spd = self->m_DataChn.getRealSndSpeed(addr.m_strIP, dataport);
         if (spd > 0)
            sndspd.insert(pair<int64_t, Address>(spd, addr));
      }
      vector<Address> bad;
      self->checkBadDest(sndspd, bad);
   }
   else
   {
      // this SPE failed to initialize. send the error to the client
      int progress = SectorError::E_SPEUDF;
      msg.setData(4, (char*)&progress, 4);
      msg.m_iDataLength = SectorMsg::m_iHdrSize + 8;
      int id = 0;
      self->m_GMP.sendto(ip.c_str(), ctrlport, id, &msg);
   }

   self->reportSphere(master_ip, master_port, transid, &bad);

   // clear this transaction
   self->m_TransManager.updateSlave(transid, self->m_iSlaveID);

   return NULL;
}

#ifndef WIN32
void* Slave::SPEShuffler(void* p)
#else
DWORD WINAPI Slave::SPEShuffler(LPVOID p)
#endif
{
   Slave* self = ((Param5*)p)->serv_instance;
   int transid = ((Param5*)p)->transid;
   string client_ip = ((Param5*)p)->client_ip;
   int client_port = ((Param5*)p)->client_ctrl_port;
   int client_data_port = ((Param5*)p)->client_data_port;
   string path = ((Param5*)p)->path;
   string localfile = ((Param5*)p)->filename;
   int bucketnum = ((Param5*)p)->bucketnum;
   CGMP* gmp = ((Param5*)p)->gmp;
   string function = ((Param5*)p)->function;
   int bucketid = ((Param5*)p)->bucketid;
   const int key = ((Param5*)p)->key;
   const int type = ((Param5*)p)->type;
   string master_ip = ((Param5*)p)->master_ip;
   int master_port = ((Param5*)p)->master_port;

   queue<Bucket>* bq = NULL;
   CMutex* bqlock = NULL;
   CCond* bqcond = NULL;
   int64_t* pendingSize = NULL;
   pthread_t shufflerex;

   bool init_success = true;

   //set up data connection, for keep-alive purpose
   if (self->m_DataChn.connect(client_ip, client_data_port) < 0)
   {
      init_success = false;
   }
   else
   {
      // read library files for MapReduce, no need for Sphere UDF
      if (type == 1)
         self->acceptLibrary(key, client_ip, client_data_port, transid);

      bq = new queue<Bucket>;
      bqlock = new CMutex;
      bqcond = new CCond;
      pendingSize = new int64_t;
      *pendingSize = 0;

      ((Param5*)p)->bq = bq;
      ((Param5*)p)->bqlock = bqlock;
      ((Param5*)p)->bqcond = bqcond;
      ((Param5*)p)->pending = pendingSize;

#ifndef WIN32
      pthread_create(&shufflerex, NULL, SPEShufflerEx, p);
#else
      DWORD ThreadID;
      shufflerex = CreateThread(NULL, 0, SPEShufflerEx, p, NULL, &ThreadID);
#endif

      self->m_SectorLog << LogStart(LogLevel::SCREEN) << "SPE Shuffler " << path << " " << localfile << " " << bucketnum << LogEnd();
   }

   while (init_success)
   {
      string speip;
      int speport;
      SectorMsg msg;
      int msgid;
      int r = gmp->recvfrom(speip, speport, msgid, &msg, false);

      // client releases the task or client has already been shutdown
      if (((r > 0) && (speip == client_ip) && (speport == client_port))
         || ((r < 0) && (!self->m_DataChn.isConnected(client_ip, client_data_port))))
      {
         Bucket b;
         b.totalnum = -1;
         b.totalsize = 0;
         bqlock->acquire();
         bq->push(b);
         bqcond->signal();
         bqlock->release();

         break;
      }

      if (r < 0)
         continue;

      if (*pendingSize > 256000000)
      {
         // too many incoming results, ask the sender to wait
         // the receiver buffer size threshold is set to 256MB. This prevents the shuffler from being overflowed
         // it also helps direct the traffic to less congested shuffler and leads to better load balance
         msg.setType(-msg.getType());
         gmp->sendto(speip, speport, msgid, &msg);
      }
      else
      {
         Bucket b;
         b.totalnum = *(int32_t*)(msg.getData() + 8);;
         b.totalsize = *(int32_t*)(msg.getData() + 12);
         b.src_ip = speip;
         b.src_dataport = *(int32_t*)msg.getData();
         b.session = *(int32_t*)(msg.getData() + 4);

         gmp->sendto(speip, speport, msgid, &msg);

         if (!self->m_DataChn.isConnected(speip, b.src_dataport))
            self->m_DataChn.connect(speip, b.src_dataport);

         bqlock->acquire();
         bq->push(b);
         *pendingSize += b.totalsize;
         bqcond->signal();
         bqlock->release();
      }
   }

   if (init_success)
   {
#ifndef WIN32
      pthread_join(shufflerex, NULL);
#else
      WaitForSingleObject(shufflerex, INFINITE);
#endif

      delete bqlock;
      delete bqcond;
      delete pendingSize;

      SectorMsg msg;
      msg.setType(1); // success, return result
      msg.setData(0, (char*)&(bucketid), 4);
      int progress = 100;
      msg.setData(4, (char*)&progress, 4);
      msg.m_iDataLength = SectorMsg::m_iHdrSize + 8;
      int id = 0;
      self->m_GMP.sendto(client_ip.c_str(), client_port, id, &msg);

      self->m_SectorLog << LogStart(LogLevel::LEVEL_3) << "bucket completed 100 " << client_ip << " " << client_port << LogEnd();
   }

   gmp->close();
   delete gmp;

   self->reportSphere(master_ip, master_port, transid);

   // clear this transaction
   self->m_TransManager.updateSlave(transid, self->m_iSlaveID);

   return NULL;
}

#ifndef WIN32
void* Slave::SPEShufflerEx(void* p)
#else
DWORD WINAPI Slave::SPEShufflerEx(LPVOID p)
#endif
{
   Slave* self = ((Param5*)p)->serv_instance;
   int transid = ((Param5*)p)->transid;
   string path = ((Param5*)p)->path;
   string localfile = ((Param5*)p)->filename;
   int bucketnum = ((Param5*)p)->bucketnum;
   const int key = ((Param5*)p)->key;
   const int type = ((Param5*)p)->type;
   string function = ((Param5*)p)->function;
   string master_ip = ((Param5*)p)->master_ip;
   int master_port = ((Param5*)p)->master_port;
   queue<Bucket>* bq = ((Param5*)p)->bq;
   CMutex* bqlock = ((Param5*)p)->bqlock;
   CCond* bqcond = ((Param5*)p)->bqcond;
   int64_t* pendingSize = ((Param5*)p)->pending;
   delete (Param5*)p;

   self->createDir(path);

   // remove old result data files
   for (int i = 0; i < bucketnum; ++ i)
   {
      int size = self->m_strHomeDir.length() + path.length() + localfile.length() + 64;
      char* tmp = new char[size];
      snprintf(tmp, size, "%s.%d", (self->m_strHomeDir + path + "/" + localfile).c_str(), i);
      LocalFS::erase(tmp);
      snprintf(tmp, size, "%s.%d.idx", (self->m_strHomeDir + path + "/" + localfile).c_str(), i);
      LocalFS::erase(tmp);
      delete [] tmp;
   }

   // index file initial offset
   vector<int64_t> offset;
   offset.resize(bucketnum);
   for (vector<int64_t>::iterator i = offset.begin(); i != offset.end(); ++ i)
      *i = 0;
   set<int> fileid;

   while (true)
   {
      bqlock->acquire();
      while (bq->empty())
         bqcond->wait(*bqlock);
      Bucket b = bq->front();
      bq->pop();
      *pendingSize -= b.totalsize;
      bqlock->release();

      if (b.totalnum == -1)
         break;

      string speip = b.src_ip;
      int dataport = b.src_dataport;
      int session = b.session;

      for (int i = 0; i < b.totalnum; ++ i)
      {
         int bucket = 0;
         if (self->m_DataChn.recv4(speip, dataport, session, bucket) < 0)
            continue;

         fileid.insert(bucket);

         char* tmp = new char[self->m_strHomeDir.length() + path.length() + localfile.length() + 64];
         sprintf(tmp, "%s.%d", (self->m_strHomeDir + path + "/" + localfile).c_str(), bucket);
         fstream datafile(tmp, ios::out | ios::binary | ios::app);
         sprintf(tmp, "%s.%d.idx", (self->m_strHomeDir + path + "/" + localfile).c_str(), bucket);
         fstream indexfile(tmp, ios::out | ios::binary | ios::app);
         delete [] tmp;
         int64_t start = offset[bucket];
         if (0 == start)
            indexfile.write((char*)&start, 8);

         int32_t len;
         char* data = NULL;
         if (self->m_DataChn.recv(speip, dataport, session, data, len) < 0)
            continue;
         datafile.write(data, len);
         delete [] data;

         tmp = NULL;
         if (self->m_DataChn.recv(speip, dataport, session, tmp, len) < 0)
            continue;
         int64_t* index = (int64_t*)tmp;
         for (int j = 0; j < len / 8; ++ j)
            index[j] += start;
         offset[bucket] = index[len / 8 - 1];
         indexfile.write(tmp, len);
         delete [] tmp;

         datafile.close();
         indexfile.close();
      }

      // update total received data
      self->m_SlaveStat.updateIO(speip, b.totalsize, +SlaveStat::SYS_IN);
   }

   // sort and reduce
   if (type == 1)
   {
      void* lh = NULL;
      self->openLibrary(key, function, lh);

      if (NULL != lh)
      {
         MR_COMPARE comp = NULL;
         MR_REDUCE reduce = NULL;
         self->getReduceFunc(lh, function, comp, reduce);

         if (NULL != comp)
         {
            char* tmp = new char[self->m_strHomeDir.length() + path.length() + localfile.length() + 64];
            for (set<int>::iterator i = fileid.begin(); i != fileid.end(); ++ i)
            {
               sprintf(tmp, "%s.%d", (self->m_strHomeDir + path + "/" + localfile).c_str(), *i);
               self->sort(tmp, comp, reduce);
            }
            delete [] tmp;
         }

         self->closeLibrary(lh);
      }
   }

   // report sphere output files
   char* tmp = new char[path.length() + localfile.length() + 64];
   vector<string> filelist;
   for (set<int>::iterator i = fileid.begin(); i != fileid.end(); ++ i)
   {
      sprintf(tmp, "%s.%d", (path + "/" + localfile).c_str(), *i);
      filelist.push_back(tmp);
      sprintf(tmp, "%s.%d.idx", (path + "/" + localfile).c_str(), *i);
      filelist.push_back(tmp);
   }
   delete [] tmp;

   self->report(master_ip, master_port, transid, filelist, 1);

   return NULL;
}

int Slave::SPEReadData(const string& datafile, const int64_t& offset, int& size, int64_t* index, const int64_t& totalrows, char*& block)
{
   SNode sn;
   string idxfile = datafile + ".idx";

   //read index
   if (m_pLocalFile->lookup(idxfile.c_str(), sn) >= 0)
   {
      fstream idx;
      idx.open((m_strHomeDir + idxfile).c_str(), ios::in | ios::binary);
      if (idx.bad() || idx.fail())
         return -1;
      idx.seekg(offset * 8);
      idx.read((char*)index, (totalrows + 1) * 8);
      idx.close();
   }
   else
   {
      if (readSectorFile(idxfile, offset * 8, (totalrows + 1) * 8, (char*)index) < 0)
         return -1;
   }

   size = index[totalrows] - index[0];
   block = new char[size];

   // read data file
   if (m_pLocalFile->lookup(datafile.c_str(), sn) >= 0)
   {
      fstream ifs;
      ifs.open((m_strHomeDir + datafile).c_str(), ios::in | ios::binary);
      if (ifs.bad() || ifs.fail())
         return -1;
      ifs.seekg(index[0]);
      ifs.read(block, size);
      ifs.close();
   }
   else
   {
      if (readSectorFile(datafile, index[0], size, block) < 0)
         return -1;
   }

   return totalrows;
}

int Slave::sendResultToFile(const SPEResult& result, const string& localfile, const int64_t& offset)
{
   fstream datafile, idxfile;
   datafile.open((m_strHomeDir + localfile).c_str(), ios::out | ios::binary | ios::app);
   idxfile.open((m_strHomeDir + localfile + ".idx").c_str(), ios::out | ios::binary | ios::app);

   datafile.write(result.m_vData[0], result.m_vDataLen[0]);

   if (offset == 0)
      idxfile.write((char*)&offset, 8);
   else
   {
      for (int i = 1; i <= result.m_vIndexLen[0]; ++ i)
         result.m_vIndex[0][i] += offset;
   }
   idxfile.write((char*)(result.m_vIndex[0] + 1), (result.m_vIndexLen[0] - 1) * 8);

   datafile.close();
   idxfile.close();

   return 0;
}

int Slave::sendResultToClient(const int& buckets, const int* sarray, const int* rarray, const SPEResult& result, const string& clientip, int clientport, int session)
{
   if (buckets == -1)
   {
      // send back result file/record size
      m_DataChn.send(clientip, clientport, session, (char*)sarray, 4);
      m_DataChn.send(clientip, clientport, session, (char*)rarray, 4);
   }
   else if (buckets == 0)
   {
      // send back the result data
      m_DataChn.send(clientip, clientport, session, result.m_vData[0], result.m_vDataLen[0]);
      m_DataChn.send(clientip, clientport, session, (char*)result.m_vIndex[0], result.m_vIndexLen[0] * 8);
   }
   else
   {
      // send back size and rec_num information
      m_DataChn.send(clientip, clientport, session, (char*)sarray, buckets * 4);
      m_DataChn.send(clientip, clientport, session, (char*)rarray, buckets * 4);
   }

   return 0;
}

int Slave::sendResultToBuckets(const int& buckets, const SPEResult& result, const SPEDestination& dest)
{
   map<int, set<int> > ResByLoc;
   map<int, int> SizeByLoc;

   for (int i = 0; i < dest.m_iLocNum; ++ i)
   {
      ResByLoc[i].clear();
      SizeByLoc[i] = 0;
   }

   // summarize information for each bucket, ignore empty buckets
   for (int r = 0; r < buckets; ++ r)
   {
      int i = dest.m_piLocID[r];
      if (0 != result.m_vDataLen[r])
      {
         ResByLoc[i].insert(r);
         SizeByLoc[i] += result.m_vDataLen[r] + (result.m_vIndexLen[r] - 1) * 8;
      }
   }

   unsigned int tn = 0;
   map<int, set<int> >::iterator p = ResByLoc.begin();

   while(!ResByLoc.empty())
   {
      int i = p->first;
      if (++ p == ResByLoc.end())
         p = ResByLoc.begin();

      if (ResByLoc[i].empty())
      {
         //skip empty buckets
         ResByLoc.erase(i);
         SizeByLoc.erase(i);
         continue;
      }

      // retrieve bucket location/address
      char* dstip = dest.m_pcOutputLoc + i * 80;
      int32_t dstport = *(int32_t*)(dest.m_pcOutputLoc + i * 80 + 68);
      int32_t shufflerport = *(int32_t*)(dest.m_pcOutputLoc + i * 80 + 72);
      int32_t session = *(int32_t*)(dest.m_pcOutputLoc + i * 80 + 76);

      SectorMsg msg;
      int32_t srcport = m_DataChn.getPort();
      msg.setData(0, (char*)&srcport, 4);
      msg.setData(4, (char*)&session, 4);
      int totalnum = ResByLoc[i].size();
      msg.setData(8, (char*)&totalnum, 4);
      int totalsize = SizeByLoc[i];
      msg.setData(12, (char*)&totalsize, 4);
      msg.m_iDataLength = SectorMsg::m_iHdrSize + 16;

      // request to send results to the slave node
      msg.setType(1);
      if (m_GMP.rpc(dstip, shufflerport, &msg, &msg) < 0)
         return -1;

      if (msg.getType() < 0)
      {
         // if all shufflers are busy, wait here a little while
         if (++ tn >= ResByLoc.size())
         {
            tn = 0;
            #ifndef WIN32
            usleep(100000);
            #else
            Sleep(100);
            #endif
         }
         continue;
      }

      if (!m_DataChn.isConnected(dstip, dstport))
      {
         if (m_DataChn.connect(dstip, dstport) < 0)
            return -1;
      }

      // send results for one bucket a time
      for (set<int>::iterator r = ResByLoc[i].begin(); r != ResByLoc[i].end(); ++ r)
      {
         int32_t id = *r;
         m_DataChn.send(dstip, dstport, session, (char*)&id, 4);
         m_DataChn.send(dstip, dstport, session, result.m_vData[id], result.m_vDataLen[id]);
         m_DataChn.send(dstip, dstport, session, (char*)(result.m_vIndex[id] + 1), (result.m_vIndexLen[id] - 1) * 8);
      }

      // update total sent data
      m_SlaveStat.updateIO(dstip, SizeByLoc[i], +SlaveStat::SYS_OUT);

      ResByLoc.erase(i);
      SizeByLoc.erase(i);
   }

   return 1;
}

int Slave::acceptLibrary(const int& key, const string& ip, int port, int session)
{
   int32_t num = -1;
   m_DataChn.recv4(ip, port, session, num);

   for(int i = 0; i < num; ++ i)
   {
      char* lib = NULL;
      int size = 0;
      m_DataChn.recv(ip, port, session, lib, size);
      char* buf = NULL;
      m_DataChn.recv(ip, port, session, buf, size);

      int buflen = m_strHomeDir.length() + 64;
      char* path = new char[buflen];
      snprintf(path, buflen, "%s/.sphere/%d", m_strHomeDir.c_str(), key);

      SNode s;
      string lib_file = string(path) + "/" + lib;
      if (LocalFS::stat(lib_file, s) < 0)
      {
         LocalFS::mkdir(path);

         fstream ofs(lib_file.c_str(), ios::out | ios::trunc | ios::binary);
         ofs.write(buf, size);
         ofs.close();

         ::chmod(lib_file.c_str(), S_IRWXU);
      }

      delete [] lib;
      delete [] buf;
      delete [] path;
   }

   if (num > 0)
   {
      int32_t confirm = 0;
      m_DataChn.send(ip, port, session, (char*)&confirm, 4);
   }

   return 0;
}

int Slave::openLibrary(const int& key, const string& lib, void*& lh)
{
   char path[64];
   sprintf(path, "%d", key);

#ifndef WIN32
   lh = dlopen((m_strHomeDir + ".sphere/" + path + "/" + lib + ".so").c_str(), RTLD_LAZY);
   if (NULL == lh)
   {
      // if no user uploaded lib, check permanent lib
      lh = dlopen((m_strHomeDir + ".sphere/perm/" + lib + ".so").c_str(), RTLD_LAZY);

      if (NULL == lh)
      {
         m_SectorLog << LogStart(LogLevel::LEVEL_2) << "Open Library Error: " << dlerror() << LogEnd();
         return -1;
      }
   }

   return 0;
#else
   return -1;
#endif
}

int Slave::getSphereFunc(void* lh, const string& function, SPHERE_PROCESS& process)
{
#ifndef WIN32
   if (NULL == lh)
      return -1;

   process = (SPHERE_PROCESS)dlsym(lh, function.c_str());
   if (NULL == process)
   {
      m_SectorLog << LogStart(LogLevel::LEVEL_2) << "Open Library Error: " << dlerror() << LogEnd();
      return -1;
   }

   return 0;
#else
   return -1;
#endif
}

int Slave::getMapFunc(void* lh, const string& function, MR_MAP& map, MR_PARTITION& partition)
{
#ifndef WIN32
   if (NULL == lh)
      return -1;

   map = (MR_MAP)dlsym(lh, (function + "_map").c_str());

   partition = (MR_PARTITION)dlsym(lh, (function + "_partition").c_str());
   if (NULL == partition)
   {
      m_SectorLog << LogStart(LogLevel::LEVEL_2) << "Open Library Error: " << dlerror() << LogEnd();
      return -1;
   }

   return 0;
#else
   return -1;
#endif
}

int Slave::getReduceFunc(void* lh, const string& function, MR_COMPARE& compare, MR_REDUCE& reduce)
{
#ifndef WIN32
   if (NULL == lh)
      return -1;

   reduce = (MR_REDUCE)dlsym(lh, (function + "_reduce").c_str());

   compare = (MR_COMPARE)dlsym(lh, (function + "_compare").c_str());
   if (NULL == compare)
   {
      m_SectorLog << LogStart(LogLevel::LEVEL_2) << "Open Library Error: " << dlerror() << LogEnd();
      return -1;
   }

   return 0;
#else
   return -1;
#endif
}

int Slave::closeLibrary(void* lh)
{
#ifndef WIN32
   if (NULL == lh)
      return -1;

   return dlclose(lh);
#else
   return -1;
#endif
}

int Slave::sort(const string& bucket, MR_COMPARE comp, MR_REDUCE red)
{
   fstream ifs(bucket.c_str(), ios::in | ios::binary);
   if (ifs.fail())
      return -1;

   ifs.seekg(0, ios::end);
   int size = ifs.tellg();
   ifs.seekg(0, ios::beg);
   char* rec = new char[size];
   ifs.read(rec, size);
   ifs.close();

   ifs.open((bucket + ".idx").c_str());
   if (ifs.fail())
   {
      delete [] rec;
      return -1;
   }

   ifs.seekg(0, ios::end);
   size = ifs.tellg();
   ifs.seekg(0, ios::beg);
   int64_t* idx = new int64_t[size / 8];
   ifs.read((char*)idx, size);
   ifs.close();

   size = size / 8 - 1;

   vector<MRRecord> vr;
   vr.resize(size);
   int64_t offset = 0;
   for (vector<MRRecord>::iterator i = vr.begin(); i != vr.end(); ++ i)
   {
      i->m_pcData = rec + idx[offset];
      i->m_iSize = idx[offset + 1] - idx[offset];
      i->m_pCompRoutine = comp;
      offset ++;
   }

   std::sort(vr.begin(), vr.end(), ltrec());

   if (red != NULL)
   {
      reduce(vr, bucket, red, NULL, 0);
   }
   else
   {
      // if reduced, no need to store these intermediate files

      fstream sorted((bucket + ".sorted").c_str(), ios::out | ios::binary | ios::trunc);
      fstream sortedidx((bucket + ".sorted.idx").c_str(), ios::out | ios::binary | ios::trunc);
      offset = 0;
      sortedidx.write((char*)&offset, 8);
      for (vector<MRRecord>::iterator i = vr.begin(); i != vr.end(); ++ i)
      {
         sorted.write(i->m_pcData, i->m_iSize);
         offset += i->m_iSize;
         sortedidx.write((char*)&offset, 8);
      }
      sorted.close();
      sortedidx.close();
   }

   delete [] rec;
   delete [] idx;

   return 0;
}

int Slave::reduce(vector<MRRecord>& vr, const string& bucket, MR_REDUCE red, void* param, int psize)
{
   SInput input;
   input.m_pcUnit = NULL;
   input.m_pcParam = (char*)param;
   input.m_iPSize = psize;


   int rdsize = 256000000;
   int risize = 1000000;

   SOutput output;
   output.m_pcResult = new char[rdsize];
   output.m_iBufSize = rdsize;
   output.m_pllIndex = new int64_t[risize];
   output.m_iIndSize = risize;
   output.m_piBucketID = new int[risize];
   output.m_llOffset = 0;

   SFile file;
   file.m_strHomeDir = m_strHomeDir;
//   file.m_strLibDir = m_strHomeDir + ".sphere/" + path + "/";
   file.m_strTempDir = m_strHomeDir + ".tmp/";
   file.m_pInMemoryObjects = &m_InMemoryObjects;

   char* idata = NULL;
   int64_t* iidx = NULL;

   try
   {
      idata = new char[256000000];
      iidx = new int64_t[1000000];
   }
   catch (...)
   {
      delete [] idata;
      delete [] iidx;
      return -1;
   }

   fstream reduced((bucket + ".reduced").c_str(), ios::out | ios::binary | ios::trunc);
   fstream reducedidx((bucket + ".reduced.idx").c_str(), ios::out | ios::binary | ios::trunc);
   int64_t roff = 0;

   for (vector<MRRecord>::iterator i = vr.begin(); i != vr.end();)
   {
      iidx[0] = 0;
      vector<MRRecord>::iterator curr = i;
      memcpy(idata, i->m_pcData, i->m_iSize);
      iidx[1] = i->m_iSize;
      int offset = 1;

      i ++;
      while ((i != vr.end()) && (i->m_pCompRoutine(curr->m_pcData, curr->m_iSize, i->m_pcData, i->m_iSize) == 0))
      {
         memcpy(idata + iidx[offset], i->m_pcData, i->m_iSize);
         iidx[offset + 1] = iidx[offset] + i->m_iSize;
         offset ++;
         i ++;
      }

      input.m_pcUnit = idata;
      input.m_pllIndex = iidx;
      input.m_iRows = offset;
      red(&input, &output, &file);

      for (int r = 0; r < output.m_iRows; ++ r)
      {
         reduced.write(output.m_pcResult + output.m_pllIndex[r], output.m_pllIndex[r + 1] - output.m_pllIndex[r]);
         roff += output.m_pllIndex[r + 1] - output.m_pllIndex[r];
         reducedidx.write((char*)&roff, 8);
      }
   }

   reduced.close();
   reducedidx.close();

   delete [] output.m_pcResult;
   delete [] output.m_pllIndex;
   delete [] output.m_piBucketID;
   delete [] idata;
   delete [] iidx;

   return 0;
}

int Slave::processData(SInput& input, SOutput& output, SFile& file, SPEResult& result, int buckets, SPHERE_PROCESS process, MR_MAP map, MR_PARTITION partition)
{
   // pass relative offset, from 0, to the processing function
   int64_t uoff = (input.m_pllIndex != NULL) ? input.m_pllIndex[0] : 0;
   for (int p = 0; p <= input.m_iRows; ++ p)
      input.m_pllIndex[p] = input.m_pllIndex[p] - uoff;

   if (NULL != process)
   {
      process(&input, &output, &file);
      if (buckets > 0)
      {
         for (int r = 0; r < output.m_iRows; ++ r)
            result.addData(output.m_piBucketID[r], output.m_pcResult + output.m_pllIndex[r], output.m_pllIndex[r + 1] - output.m_pllIndex[r]);
      }
      else
      {
         // if no bucket is used, do NOT check the BucketID field, so devlopers do not need to assign these values
         for (int r = 0; r < output.m_iRows; ++ r)
            result.addData(0, output.m_pcResult + output.m_pllIndex[r], output.m_pllIndex[r + 1] - output.m_pllIndex[r]);
      }
   }
   else
   {
      if (NULL == map)
      {
         // partition input directly if there is no map
         for (int r = 0; r < input.m_iRows; ++ r)
         {
            char* data = input.m_pcUnit + input.m_pllIndex[r];
            int size = input.m_pllIndex[r + 1] - input.m_pllIndex[r];
            result.addData(partition(data, size, input.m_pcParam, input.m_iPSize), data, size);
         }
      }
      else
      {
         map(&input, &output, &file);
         for (int r = 0; r < output.m_iRows; ++ r)
         {
            char* data = output.m_pcResult + output.m_pllIndex[r];
            int size = output.m_pllIndex[r + 1] - output.m_pllIndex[r];
            result.addData(partition(data, size, input.m_pcParam, input.m_iPSize), data, size);
	 }
      }
   }

   // restore the original offset
   for (int p = 0; p <= input.m_iRows; ++ p)
      input.m_pllIndex[p] = input.m_pllIndex[p] + uoff;

   return 0;
}

int Slave::deliverResult(const int& buckets, SPEResult& result, SPEDestination& dest)
{
   int ret = 0;

   if (buckets == -1)
      ret = sendResultToFile(result, dest.m_strLocalFile + dest.m_pcLocalFileID, dest.m_piSArray[0]);
   else if (buckets > 0)
      ret = sendResultToBuckets(buckets, result, dest);

   for (int b = 0; b < buckets; ++ b)
   {
      dest.m_piSArray[b] += result.m_vDataLen[b];
      if (result.m_vDataLen[b] > 0)
         dest.m_piRArray[b] += result.m_vIndexLen[b] - 1;
   }

   if (buckets != 0)
      result.clear();

   return ret;
}

int Slave::checkBadDest(multimap<int64_t, Address>& sndspd, vector<Address>& bad)
{
   bad.clear();

   if (sndspd.empty())
      return 0;

   int m = sndspd.size() / 2;
   multimap<int64_t, Address>::iterator p = sndspd.begin();
   for (int i = 0; i < m; ++ i)
      ++ p;

   int64_t median = p->first;

   int locpos = 0;
   for (multimap<int64_t, Address>::iterator i = sndspd.begin(); i != sndspd.end(); ++ i)
   {
      if (i->first > (median / 2))
         return bad.size();

      bad.push_back(i->second);
      locpos ++;
   }

   return bad.size();
}

int Slave::readSectorFile(const string& filename, const int64_t& offset, const int64_t& size, char* buf)
{
   SectorMsg msg;
   msg.setType(110); // open the index file
   msg.setKey(0);

   int32_t mode = 1;
   msg.setData(0, (char*)&mode, 4);
   int32_t port = m_DataChn.getPort();
   msg.setData(4, (char*)&port, 4);
   int32_t len_name = filename.length() + 1;
   msg.setData(8, (char*)&len_name, 4);
   msg.setData(12, filename.c_str(), len_name);
   int32_t len_opt = 0;
   msg.setData(12 + len_name, (char*)&len_opt, 4);

   Address addr;
   m_Routing.lookup(filename, addr);

   if (m_GMP.rpc(addr.m_strIP.c_str(), addr.m_iPort, &msg, &msg) < 0)
      return -1;
   if (msg.getType() < 0)
      return -1;

   int32_t session = *(int32_t*)msg.getData();
   string srcip = msg.getData() + 24;
   int32_t srcport = *(int32_t*)(msg.getData() + 64 + 24);

   // connect to the slave node with the file.
   if (!m_DataChn.isConnected(srcip, srcport))
   {
      if (m_DataChn.connect(srcip, srcport) < 0)
         return -1;
   }

   int32_t cmd = 1;
   m_DataChn.send(srcip, srcport, session, (char*)&cmd, 4);

   char req[16];
   *(int64_t*)req = offset;
   *(int64_t*)(req + 8) = size;
   if (m_DataChn.send(srcip, srcport, session, req, 16) < 0)
      return -1;

   int response = -1;
   if (m_DataChn.recv4(srcip, srcport, session, response) < 0)
      return -1;

   char* tmp = NULL;
   int recvsize = size;
   if (m_DataChn.recv(srcip, srcport, session, tmp, recvsize) < 0)
      return -1;
   if (recvsize == size)
      memcpy(buf, tmp, size);
   delete [] tmp;

   // file close command: 5
   cmd = 5;
   m_DataChn.send(srcip, srcport, session, (char*)&cmd, 4);
   m_DataChn.recv4(srcip, srcport, session, response);

   // update total received data
   m_SlaveStat.updateIO(srcip, size, +SlaveStat::SYS_IN);

   if (recvsize != size)
      return -1;

   return size;
}
