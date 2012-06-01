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
   Yunhong Gu, last updated 04/18/2011
*****************************************************************************/

#ifndef WIN32
   #include <netdb.h>
   #include <sys/times.h>
   #include <sys/statvfs.h>
#ifndef __APPLE__
   #include <sys/vfs.h>
#endif
   #include <unistd.h>
   #include <utime.h>
#else
   #include <psapi.h>
   #include <stdio.h>
   #include <sys/utime.h>
   #include <windows.h>
#endif
#include <iostream>
#include <sstream>

#include <common.h>
#include "slave.h"
#include "ssltransport.h"

#define ERR_MSG( msg ) \
{\
   m_SectorLog << LogStart(LogLevel::LEVEL_1) << msg << LogEnd(); \
}

#define INFO_MSG( msg ) \
{\
   m_SectorLog << LogStart(LogLevel::LEVEL_3) << msg << LogEnd(); \
}

#define DBG_MSG( msg ) \
{\
   m_SectorLog << LogStart(LogLevel::LEVEL_9) << msg << LogEnd(); \
}

using namespace std;
using namespace sector;

Slave::Slave():
m_iSlaveID(-1),
m_iDataPort(0),
m_iLocalPort(0),
m_iMasterPort(0),
m_strBase("./"),
m_pLocalFile(NULL),
m_bRunning(false),
m_bDiskHealth(true),
m_bNetworkHealth(true)
{
}

Slave::~Slave()
{
   delete m_pLocalFile;
}

int Slave::init(const string* base, const SlaveConf* global_conf)
{
   bool base_found = true;
   if ((NULL != base) && !base->empty())
      m_strBase = Metadata::revisePath(*base);
   else if (ConfLocation::locate(m_strBase) < 0)
      base_found = false;

   string conf_file = m_strBase + "/conf/slave.conf";
   if ((base_found && (m_SysConfig.init(conf_file) < 0) && !global_conf) || (!base_found && !global_conf))
   {
      cerr << "unable to locate or initialize from configuration file; quit.\n";
      return -1;
   }

   // Global Configuration will overwrite local configurations
   m_SysConfig.set(global_conf);

   // obtain master IP address
   m_strMasterHost = m_SysConfig.m_strMasterHost;
   struct hostent* masterip = gethostbyname(m_strMasterHost.c_str());
   if (NULL == masterip)
   {
      cerr << "invalid master address " << m_strMasterHost << endl;
      return -1;
   }
   char buf[64];
   m_strMasterIP = inet_ntop(AF_INET, masterip->h_addr_list[0], buf, 64);
   m_iMasterPort = m_SysConfig.m_iMasterPort;

   UDTTransport::initialize();

   // init GMP
   m_GMP.init(0);
   m_iLocalPort = m_GMP.getPort();

   // initialize local directory, m_strHomeDir must end with a "/"
   m_strHomeDir = Metadata::revisePath(m_SysConfig.m_strHomeDir);
   if (m_strHomeDir.empty())
      m_strHomeDir = "./";
   else if (m_strHomeDir[m_strHomeDir.length() - 1] != '/')
      m_strHomeDir.append(1, '/');

   // check local directory
   if (createSysDir() < 0)
   {
      cerr << "unable to create system directory " << m_strHomeDir << endl;
      return -1;
   }

   // initialize slave log
   m_SectorLog.init((m_strHomeDir + "/.log").c_str());
   m_SectorLog.setLevel(m_SysConfig.m_iLogLevel);
   m_SectorLog.copyScreen(m_SysConfig.m_bVerbose);

   //copy permanent sphere libraries
   vector<SNode> filelist;
   LocalFS::list_dir(m_strBase + "/slave/sphere", filelist);
   for (vector<SNode>::iterator i = filelist.begin(); i != filelist.end(); ++ i)
   {
      // TODO: check dir and non-.so files
      LocalFS::copy(m_strBase + "/slave/sphere/" + i->m_strName, m_strHomeDir + "/.sphere/perm/" + i->m_strName);
   }

   INFO_MSG( "Scanning " << m_strHomeDir);
   m_pLocalFile = new Index;
   m_pLocalFile->init(m_strHomeDir + ".metadata");
   m_pLocalFile->scan(m_strHomeDir.c_str(), "/");

   m_SectorLog.insert("Sector slave started.");

   return 1;
}

int Slave::connect()
{
   // join the server
   SSLTransport::init();

   string cert = m_strBase + "/conf/master_node.cert";

   int64_t availdisk;
   LocalFS::get_dir_space(m_strHomeDir, availdisk);

   m_iSlaveID = -1;

//   string metafile = m_strHomeDir + ".tmp/metadata.dat";
   string metafile =  tmpnam(0) + string("metadata.dat");
   int ret = m_pLocalFile->serialize("/", metafile);
   if (ret) 
   {
     ERR_MSG( "Error writing metadata to a file " + metafile);
     return -1;
   }
   
   set<Address, AddrComp> masters;
   Address m;
   m.m_strIP = m_strMasterIP;
   m.m_iPort = m_iMasterPort;
   masters.insert(m);
   bool first = true;

   while (!masters.empty())
   {
      string mip = masters.begin()->m_strIP;
      int mport = masters.begin()->m_iPort;
      masters.erase(masters.begin());

      SSLTransport secconn;
      if ((secconn.initClientCTX(cert.c_str()) < 0) ||
         (secconn.open(NULL, 0) < 0) ||
         (secconn.connect(mip.c_str(), mport) < 0))
      {
         ERR_MSG( "Unable to set up secure channel to the master");
         return -1;
      }

      if (first)
      {
         secconn.getLocalIP(m_strLocalIP);

         // init data exchange channel
         m_iDataPort = 0;
         if (m_DataChn.init(m_strLocalIP, m_iDataPort) < 0)
         {
            ERR_MSG("Unable to create data channel");
            secconn.close();
            return -1;
         }
      }

      // send in the version first
      secconn.send((char*)&SectorVersion, 4);

      // slave join command type = 1
      int32_t cmd = 1;
      secconn.send((char*)&cmd, 4);

      //send information about home dir to the master, storage path is to differentiate multiple slaves on the same node
      int32_t size = m_strHomeDir.length() + 1;
      secconn.send((char*)&size, 4);
      secconn.send(m_strHomeDir.c_str(), size);

      int32_t res = -1;
      secconn.recv((char*)&res, 4);
      if (res < 0)
      {
         ERR_MSG("Slave join rejected. code: " << res);
         return res;
      }

      // send base dir, so that master can automatically restart this slave
      size = m_strBase.length() + 1;
      secconn.send((char*)&size, 4);
      secconn.send(m_strBase.c_str(), size);

      //send slave node information to the master
      secconn.send((char*)&m_iLocalPort, 4);
      secconn.send((char*)&m_iDataPort, 4);
      secconn.send((char*)&(availdisk), 8);
      secconn.send((char*)&(m_iSlaveID), 4);

      if (first)
         m_iSlaveID = res;

      //send local metadata
      SNode s;
      LocalFS::stat(metafile, s);
      size = s.m_llSize;
      secconn.send((char*)&size, 4);
      secconn.sendfile(metafile.c_str(), 0, size);

      if (!first)
      {
         secconn.close();
         continue;
      }

      // move out-of-date files to the ".attic" directory
      size = 0;
      secconn.recv((char*)&size, 4);

      if (size > 0)
      {
         string leftfile = tmpnam(0) + string(".metadata.left.dat");
         //string leftfile = m_strHomeDir + ".tmp/metadata.left.dat";
         int ret = secconn.recvfile(leftfile.c_str(), 0, size);
         if (ret < 0) 
         {
            ERR_MSG("Error receiving metadata file from master");
            return -1;
         }

         Metadata* attic = NULL;
         attic = new Index;
         attic->init(leftfile);
         attic->deserialize("/", leftfile, NULL);
         LocalFS::erase(leftfile);

         vector<string> fl;
         attic->list_r("/", fl);
         INFO_MSG("CONFLICT no of files: " << fl.size());
         for (vector<string>::iterator i = fl.begin(); i != fl.end(); ++ i)
         {
            // move it from local file system
            string dst_file = ".attic/" + *i;
            string dst_dir = dst_file.substr(0, dst_file.rfind('/'));
            createDir(dst_dir);
            if (LocalFS::rename(m_strHomeDir + *i, m_strHomeDir + dst_file)< 0)
            {
               ERR_MSG( "Error: failed to rename file " << m_strHomeDir + *i << " to " << m_strHomeDir + dst_file);
               perror("move file");
            }

            // remove it from the local metadata
            m_pLocalFile->remove(*i);

            INFO_MSG( "CONFLICT -> ATTIC: " << m_strHomeDir + *i << " " << m_strHomeDir + dst_file);
         }

         attic->clear();
         delete attic;

         m_SectorLog.insert("WARNING: certain files have been moved to ./attic due to conflicts.");
      }

      int id = 0;
      secconn.recv((char*)&id, 4);
      Address addr;
      addr.m_strIP = mip;
      addr.m_iPort = mport;
      m_Routing.insert(id, addr);

      int num;
      secconn.recv((char*)&num, 4);
      for (int i = 0; i < num; ++ i)
      {
         char ip[64];
         size = 0;
         secconn.recv((char*)&id, 4);
         secconn.recv((char*)&size, 4);
         secconn.recv(ip, size);
         addr.m_strIP = ip;
         secconn.recv((char*)&addr.m_iPort, 4);

         m_Routing.insert(id, addr);

         masters.insert(addr);
      }

      first = false;
      secconn.close();
   }

   SSLTransport::destroy();

   LocalFS::erase(metafile);

   // initialize slave statistics
   m_SlaveStat.init();

   INFO_MSG("This Sector slave is successfully initialized and running now");

   return 1;
}

void Slave::run()
{
   string ip;
   int port;
   int32_t id;
   SectorMsg* msg = new SectorMsg;
   msg->resize(65536);

   INFO_MSG( "Slave process: " << "GMP " << m_iLocalPort << " DATA " << m_DataChn.getPort());

   m_bRunning = true;

#ifndef WIN32
   pthread_t worker_thread;
   pthread_create(&worker_thread, NULL, worker, this);
#else
   DWORD ThreadID;
   HANDLE worker_thread = CreateThread(NULL, 0, worker, this, 0, &ThreadID);
#endif

#ifndef WIN32
   pthread_t delete_worker_thread;
   pthread_create(&delete_worker_thread, NULL, deleteWorker, this);
#else
   DWORD ThreadID;
   HANDLE delete_worker_thread = CreateThread(NULL, 0, deleteWorker, this, 0, &ThreadID);
#endif
   while (m_bRunning)
   {
      if (m_GMP.recvfrom(ip, port, id, msg) < 0)
         break;

      DBG_MSG( "Received cmd " << msg->getType() << " from "  << ip << ":" << port );

      // a slave only accepts commands from the masters
      Address addr;
      addr.m_strIP = ip;
      addr.m_iPort = port;
      if (m_Routing.getRouterID(addr) < 0)
         continue;

      switch (msg->getType() / 100)
      {
      case 0:
         processSysCmd(ip, port, id, msg);
         break;

      case 1:
         processFSCmd(ip, port, id, msg);
         break;

      case 2:
         processDCCmd(ip, port, id, msg);
         break;

      case 3:
         processDBCmd(ip, port, id, msg);
         break;

      case 10:
         processMCmd(ip, port, id, msg);
         break;

      #ifdef DEBUG
         processDebugCmd(ip, port, id, msg);
         break;
      #endif

      default:
         break;
      }
   }

   delete msg;

#ifndef WIN32
   pthread_join(worker_thread, NULL);
#else
   WaitForSingleObject(worker_thread, INFINITE);
#endif

   // TODO: check and cancel all file&spe threads

   INFO_MSG( "Slave is stopped by master");
}

void Slave::close()
{
   UDTTransport::release();
}

int Slave::processSysCmd(const string& ip, const int port, int id, SectorMsg* msg)
{
   switch (msg->getType())
   {
   case 1: // probe
   {
      m_GMP.sendto(ip, port, id, msg);
      break;
   }

   case 8: // stop
   {
      // stop the slave node
      m_bRunning = false;
      m_RunLock.acquire();
      m_RunCond.signal();
      m_RunLock.release();
      break;
   }

   default:
      return -1;
   }

   return 0;
}

int Slave::processFSCmd(const string& ip, const int port, int id, SectorMsg* msg)
{
   switch (msg->getType())
   {
   case 103: // mkdir
   {
      int r = createDir(msg->getData());
      if (r < 0 )
          ERR_MSG("Error creating new directory " << msg->getData());
      // TODO: update metatdata

      DBG_MSG ("Created new directory " << msg->getData());

      break;
   }

   case 104: // move dir/file
   {
      string src = msg->getData();
      string dst = msg->getData() + src.length() + 1;
      string newname = msg->getData() + src.length() + 1 + dst.length() + 1;

      src = Metadata::revisePath(src);
      dst = Metadata::revisePath(dst);
      newname = Metadata::revisePath(newname);

      m_pLocalFile->move(src.c_str(), dst.c_str(), newname.c_str());
      int r = createDir(dst);
      if (r < 0 )
          ERR_MSG("Error creating new directory in move " << dst);
      r = LocalFS::rename(m_strHomeDir + src, m_strHomeDir + dst + newname);
       if (r < 0 )
          ERR_MSG("Error moving file/directory " << m_strHomeDir + src << " " << m_strHomeDir + dst + newname);

      // TODO: check return value and acknowledge error to master.

      DBG_MSG( "Dir/file moved from " << src << " to " << dst << "/" << newname);

      break;
   }

   case 105: // remove dir/file
   {
      string path = Metadata::revisePath(msg->getData());
      m_pLocalFile->remove(path, true);

      DBG_MSG( "Delete enqueued - " << path);
      m_deleteLock.acquire();
      m_deleteQueue.push_back( m_strHomeDir + path );
      m_deleteCond.signal();
      m_deleteLock.release();

      break;
   }

   case 107: // utime
   {
      char* path = msg->getData();

      utimbuf ut;
      ut.actime = *(int64_t*)(msg->getData() + strlen(path) + 1);
      ut.modtime = *(int64_t*)(msg->getData() + strlen(path) + 1);;
      utime((m_strHomeDir + path).c_str(), &ut);

      DBG_MSG( "Dir/file " << path << " timestamp changed ");

      break;
   }

   case 110: // open file
   {
      Param2* p = new Param2;
      p->serv_instance = this;
      p->client_ip = msg->getData();
      p->client_port = *(int*)(msg->getData() + 64);
      p->key = *(int*)(msg->getData() + 136);
      p->mode = *(int*)(msg->getData() + 140);
      p->transid = *(int*)(msg->getData() + 144);
      memcpy(p->crypto_key, msg->getData() + 148, 16);
      memcpy(p->crypto_iv, msg->getData() + 164, 8);
      p->filename = msg->getData() + 172;

      p->master_ip = ip;
      p->master_port = port;

      // the slave must also lock the file IO. Because there are multiple master servers,
      // if one master is down, locks on this file may be lost when another master take over control of the file.
      if (m_pLocalFile->lock(p->filename, p->key, p->mode) < 0)
      {
         msg->setType(-msg->getType());
         msg->m_iDataLength = SectorMsg::m_iHdrSize;
         m_GMP.sendto(ip, port, id, msg);
         delete p;
         break;
      }

      INFO_MSG( "TID " << p->transid << " " << p->client_ip << ":" << p->client_port << " Opened file " << p->filename);

      m_TransManager.addSlave(p->transid, m_iSlaveID);

#ifndef WIN32
      pthread_t file_handler;
      pthread_create(&file_handler, NULL, fileHandler, p);
      pthread_detach(file_handler);
#else
      DWORD ThreadID;
      HANDLE file_handler = CreateThread(NULL, 0, fileHandler, p, NULL, &ThreadID);
#endif

      msg->m_iDataLength = SectorMsg::m_iHdrSize;
      m_GMP.sendto(ip, port, id, msg);

      break;
   }

   case 111: // create a replica to local
   {
      Param3* p = new Param3;
      p->serv_instance = this;
      p->transid = *(int32_t*)msg->getData();
      p->dir = *(int32_t*)(msg->getData() + 4);
      p->src = msg->getData() + 8;
      p->dst = msg->getData() + 8 + p->src.length() + 1;

      p->master_ip = ip;
      p->master_port = port;

      DBG_MSG("TID " << p->transid << " Creating replica " << p->src << " " << p->dst);

      m_TransManager.addSlave(p->transid, m_iSlaveID);

#ifndef WIN32
      pthread_t replica_handler;
      pthread_create(&replica_handler, NULL, copy, p);
      pthread_detach(replica_handler);
#else
      DWORD ThreadID;
      HANDLE replica_handler = CreateThread(NULL, 0, copy, p, NULL, &ThreadID);
#endif

      m_GMP.sendto(ip, port, id, msg);

      break;
   }

   default:
      return -1;
   }

   return 0;
}

int Slave::processDCCmd(const string& ip, const int port, int id, SectorMsg* msg)
{
   switch (msg->getType())
   {
   case 203: // processing engine
   {
      Param4* p = new Param4;
      p->serv_instance = this;
      p->client_ip = msg->getData();
      p->client_ctrl_port = *(int32_t*)(msg->getData() + 64);
      p->client_data_port = *(int32_t*)(msg->getData() + 68);
      p->speid = *(int32_t*)(msg->getData() + 72);
      p->key = *(int32_t*)(msg->getData() + 76);
      p->function = msg->getData() + 80;
      int offset = 80 + p->function.length() + 1;
      p->rows = *(int32_t*)(msg->getData() + offset);
      p->psize = *(int32_t*)(msg->getData() + offset + 4);
      if (p->psize > 0)
      {
         p->param = new char[p->psize];
         memcpy(p->param, msg->getData() + offset + 8, p->psize);
      }
      else
         p->param = NULL;
      p->type = *(int32_t*)(msg->getData() + msg->m_iDataLength - SectorMsg::m_iHdrSize - 8);
      p->transid = *(int32_t*)(msg->getData() + msg->m_iDataLength - SectorMsg::m_iHdrSize - 4);

      p->master_ip = ip;
      p->master_port = port;

      m_SectorLog << LogStart(LogLevel::LEVEL_3) << "starting SPE ... " << p->speid << " " << p->client_data_port << " " << p->function << " " << p->transid << LogEnd();

      m_TransManager.addSlave(p->transid, m_iSlaveID);

#ifndef WIN32
      pthread_t spe_handler;
      pthread_create(&spe_handler, NULL, SPEHandler, p);
      pthread_detach(spe_handler);
#else
      DWORD ThreadID;
      HANDLE spe_handler = CreateThread(NULL, 0, SPEHandler, p, NULL, &ThreadID);
#endif

      msg->m_iDataLength = SectorMsg::m_iHdrSize;
      m_GMP.sendto(ip, port, id, msg);

      break;
   }

   case 204: // accept SPE buckets
   {
      CGMP* gmp = new CGMP;
      gmp->init();

      Param5* p = new Param5;
      p->serv_instance = this;
      p->client_ip = msg->getData();
      p->client_ctrl_port = *(int32_t*)(msg->getData() + 64);
      p->bucketnum = *(int32_t*)(msg->getData() + 68);
      p->bucketid = *(int32_t*)(msg->getData() + 72);
      p->path = msg->getData() + 80;
      int offset = 80 + p->path.length() + 1 + 4;

      p->filename = msg->getData() + offset;
      p->gmp = gmp;

      offset += p->filename.length() + 1;

      p->key = *(int32_t*)(msg->getData() + offset);
      p->type = *(int32_t*)(msg->getData() + offset + 4);
      if (p->type == 1)
      {
         p->function = msg->getData() + offset + 4 + 4 + 4;
      }
      p->transid = *(int32_t*)(msg->getData() + msg->m_iDataLength - SectorMsg::m_iHdrSize - 8);
      p->client_data_port = *(int32_t*)(msg->getData() + msg->m_iDataLength - SectorMsg::m_iHdrSize - 4);

      p->master_ip = ip;
      p->master_port = port;

      m_SectorLog << LogStart(LogLevel::LEVEL_3) << "starting SPE Bucket ... " << p->filename << " " << p->key << " " << p->type << " " << p->transid << LogEnd();

      m_TransManager.addSlave(p->transid, m_iSlaveID);

#ifndef WIN32
      pthread_t spe_shuffler;
      pthread_create(&spe_shuffler, NULL, SPEShuffler, p);
      pthread_detach(spe_shuffler);
#else
      DWORD ThreadID;
      HANDLE spe_shuffler = CreateThread(NULL, 0, SPEShuffler, p, NULL, &ThreadID);
#endif

      *(int32_t*)msg->getData() = gmp->getPort();
      msg->m_iDataLength = SectorMsg::m_iHdrSize + 4;
      m_GMP.sendto(ip, port, id, msg);

      break;
   }

   default:
      return -1;
   }

   return 0;
}

int Slave::processDBCmd(const string& /*ip*/, const int /*port*/, int /*id*/, SectorMsg* msg)
{
   switch (msg->getType())
   {
   case 301: // create a new table

   case 302: // add a new attribute

   case 303: // delete an attribute

   case 304: // delete a table

   default:
      return -1;
   }

   return 0;
}

int Slave::processMCmd(const string& ip, const int port, int id, SectorMsg* msg)
{
   switch (msg->getType())
   {
   case 1001: // new master
   {
      int32_t key = *(int32_t*)msg->getData();
      Address addr;
      addr.m_strIP = msg->getData() + 4;
      addr.m_iPort = *(int32_t*)(msg->getData() + 68);
      m_Routing.insert(key, addr);
      msg->m_iDataLength = SectorMsg::m_iHdrSize + 4;
      m_GMP.sendto(ip, port, id, msg);
      INFO_MSG("New master " << addr.m_strIP << ":" << addr.m_iPort);
      break;
   }

   case 1006: // master lost
   {
      m_Routing.remove(*(int32_t*)msg->getData());
      msg->m_iDataLength = SectorMsg::m_iHdrSize + 4;
      m_GMP.sendto(ip, port, id, msg);
      
      ERR_MSG("Master lost ");
      break;
   }

   default:
      ERR_MSG("Bad MC cmd " << msg->getType());
      return -1; 
   }

   return 0;
}

#ifdef DEBUG
int Slave::processDebugCmd(const string& /*ip*/, const int /*port*/, int /*id*/, SectorMsg* msg)
{
   switch (msg->getType())
   {
   case 9901: // enable pseudo disk error
      m_bDiskHealth = false;
      break;

   case 9902: // enable pseudo network error
      m_bNetworkHealth = false;
      break;

   default:
      return -1;
   }

   return 0;
}
#endif

int Slave::report(const string& master_ip, const int& master_port, const int32_t& transid, const string& filename, const int32_t& change)
{
   vector<string> filelist;

   if (change > 0)
   {
      SNode s;
      if (LocalFS::stat(m_strHomeDir + filename, s) >= 0)
      {
         filelist.push_back(filename);

         if (s.m_bIsDir)
            getFileList(filename, filelist);
      }
      else
      {
         //log error
      }
   }

   return report(master_ip, master_port, transid, filelist, change);
}

int Slave::getFileList(const string& path, vector<string>& filelist)
{
   vector<SNode> sl;
   if (LocalFS::list_dir(m_strHomeDir + path, sl) < 0)
      return -1;

   for (vector<SNode>::iterator i = sl.begin(); i != sl.end(); ++ i)
   {
      filelist.push_back(path + "/" + i->m_strName);

      if (i->m_bIsDir)
      {
         getFileList(path + "/" + i->m_strName, filelist);
      }
   }

   return filelist.size();
}

int Slave::report(const string& master_ip, const int& master_port, const int32_t& transid, const vector<string>& filelist, const int32_t& change)
{
   vector<string> serlist;
   if( change != FileChangeType::FILE_UPDATE_NO && change != FileChangeType::FILE_UPDATE_NEW_FAILED && change != FileChangeType::FILE_UPDATE_WRITE_FAILED && change != FileChangeType::FILE_UPDATE_REPLICA_FAILED )
   {
      for (vector<string>::const_iterator i = filelist.begin(); i != filelist.end(); ++ i)
      {
         SNode sn;
         if (LocalFS::stat(m_strHomeDir + *i, sn) < 0)
            continue;

         // IMPORTANT: this name must be full path so that both local index and master index can be updated properly
         sn.m_strName = *i;

         if (change == FileChangeType::FILE_UPDATE_WRITE)
         {
            // file may be created on write; in this case, create a new meta entry instead of update non-existing one
            if (m_pLocalFile->update(sn.m_strName, sn.m_llTimeStamp, sn.m_llSize) < 0)
               m_pLocalFile->create(sn);
         }
         else if (change == FileChangeType::FILE_UPDATE_NEW)
            m_pLocalFile->create(sn);
         else if (change == FileChangeType::FILE_UPDATE_REPLICA)
            m_pLocalFile->create(sn);

         char* buf = NULL;
         sn.serialize(buf);
         serlist.push_back(buf);
         delete [] buf;
      }
   }
   else if( change == FileChangeType::FILE_UPDATE_REPLICA_FAILED )
   {
      for (vector<string>::const_iterator i = filelist.begin(); i != filelist.end(); ++ i)
      {
         SNode sn;

         // IMPORTANT: this name must be full path so that both local index and master index can be updated properly
         sn.m_strName = *i;
         sn.m_bIsDir = false;
         sn.m_llTimeStamp = 0;
         sn.m_llSize = 0;
         sn.m_iReplicaNum = 0;
         sn.m_iReplicaDist = 0;

         char* buf = NULL;
         sn.serialize(buf);
         serlist.push_back(buf);
         delete [] buf;
      }
   }

   SectorMsg msg;
   msg.setType(1);
   msg.setKey(0);
   msg.setData(0, (char*)&transid, 4);
   msg.setData(4, (char*)&m_iSlaveID, 4);
   msg.setData(8, (char*)&change, 4);
   int32_t num = serlist.size();
   msg.setData(12, (char*)&num, 4);
   int pos = 16;
   for (vector<string>::iterator i = serlist.begin(); i != serlist.end(); ++ i)
   {
      int32_t bufsize = i->length() + 1;
      msg.setData(pos, (char*)&bufsize, 4);
      msg.setData(pos + 4, i->c_str(), bufsize);
      pos += bufsize + 4;
   }

   //TODO: if the current master is down, try a different master
   if (m_GMP.rpc(master_ip.c_str(), master_port, &msg, &msg) < 0)
      return -1;

   if (msg.getType() < 0)
      return *(int32_t*)msg.getData();

   return 1;
}

int Slave::reportMO(const string& master_ip, const int& master_port, const int32_t& transid)
{
   vector<MemObj> tba;
   vector<string> tbd;
   if (m_InMemoryObjects.update(tba, tbd) <= 0)
      return 0;

   if (!tba.empty())
   {
      vector<string> serlist;
      for (vector<MemObj>::const_iterator i = tba.begin(); i != tba.end(); ++ i)
      {
         SNode sn;
         sn.m_strName = i->m_strName;
         sn.m_bIsDir = 0;
         sn.m_llTimeStamp = i->m_llCreationTime;
         sn.m_llSize = 8;

         char* buf = NULL;
         sn.serialize(buf);
         serlist.push_back(buf);
         delete [] buf;
      }

      SectorMsg msg;
      msg.setType(1);
      msg.setKey(0);
      msg.setData(0, (char*)&transid, 4);
      msg.setData(4, (char*)&m_iSlaveID, 4);
      int32_t num = serlist.size();
      msg.setData(8, (char*)&num, 4);
      int pos = 12;
      for (vector<string>::iterator i = serlist.begin(); i != serlist.end(); ++ i)
      {
         int32_t bufsize = i->length() + 1;
         msg.setData(pos, (char*)&bufsize, 4);
         msg.setData(pos + 4, i->c_str(), bufsize);
         pos += bufsize + 4;
      }

      if (m_GMP.rpc(master_ip.c_str(), master_port, &msg, &msg) < 0)
         return -1;
   }

   if (!tbd.empty())
   {
      SectorMsg msg;
      msg.setType(7);
      msg.setKey(0);
      msg.setData(0, (char*)&transid, 4);
      msg.setData(4, (char*)&m_iSlaveID, 4);
      int32_t num = tbd.size();
      msg.setData(8, (char*)&num, 4);
      int pos = 12;
      for (vector<string>::iterator i = tbd.begin(); i != tbd.end(); ++ i)
      {
         int32_t bufsize = i->length() + 1;
         msg.setData(pos, (char*)&bufsize, 4);
         msg.setData(pos + 4, i->c_str(), bufsize);
         pos += bufsize + 4;
      }

      if (m_GMP.rpc(master_ip.c_str(), master_port, &msg, &msg) < 0)
         return -1;
   }

   return 0;
}

int Slave::reportSphere(const string& master_ip, const int& master_port, const int& transid, const vector<Address>* bad)
{
   SectorMsg msg;
   msg.setType(4);
   msg.setKey(0);
   msg.setData(0, (char*)&transid, 4);
   msg.setData(4, (char*)&m_iSlaveID, 4);

   int num = (NULL == bad) ? 0 : bad->size();
   msg.setData(8, (char*)&num, 4);
   for (int i = 0; i < num; ++ i)
   {
      msg.setData(12 + 68 * i, (*bad)[i].m_strIP.c_str(), (*bad)[i].m_strIP.length() + 1);
      msg.setData(12 + 68 * i + 64, (char*)&((*bad)[i].m_iPort), 4);
   }

   if (m_GMP.rpc(master_ip.c_str(), master_port, &msg, &msg) < 0)
      return -1;

   if (msg.getType() < 0)
      return *(int32_t*)msg.getData();

   return 1;
}

int Slave::createDir(const string& path)
{
   vector<string> dir;
   Metadata::parsePath(path.c_str(), dir);

   string currpath = m_strHomeDir;
   for (vector<string>::iterator i = dir.begin(); i != dir.end(); ++ i)
   {
      currpath += *i;
      if ((LocalFS::mkdir(currpath) < 0) && (errno != EEXIST))
         return -1;
      currpath += "/";
   }

   return 1;
}

int Slave::createSysDir()
{
   // check local directory
   SNode s;
   if ((LocalFS::stat(m_strHomeDir, s) < 0) || !s.m_bIsDir)
   {
      if (errno != ENOENT)
         return -1;

      vector<string> dir;
      Metadata::parsePath(m_strHomeDir.c_str(), dir);

      string currpath = "/";
      for (vector<string>::iterator i = dir.begin(); i != dir.end(); ++ i)
      {
         currpath += *i;
         if (LocalFS::mkdir(currpath) < 0)
            return -1;
         currpath += "/";
      }
   }

   if (LocalFS::mkdir(m_strHomeDir + ".metadata") < 0)
      return -1;
   LocalFS::clean_dir(m_strHomeDir + ".metadata");

   if (LocalFS::mkdir(m_strHomeDir + ".log") < 0)
      return -1;

   if (LocalFS::mkdir(m_strHomeDir + ".sphere") < 0)
      return -1;
   //LocalFS::clean_dir(m_strHomeDir + ".sphere");
   if (LocalFS::mkdir(m_strHomeDir + ".sphere/perm") < 0)
      return -1;

   if (LocalFS::mkdir(m_strHomeDir + ".tmp") < 0)
      return -1;
   LocalFS::clean_dir(m_strHomeDir + ".tmp");

   if (LocalFS::mkdir(m_strHomeDir + ".attic") < 0)
      return -1;
   //TODO: check slave.conf option to decide if to clean .attic
   //LocalFS::clean_dir(m_strHomeDir + ".attic");
   if (LocalFS::mkdir(m_strHomeDir + ".recylcebin") < 0)
      return -1;

   return 0;
}

void SlaveStat::init()
{
   m_llStartTime = m_llTimeStamp = CTimer::getTime();
   m_llCurrMemUsed = 0;
   m_llCurrCPUUsed = 0;
   m_llTotalInputData = 0;
   m_llTotalOutputData = 0;
   m_mSysIndInput.clear();
   m_mSysIndOutput.clear();
   m_mCliIndInput.clear();
   m_mCliIndOutput.clear();
}

void SlaveStat::refresh()
{
   m_llTimeStamp = CTimer::getTime();

#ifndef WIN32
   // THIS CODE IS FOR LINUX ONLY. NOT PORTABLE

   m_llCurrMemUsed = 0;

   int pid = getpid();
   char memfile[64];
   sprintf(memfile, "/proc/%d/status", pid);
   ifstream ifs;
   ifs.open(memfile, ios::in);
   if (!ifs.fail())
   {
      // this looks for the VmSize line in the process status file
      char buf[1024];
      buf[0] = '\0';
      while (!ifs.eof())
      {
         ifs.getline(buf, 1024);
         if (string(buf).substr(0, 6) == "VmSize")
            break;
      }
      string tmp;
      stringstream ss;
      ss.str(buf);
      ss >> tmp >> m_llCurrMemUsed;
      m_llCurrMemUsed *= 1024;
      ifs.close();
   }

   clock_t hz = sysconf(_SC_CLK_TCK);
   tms cputime;
   times(&cputime);
   m_llCurrCPUUsed = (cputime.tms_utime + cputime.tms_stime) * 1000000LL / hz;
#else
   PROCESS_MEMORY_COUNTERS pmc;
   GetProcessMemoryInfo(::GetCurrentProcess(), &pmc, sizeof(pmc));
   m_llCurrMemUsed = pmc.WorkingSetSize;

   FILETIME kernel, user;
   GetProcessTimes(::GetCurrentProcess(), NULL, NULL, &kernel, &user);
   ULARGE_INTEGER k, u;
   k.LowPart = kernel.dwLowDateTime;
   k.HighPart = kernel.dwHighDateTime;
   u.LowPart = user.dwLowDateTime;
   u.HighPart = user.dwHighDateTime;
   m_llCurrCPUUsed = (k.QuadPart + u.QuadPart) / 10000000;
#endif
}

void SlaveStat::updateIO(const string& ip, const int64_t& size, const int& type)
{
   CGuardEx sg(m_StatLock);

   map<string, int64_t>::iterator a;

   if (type == SYS_IN)
   {
      map<string, int64_t>::iterator a = m_mSysIndInput.find(ip);
      if (a == m_mSysIndInput.end())
      {
         m_mSysIndInput[ip] = 0;
         a = m_mSysIndInput.find(ip);
      }

      a->second += size;
      m_llTotalInputData += size;
   }
   else if (type == SYS_OUT)
   {
      map<string, int64_t>::iterator a = m_mSysIndOutput.find(ip);
      if (a == m_mSysIndOutput.end())
      {
         m_mSysIndOutput[ip] = 0;
         a = m_mSysIndOutput.find(ip);
      }

      a->second += size;
      m_llTotalOutputData += size;
   }
   else if (type == CLI_IN)
   {
      map<string, int64_t>::iterator a = m_mCliIndInput.find(ip);
      if (a == m_mCliIndInput.end())
      {
         m_mCliIndInput[ip] = 0;
         a = m_mCliIndInput.find(ip);
      }

      a->second += size;
      m_llTotalInputData += size;
   }
   else if (type == CLI_OUT)
   {
      map<string, int64_t>::iterator a = m_mCliIndOutput.find(ip);
      if (a == m_mCliIndOutput.end())
      {
         m_mCliIndOutput[ip] = 0;
         a = m_mCliIndOutput.find(ip);
      }

      a->second += size;
      m_llTotalOutputData += size;
   }
}

int SlaveStat::serializeIOStat(char*& buf, int& size)
{
   size = (m_mSysIndInput.size() + m_mSysIndOutput.size() + m_mCliIndInput.size() + m_mCliIndOutput.size()) * 24 + 16;
   buf = new char[size];

   CGuardEx sg(m_StatLock);

   char* p = buf;
   *(int32_t*)p = m_mSysIndInput.size();
   p += 4;
   for (map<string, int64_t>::iterator i = m_mSysIndInput.begin(); i != m_mSysIndInput.end(); ++ i)
   {
      strcpy(p, i->first.c_str());
      *(int64_t*)(p + 16) = i->second;
      p += 24;
   }

   *(int32_t*)p = m_mSysIndOutput.size();
   p += 4;
   for (map<string, int64_t>::iterator i = m_mSysIndOutput.begin(); i != m_mSysIndOutput.end(); ++ i)
   {
      strcpy(p, i->first.c_str());
      *(int64_t*)(p + 16) = i->second;
      p += 24;
   }

   *(int32_t*)p = m_mCliIndInput.size();
   p += 4;
   for (map<string, int64_t>::iterator i = m_mCliIndInput.begin(); i != m_mCliIndInput.end(); ++ i)
   {
      strcpy(p, i->first.c_str());
      *(int64_t*)(p + 16) = i->second;
      p += 24;
   }

   *(int32_t*)p = m_mCliIndOutput.size();
   p += 4;
   for (map<string, int64_t>::iterator i = m_mCliIndOutput.begin(); i != m_mCliIndOutput.end(); ++ i)
   {
      strcpy(p, i->first.c_str());
      *(int64_t*)(p + 16) = i->second;
      p += 24;
   }

   return (m_mSysIndInput.size() + m_mSysIndOutput.size() + m_mCliIndInput.size() + m_mCliIndOutput.size()) * 24 + 16;
}

#ifndef WIN32
void* Slave::worker(void* param)
#else
DWORD WINAPI Slave::worker(LPVOID param)
#endif
{
   Slave* self = (Slave*)param;

   int64_t last_report_time = CTimer::getTime();
   int64_t last_gc_time = CTimer::getTime();
   srandomdev();

   while (self->m_bRunning)
   {
      // report every 30 - 60 seconds
      int wait_time = 30 + rand() % 30; // seconds
      self->m_RunCond.wait(self->m_RunLock, wait_time * 1000);

      // report to master every half minute
      if (CTimer::getTime() - last_report_time < 30000000)
         continue;

      // get total available disk size and file size
      LocalFS::get_dir_space(self->m_strHomeDir.c_str(), self->m_SlaveStat.m_llAvailSize);
      // Check local disk health.
      fstream test;
      test.open((self->m_strHomeDir + "/.test").c_str(), ios::in | ios::out | ios::binary | ios::trunc);
      if (test.fail())
         self->m_SlaveStat.m_llAvailSize = 0;
      test.close();
      LocalFS::erase(self->m_strHomeDir + "/.test");
      self->m_SlaveStat.m_llDataSize = self->m_pLocalFile->getTotalDataSize("/");
      // users may limit the maximum disk size used by Sector
      if (self->m_SysConfig.m_llMaxDataSize >= 0)
      {
         int64_t avail_limit = self->m_SysConfig.m_llMaxDataSize - self->m_SlaveStat.m_llDataSize;

         if (avail_limit < 0)
            avail_limit = 0;
         if (avail_limit < self->m_SlaveStat.m_llAvailSize)
            self->m_SlaveStat.m_llAvailSize = avail_limit;
      }

      self->m_SlaveStat.refresh();

      SectorMsg msg;
      msg.setType(10);
      msg.setKey(0);
      msg.setData(0, (char*)&(self->m_SlaveStat.m_llTimeStamp), 8);
      msg.setData(8, (char*)&(self->m_SlaveStat.m_llAvailSize), 8);
      msg.setData(16, (char*)&(self->m_SlaveStat.m_llDataSize), 8);
      msg.setData(24, (char*)&(self->m_SlaveStat.m_llCurrMemUsed), 8);
      msg.setData(32, (char*)&(self->m_SlaveStat.m_llCurrCPUUsed), 8);
      msg.setData(40, (char*)&(self->m_SlaveStat.m_llTotalInputData), 8);
      msg.setData(48, (char*)&(self->m_SlaveStat.m_llTotalOutputData), 8);

      char* buf = NULL;
      int size = 0;
      self->m_SlaveStat.serializeIOStat(buf, size);
      msg.setData(56, buf, size);
      delete [] buf;

      map<uint32_t, Address> al;
      self->m_Routing.getListOfMasters(al);

      for (map<uint32_t, Address>::iterator i = al.begin(); i != al.end(); ++ i)
      {
         int id = 0;
         self->m_GMP.sendto(i->second.m_strIP, i->second.m_iPort, id, &msg);
      }

      last_report_time = CTimer::getTime();

      // clean broken data channels every hour
      if (CTimer::getTime() - last_gc_time > 3600000000LL)
      {
         self->m_DataChn.garbageCollect();
         last_gc_time = CTimer::getTime();
      }
   }

   return NULL;
}


#ifndef WIN32
void* Slave::deleteWorker(void* param)
#else
DWORD WINAPI Slave::deleteWorker(LPVOID param)
#endif
{
   Slave* self = (Slave*)param;

   while (self->m_bRunning)
   {   
      self->m_deleteLock.acquire();
      self->m_deleteCond.wait( self->m_deleteLock, 1000 ); 

      if( !self->m_deleteQueue.empty() )
      {
        std::string path = self->m_deleteQueue.front();
        self->m_deleteQueue.pop_front();
        self->m_deleteLock.release();
        self->m_SectorLog << LogStart(LogLevel::LEVEL_9) << "Delete dequeued - " << path << LogEnd();
        int rc = LocalFS::rmdir( path );
        if( rc < 0 )
          self->m_SectorLog << LogStart(LogLevel::LEVEL_1) << "Delete complete FAIL - " << path << ", rc = " << rc << LogEnd();
        else
          self->m_SectorLog << LogStart(LogLevel::LEVEL_9) << "Delete complete  - " << path << LogEnd();
      }
      else
        self->m_deleteLock.release();
   }

   return NULL;
}

