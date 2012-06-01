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
   Yunhong Gu, last updated 04/14/2011
*****************************************************************************/

#ifndef WIN32
   #include <utime.h>
#else
   #include <sys/types.h>
   #include <sys/utime.h>
#endif

#define ERR_MSG( msg ) \
{\
   self->m_SectorLog << LogStart(LogLevel::LEVEL_1) << "TID " << transid << " " << \
     client_ip << ":" << client_port << " cmd " << cmd << " " << filename << " " << msg << LogEnd(); \
}

#define INFO_MSG( msg ) \
{\
   self->m_SectorLog << LogStart(LogLevel::LEVEL_3) << "TID " << transid << " " << \
     client_ip << ":" << client_port << " cmd " << cmd << " " << filename << " " << msg << LogEnd(); \
}

#define DBG_MSG( msg ) \
{\
   self->m_SectorLog << LogStart(LogLevel::LEVEL_9) << "TID " << transid << " " << \
     client_ip << ":" << client_port << " cmd " << cmd << " " << filename << " " << msg << LogEnd(); \
}

#define DBG_REP( msg ) \
{\
  self->m_SectorLog << LogStart(LogLevel::LEVEL_9) << "TID " << transid << " file " << src << " " <<  msg \
   << LogEnd(); \
}

#include "slave.h"
#include "writelog.h"

using namespace std;
using namespace sector;

#ifndef WIN32
void* Slave::fileHandler(void* p)
#else
DWORD WINAPI Slave::fileHandler(LPVOID p)
#endif
{
   Slave* self = ((Param2*)p)->serv_instance;
   string filename = self->m_strHomeDir + ((Param2*)p)->filename;
   string sname = ((Param2*)p)->filename;
   int key = ((Param2*)p)->key;
   int mode = ((Param2*)p)->mode;
   int transid = ((Param2*)p)->transid;
   string client_ip = ((Param2*)p)->client_ip;
   int client_port = ((Param2*)p)->client_port;
   unsigned char crypto_key[16];
   unsigned char crypto_iv[8];
   memcpy(crypto_key, ((Param2*)p)->crypto_key, 16);
   memcpy(crypto_iv, ((Param2*)p)->crypto_iv, 8);
   string master_ip = ((Param2*)p)->master_ip;
   int master_port = ((Param2*)p)->master_port;
   delete (Param2*)p;

   // uplink and downlink addresses for write, no need for read
   string src_ip = client_ip;
   int src_port = client_port;
   string dst_ip;
   int dst_port = -1;

   // IO permissions
   bool bRead = mode & 1;
   bool bWrite = mode & 2;
   bool bTrunc = mode & 4;
   bool bSecure = mode & 16;

   int64_t orig_size = -1;
   int64_t orig_ts = -1;
   bool file_change = false;

   int last_timestamp = 0;

   int32_t cmd = 0;

   int reads = 0;
   int writes = 0;

   INFO_MSG ("Connecting");

   if ((!self->m_DataChn.isConnected(client_ip, client_port)) && (self->m_DataChn.connect(client_ip, client_port) < 0))
   {
      ERR_MSG("Failed to connect to client");

      // release transactions and file locks
      self->m_TransManager.updateSlave(transid, self->m_iSlaveID);
      self->m_pLocalFile->unlock(sname, key, mode);
      self->report(master_ip, master_port, transid, sname, +FileChangeType::FILE_UPDATE_NO);

      return NULL;
   }

   Crypto* encoder = NULL;
   Crypto* decoder = NULL;
   if (bSecure)
   {
      encoder = new Crypto;
      encoder->initEnc(crypto_key, crypto_iv);
      decoder = new Crypto;
      decoder->initDec(crypto_key, crypto_iv);      
   }

   //create a new directory or file in case it does not exist
   if (bWrite)
   {
      self->createDir(sname.substr(0, sname.rfind('/')));

      SNode s;
      if (LocalFS::stat(filename, s) < 0)
      {
         ofstream newfile(filename.c_str(), ios::out | ios::binary | ios::trunc);
         if (!newfile) 
         {
            ERR_MSG("Cannot create new file");
         }
         newfile.close();
      }
      else
      {
         orig_size = s.m_llSize;
         orig_ts = s.m_llTimeStamp;
      }
   }

   timeval t1, t2;
   gettimeofday(&t1, 0);
   int64_t rb = 0;
   int64_t wb = 0;

   WriteLog writelog;

   fstream fhandle;
   if (!bTrunc)
      fhandle.open(filename.c_str(), ios::in | ios::out | ios::binary);
   else
      fhandle.open(filename.c_str(), ios::in | ios::out | ios::binary | ios::trunc);

   if (!fhandle) 
      ERR_MSG("Error opening file");

   // a file session is successful only if the client issue a close() request
   bool success = true;
   bool run = true;

   while (run)
   {
      if (self->m_DataChn.recv4(client_ip, client_port, transid, cmd) < 0)
      {
         ERR_MSG("Connection broken");
         break;
      }

      switch (cmd)
      {
      case 1: // read
         {
            reads = reads + 1;
            char* param = NULL;
            int tmp = 8 * 2;
            if (self->m_DataChn.recv(client_ip, client_port, transid, param, tmp) < 0)
            {
               success = false;
               ERR_MSG("Error receiving command in read ");
               break;
            }
            int64_t offset = *(int64_t*)param;
            int64_t size = *(int64_t*)(param + 8);
            delete [] param;

            int32_t response = bRead ? 0 : -1;
            if (fhandle.fail() || !success || !self->m_bDiskHealth || !self->m_bNetworkHealth)
            {
               ERR_MSG("Returning error on file error - disk health - network health - error");
               response = -1;
            }

            if (response == -1)
               ERR_MSG("Sending response -1");

            if (self->m_DataChn.send(client_ip, client_port, transid, (char*)&response, 4) < 0)
            {
               ERR_MSG( "Error sending response in read");
               break;
            }
            if (response == -1)
               break;

            if (self->m_DataChn.sendfile(client_ip, client_port, transid, fhandle, offset, size, encoder) < 0)
            {
               ERR_MSG( "Error reading and sending in read");
               success = false;
            }
            else
               rb += size;

            // update total sent data size
            self->m_SlaveStat.updateIO(client_ip, param[1], (key == 0) ? +SlaveStat::SYS_OUT : +SlaveStat::CLI_OUT);
            if (reads < 4) // logging first 3 reads
               DBG_MSG("Read offset " << offset << " size " << size);

            break;
         }

      case 2: // write
         {
            writes = writes + 1;
            if (!bWrite)
            {
               // if the client does not have write permission, disconnect it immediately
               success = false;
               ERR_MSG("Client does not have write permission");
               break;
            }

            //receive offset and size information from uplink
            char* param = NULL;
            int tmp = 8 * 2;
            if (self->m_DataChn.recv(src_ip, src_port, transid, param, tmp) < 0)
            {
               ERR_MSG("Error receiving command in write");
               break;
            }
            int64_t offset = *(int64_t*)param;
            int64_t size = *(int64_t*)(param + 8);
            delete [] param;

            // no secure transfer between two slaves
            Crypto* tmp_decoder = decoder;
            if ((client_ip != src_ip) || (client_port != src_port))
               tmp_decoder = NULL;

            bool io_status = (size > 0); 
            if (!io_status || (self->m_DataChn.recvfile(src_ip, src_port, transid, fhandle, offset, size, tmp_decoder) < size))
            {
               ERR_MSG("Error receiving and writing in write");
               io_status = false;
            }

            //TODO: send incomplete write to next slave on chain, rather than -1

            if (dst_port > 0)
            {
               // send offset and size parameters
               char req[16];
               *(int64_t*)req = offset;
               if (io_status)
                  *(int64_t*)(req + 8) = size;
               else
                  *(int64_t*)(req + 8) = -1;
               self->m_DataChn.send(dst_ip, dst_port, transid, req, 16);

               // send the data to the next replica in the chain
               if (size > 0)
                  self->m_DataChn.sendfile(dst_ip, dst_port, transid, fhandle, offset, size);
            }

            if (!io_status)
               break;

            wb += size;

            // update total received data size
            self->m_SlaveStat.updateIO(src_ip, size, (key == 0) ? +SlaveStat::SYS_IN : +SlaveStat::CLI_IN);

            // update write log
            writelog.insert(offset, size);
            file_change = true;
            if (writes < 4) // logging first 3 writes
               DBG_MSG("Write offset " << offset << " size " << size);    

            break;
         }

      case 3: // download
         {
            int64_t offset;
            if (self->m_DataChn.recv8(client_ip, client_port, transid, offset) < 0)
            {
               success = false;
               ERR_MSG("Error receiving parameters in download");
               break;
            }

            int32_t response = bRead ? 0 : -1;
            if (fhandle.fail() || !success || !self->m_bDiskHealth || !self->m_bNetworkHealth)
            {
               ERR_MSG("File - disk - network error");
               response = -1;
            }
            if (self->m_DataChn.send(client_ip, client_port, transid, (char*)&response, 4) < 0)
            {
               ERR_MSG("Error sendong responce");
               break;
            }
            if (response == -1)
               break;

            fhandle.seekg(0, ios::end);
            int64_t size = (int64_t)(fhandle.tellg());
            fhandle.seekg(0, ios::beg);

            size -= offset;

            int64_t unit = 64000000; //send 64MB each time
            int64_t tosend = size;
            int64_t sent = 0;
            while (tosend > 0)
            {
               int64_t block = (tosend < unit) ? tosend : unit;
               if (self->m_DataChn.sendfile(client_ip, client_port, transid, fhandle, offset + sent, block, encoder) < 0)
               {
                  success = false;
                  ERR_MSG("Error in sending data from file");
                  break;
               }

               sent += block;
               tosend -= block;
            }
            rb += sent;

            // update total sent data size
            self->m_SlaveStat.updateIO(client_ip, size, (key == 0) ? +SlaveStat::SYS_OUT : +SlaveStat::CLI_OUT);

            break;
         }

      case 4: // upload
         {
            if (!bWrite)
            {
               // if the client does not have write permission, disconnect it immediately
               success = false;
               ERR_MSG("Client does not have write permission - disconnect"); 
               break;
            }

            int64_t offset = 0;
            int64_t size;
            if (self->m_DataChn.recv8(client_ip, client_port, transid, size) < 0)
            {
               success = false;
               ERR_MSG("Bad size in upload");
               break;
            }

            //TODO: check available size
            int32_t response = 0;
            if (fhandle.fail() || !success || !self->m_bDiskHealth || !self->m_bNetworkHealth)
            {
               ERR_MSG("File - disk - network error");
               response = -1;
            }
            if (self->m_DataChn.send(client_ip, client_port, transid, (char*)&response, 4) < 0)
            {
               ERR_MSG("Error sending responce");
               break;
            }
            if (response == -1)
               break;

            int64_t unit = 64000000; //send 64MB each time
            int64_t torecv = size;
            int64_t recd = 0;

            // no secure transfer between two slaves
            Crypto* tmp_decoder = decoder;
            if ((client_ip != src_ip) || (client_port != src_port))
               tmp_decoder = NULL;

            while (torecv > 0)
            {
               int64_t block = (torecv < unit) ? torecv : unit;

               if (self->m_DataChn.recvfile(src_ip, src_port, transid, fhandle, offset + recd, block, tmp_decoder) < 0)
               {
                  success = false;
                  ERR_MSG( "Error receiving file in upload");
                  break;
               }

               if (dst_port > 0)
               {
                  // write to uplink for next replica in the chain
                  if (self->m_DataChn.sendfile(dst_ip, dst_port, transid, fhandle, offset + recd, block) < 0)
                  {
                     ERR_MSG("Error sending to next replica in upload chain"); 
                     break;
                  }
               }

               recd += block;
               torecv -= block;
            }
            wb += recd;

            // update total received data size
            self->m_SlaveStat.updateIO(src_ip, size, (key == 0) ? +SlaveStat::SYS_IN : +SlaveStat::CLI_IN);

            // update write log
            writelog.insert(0, size);
            file_change = true;

            break;
         }

      case 5: // end session
         // the file has been successfully closed
         run = false;
         DBG_MSG("Sesion end");
         break;

      case 6: // read file path for local IO optimization
         self->m_DataChn.send(client_ip, client_port, transid, self->m_strHomeDir.c_str(), self->m_strHomeDir.length() + 1);
         DBG_MSG("Send to client path in local IO optimization");
         break;

      case 7: // synchronize with the client, make sure write is correct
      {
         //TODO: merge all three recv() to one
         int32_t size = 0;
         if (self->m_DataChn.recv4(client_ip, client_port, transid, size) < 0)
         {
            ERR_MSG("Error getting size in synchronize");
            break;
         }
         char* buf = NULL;
         if (self->m_DataChn.recv(client_ip, client_port, transid, buf, size) < 0)
         {
            ERR_MSG("Error receiving par 2");
            break;
         }
         last_timestamp = 0;
         if (self->m_DataChn.recv4(client_ip, client_port, transid, last_timestamp) < 0)
         {
            ERR_MSG("Error receiving par 3");
            break;
         }

         WriteLog log;
         log.deserialize(buf, size);
         delete [] buf;

         int32_t confirm = -1;
         if (writelog.compare(log))
            confirm = 1;

         writelog.clear();

         if (confirm > 0)
         {
            //synchronize timestamp
            utimbuf ut;
            ut.actime = last_timestamp;
            ut.modtime = last_timestamp;
            utime(filename.c_str(), &ut);
         }

         if (self->m_DataChn.send(client_ip, client_port, transid, (char*)&confirm, 4) < 0)
            ERR_MSG("Error sending confirm");
         break;
      }

      case 8: // specify up and down links
      {
         char* buf = NULL;
         int size = 136;
         if (self->m_DataChn.recv(client_ip, client_port, transid, buf, size) < 0)
         {
            ERR_MSG("Error geting parameters");
            break;
         }

         int32_t response = bWrite ? 0 : -1;
         if (fhandle.fail() || !success || !self->m_bDiskHealth || !self->m_bNetworkHealth)
         {
            ERR_MSG("File - disk - network error"); 
            response = -1;
         }
         if (self->m_DataChn.send(client_ip, client_port, transid, (char*)&response, 4) < 0)
         {
            ERR_MSG("Error sending response");
            break;
         }
         if (response == -1)
            break;

         src_ip = buf;
         src_port = *(int32_t*)(buf + 64);
         dst_ip = buf + 68;
         dst_port = *(int32_t*)(buf + 132);
         delete [] buf;

         if (src_port > 0)
         {
            // connect to uplink in the write chain
            if (!self->m_DataChn.isConnected(src_ip, src_port))
               self->m_DataChn.connect(src_ip, src_port);
         }
         else
         {
            // first node in the chain, read from client
            src_ip = client_ip;
            src_port = client_port;
         }
         
         if (dst_port > 0)
         {
            //connect downlink in the write chain
            if (!self->m_DataChn.isConnected(dst_ip, dst_port))
               self->m_DataChn.connect(dst_ip, dst_port);
         }

         break;
      }

      default:
         ERR_MSG("Undefined comand");
         break;
      }
   }

   cmd = -1; // for comand after end of main loop

   // close local file
   fhandle.close();

   // update final timestamp
   if (last_timestamp > 0)
   {
      utimbuf ut;
      ut.actime = last_timestamp;
      ut.modtime = last_timestamp;
      utime(filename.c_str(), &ut);
   }

   gettimeofday(&t2, 0);
   int duration = t2.tv_sec - t1.tv_sec;
   double avgRS = 0;
   double avgWS = 0;
   if (duration > 0)
   {
      avgRS = rb / duration * 8.0 / 1000000.0;
      avgWS = wb / duration * 8.0 / 1000000.0;
   }

   INFO_MSG("File server closed, duration " << duration << 
       " reads " << reads << " " << rb << " bytes " << (long long)avgRS 
      << " mb/sec, writes " << writes << " " << wb << " bytes " << (long long)avgWS << " mb/sec ");

   // clear this transaction
   self->m_TransManager.updateSlave(transid, self->m_iSlaveID);

   // unlock the file
   // this must be done before the client is disconnected, otherwise if the client immediately re-open the file,
   // the lock may not be released yet
   self->m_pLocalFile->unlock(sname, key, mode);

   // report to master the task is completed
   // this also must be done before the client is disconnected, otherwise client may not be able to
   // immediately re-open the file as the master is not updated
   if (bWrite)
   {
      // File update can be optimized outside Sector if the write is from local
      // thus the slave will not be able to know if the file has been changed, unless it checks the content
      // we check file size and timestamp here, but this is actually not enough,
      // especially when the time stamp granularity is too low
      SNode s;
      LocalFS::stat(filename, s);
      if ((s.m_llSize != orig_size) || (s.m_llTimeStamp != orig_ts))
         file_change = true;
   }
   int change = file_change ? +FileChangeType::FILE_UPDATE_WRITE : +FileChangeType::FILE_UPDATE_NO;

   DBG_MSG("Updating master");
   self->report(master_ip, master_port, transid, sname, change);

   if (bSecure)
   {
      encoder->release();
      delete encoder;
      decoder->release();
      delete decoder;
   }

   if (success)
   {
      DBG_MSG("Updating peer with success");
      self->m_DataChn.send(client_ip, client_port, transid, (char*)&cmd, 4);
   }
   else
   {
      ERR_MSG("Updating peer with error status");
      self->m_DataChn.sendError(client_ip, client_port, transid);
   }

   return NULL;
}

#ifndef WIN32
void* Slave::copy(void* p)
#else
DWORD WINAPI Slave::copy(LPVOID p)
#endif
{
   int rc;
   Slave* self = ((Param3*)p)->serv_instance;
   int transid = ((Param3*)p)->transid;
   int dir = ((Param3*)p)->dir;
   string src = ((Param3*)p)->src;
   string dst = ((Param3*)p)->dst;
   string master_ip = ((Param3*)p)->master_ip;
   int master_port = ((Param3*)p)->master_port;
   delete (Param3*)p;

   if (src.c_str()[0] == '\0')
      src = "/" + src;
   if (dst.c_str()[0] == '\0')
      dst = "/" + dst;

   DBG_REP(" Replication start");

   bool success = true;

   queue<string> tr;	// files to be replicated
   queue<string> td;	// directories to be explored

   if (dir > 0)
      td.push(src);
   else
      tr.push(src);

   while (!td.empty())
   {
      // FIXME: The logic in this function just doesn't handle the case where there are
      //        multiple files to be replicated and some fail while others succeed.
      DBG_REP( "BUG: attempt to replicate whole directory; this DOES NOT WORK, skipping" );
      success = false;
      break;

      // If the file to be replicated is a directory, recursively list all files first

      string src_path = td.front();
      td.pop();

      // try list this path
      SectorMsg msg;
      msg.setType(101);
      msg.setKey(0);
      msg.setData(0, src_path.c_str(), src_path.length() + 1);

      Address addr;
      self->m_Routing.lookup(src_path, addr);

      if (self->m_GMP.rpc(addr.m_strIP.c_str(), addr.m_iPort, &msg, &msg) < 0)
      {
         success = false;
         break;
      }

      // the master only returns positive if this is a directory
      if (msg.getType() >= 0)
      {
         // if this is a directory, create it, and put all files and sub-directories into the queue of files to be copied

         // create a local dir
         string dst_path = dst;
         if (src != src_path)
            dst_path += "/" + src_path.substr(src.length() + 1, src_path.length() - src.length() - 1);

         //create at .tmp first, then move to real location
         self->createDir(string(".tmp") + dst_path);

         string filelist = msg.getData();
         unsigned int s = 0;
         while (s < filelist.length())
         {
            int t = filelist.find(';', s);
            SNode sn;
            sn.deserialize(filelist.substr(s, t - s).c_str());
            if (sn.m_bIsDir)
               td.push(src_path + "/" + sn.m_strName);
            else
               tr.push(src_path + "/" + sn.m_strName);
            s = t + 1;
         }

         continue;
      }
   }

   // FIXME: The logic in this function just doesn't handle the case where there are
   //        multiple files to be replicated and some fail while others succeed.
   if( tr.size() > 1 )
      DBG_REP( "BUG: attempt to replicate multiple files; good luck if only some succeed." );

   while (!tr.empty())
   {
      string src_path = tr.front();
      tr.pop();

      SNode tmp;
      if (self->m_pLocalFile->lookup(src_path.c_str(), tmp) >= 0)
      {
         //if file is local, copy directly
         //note that in this case, src != dst, therefore this is a regular "cp" command, not a system replication

         //IMPORTANT!!!
         //local files must be read directly from local disk, and cannot be read via datachn due to its limitation
         DBG_REP("Local copy");
         string dst_path = dst;
         if (src != src_path)
            dst_path += "/" + src_path.substr(src.length() + 1, src_path.length() - src.length() - 1);

         //copy to .tmp first, then move to real location
         self->createDir(string(".tmp") + dst_path.substr(0, dst_path.rfind('/')));
         rc = LocalFS::copy(self->m_strHomeDir + src_path, self->m_strHomeDir + ".tmp" + dst_path);
         if (rc < 0 )
           DBG_REP("Error in copy " << rc);
      }
      else
      {
         // open the file and copy it to local
         SectorMsg msg;
         msg.setType(110);
         msg.setKey(0);

         int32_t mode = SF_MODE::READ;
         msg.setData(0, (char*)&mode, 4);
         int32_t localport = self->m_DataChn.getPort();
         msg.setData(4, (char*)&localport, 4);
         int32_t len_name = src_path.length() + 1;
         msg.setData(8, (char*)&len_name, 4);
         msg.setData(12, src_path.c_str(), len_name);
         int32_t len_opt = 0;
         msg.setData(12 + len_name, (char*)&len_opt, 4);

         Address addr;
         self->m_Routing.lookup(src_path, addr);

         if ((self->m_GMP.rpc(addr.m_strIP.c_str(), addr.m_iPort, &msg, &msg) < 0) || (msg.getType() < 0))
         {
            DBG_REP("Error opening file in slave");
            success = false;
         }

         int32_t session;
         int64_t size;
         time_t ts;
         string ip;
         int32_t port;

         if( success )
         {
             session = *(int32_t*)msg.getData();
             size = *(int64_t*)(msg.getData() + 4);
             ts = *(int64_t*)(msg.getData() + 12);
             ip = msg.getData() + 24;
             port = *(int32_t*)(msg.getData() + 64 + 24);

             DBG_REP("Creating replica from " << ip << ":" << port);
             if (!self->m_DataChn.isConnected(ip, port))
             {
                DBG_REP("Not connected to slave - connect " << ip << ":" << port);
                if (self->m_DataChn.connect(ip, port) < 0)
                {
                   DBG_REP("Failed to connect to slave " << ip << ":" << port);
                   success = false;
                }
             }
         }

         // download command: 3
         int32_t cmd = 3;
         int rc = 0;
         if( success )
         {
             rc = self->m_DataChn.send(ip, port, session, (char*)&cmd, 4);
             if (rc < 0) 
             {
                 DBG_REP("Error sending download command");
                 success = false;
             }
         }

         int64_t offset = 0;
         if( success )
         {
             rc = self->m_DataChn.send(ip, port, session, (char*)&offset, 8);
             if (rc < 0)
             {
                 DBG_REP("Error sending download command offset");
                 success = false;
             }
         }

         int response = -1;
         if( success )
         {
             if ((self->m_DataChn.recv4(ip, port, session, response) < 0) || (-1 == response))
             {
                DBG_REP("Error receiving download response");
                success = false;
             }
         }

         string dst_path = dst;
         if (src != src_path)
            dst_path += "/" + src_path.substr(src.length() + 1, src_path.length() - src.length() - 1);

         if( success )
         {
             //copy to .tmp first, then move to real location
             rc = self->createDir(string(".tmp") + dst_path.substr(0, dst_path.rfind('/')));
             if (rc < 0) 
             {
                 DBG_REP("Error creating temp dir");
                 success = false;
             }
         }

         fstream ofs;
         if( success )
         {
             ofs.open((self->m_strHomeDir + ".tmp" + dst_path).c_str(), ios::out | ios::binary | ios::trunc);
             if (ofs.fail()) 
             {
                  DBG_REP("Error creating opening file");
                  success = false;
             }
         }

         if( success ) 
         {
         int64_t unit = 64000000; //send 64MB each time
         int64_t torecv = size;
         int64_t recd = 0;
         while (torecv > 0)
         {
            int64_t block = (torecv < unit) ? torecv : unit;
            if (self->m_DataChn.recvfile(ip, port, session, ofs, offset + recd, block) < 0)
            {
               DBG_REP("Error receiving block of data");
               success = false;
               break;
            }

            recd += block;
            torecv -= block;
         }
         }

         // update total received data size
         self->m_SlaveStat.updateIO(ip, size, +SlaveStat::SYS_IN);

         ofs.close();

         // FIXME: the next two commands can fail, but should failure mean the replication failed, or not?
         //        Technically, we got the whole file;  it's just the source slave's state hasn't been
         //        updated.  Right now, we're assuming success.
         cmd = 5;
         rc = self->m_DataChn.send(ip, port, session, (char*)&cmd, 4);
         if (rc < 0) 
             DBG_REP("Error sending close command");
         rc = self->m_DataChn.recv4(ip, port, session, cmd);
         if (rc < 0)
             DBG_REP("Error receiving close confirmation");

         if( success )
         {
             if (src == dst)
             {
                //utime: update timestamp according to the original copy, for replica only; files created by "cp" have new timestamp
                utimbuf ut;
                ut.actime = ts;
                ut.modtime = ts;
                utime((self->m_strHomeDir + ".tmp" + dst_path).c_str(), &ut);
             }
         }
      }
   }

   if (success)
   {
      // move from temporary dir to the real dir when the copy is completed
      self->createDir(dst.substr(0, dst.rfind('/')));
      rc = LocalFS::rename(self->m_strHomeDir + ".tmp" + dst, self->m_strHomeDir + dst);
      if (rc < 0 ) 
      {
         DBG_REP("Error moving copied file from tmp");
         success = false;
      }
   }

   if( success )
   {
      // if the file has been modified during the replication, remove this replica
      int32_t type = (src == dst) ? +FileChangeType::FILE_UPDATE_REPLICA : +FileChangeType::FILE_UPDATE_NEW;
      DBG_REP("Updating master with success");
      if (self->report(master_ip, master_port, transid, dst, type) < 0)
      {
         DBG_REP("File was updated during replication, remove replica");
         LocalFS::erase(self->m_strHomeDir + dst);
      }
   }
   else
   {
      DBG_REP("Failed, remove all temporary files");
      // failed, remove all temporary files
      rc = LocalFS::erase(self->m_strHomeDir + ".tmp" + dst);
      if (rc < 0) DBG_REP ("Error deleting temp file");
      rc = self->report(master_ip, master_port, transid, std::vector<std::string>(1, dst), (src == dst) ? +FileChangeType::FILE_UPDATE_REPLICA_FAILED : +FileChangeType::FILE_UPDATE_NEW_FAILED);
      if (rc < 0) DBG_REP ("Error reporting to master");
   }

   // clear this transaction
   self->m_TransManager.updateSlave(transid, self->m_iSlaveID);

   DBG_REP("Replication complete");

   return NULL;
}
