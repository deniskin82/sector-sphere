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
   Yunhong Gu [gu@lac.uic.edu], last updated 03/08/2010
*****************************************************************************/

#ifndef __SECTOR_FS_CLIENT_H__
#define __SECTOR_FS_CLIENT_H__

#include <writelog.h>
#include <client.h>
#include <vector>
#include <fstream>

namespace sector
{

class FSClient
{
friend class Client;

private:
   FSClient();
   ~FSClient();
   const FSClient& operator=(const FSClient&) {return *this;}

public:
   int open(const std::string& filename, int mode = SF_MODE::READ, const SF_OPT* option = NULL);
   int reopen();
   int64_t read(char* buf, const int64_t& offset, const int64_t& size, const int64_t& prefetch = 0);
   int64_t write(const char* buf, const int64_t& offset, const int64_t& size, const int64_t& buffer = 0);
   int64_t read(char* buf, const int64_t& size);
   int64_t write(const char* buf, const int64_t& size);
   int64_t download(const char* localpath, const bool& cont = false);
   int64_t upload(const char* localpath, const bool& cont = false);
   int flush();
   //int truncate(const int64_t& size);
   int close();

   int64_t seekp(int64_t off, int pos = SF_POS::BEG);
   int64_t seekg(int64_t off, int pos = SF_POS::BEG);
   int64_t tellp();
   int64_t tellg();
   bool eof();

private:
   int64_t prefetch(const int64_t& offset, const int64_t& size);
   int flush_();
   int organizeChainOfWrite();

private:
   int32_t m_iSession;		// session ID for data channel
   std::string m_strSlaveIP;	// slave IP address
   int32_t m_iSlaveDataPort;	// slave port number
   std::vector<Address> m_vReplicaAddress;	//list of addresses of all replica nodes

   unsigned char m_pcKey[16];
   unsigned char m_pcIV[8];
   Crypto* m_pEncoder;
   Crypto* m_pDecoder;

   std::string m_strFileName;	// Sector file name
   int64_t m_llSize;            // file size
   int64_t m_llTimeStamp;	// time stamp

   int64_t m_llCurReadPos;	// current read position
   int64_t m_llCurWritePos;	// current write position

   bool m_bRead;		// read permission
   bool m_bWrite;		// write permission
   bool m_bSecure;		// if the data transfer should be secure

   bool m_bLocalOpt;		// optimize local Sector IO with direct IO to native files
   bool m_bReadLocal;		// if this file exist on the same node and can be read directly
   bool m_bWriteLocal;		// if this file exist on the same node and can be written directly 
   std::string m_strLocalPath;	// path of the file if it is local
   std::fstream m_LocalFile;	// file stream for local IO

   int m_iWriteBufSize;		// write buffer size
   WriteLog m_WriteLog;		// write log
   int64_t m_llLastFlushTime;   // last time write is flushed

private:
   pthread_mutex_t m_FileLock;
   bool m_bOpened;		// if a file is actively for IO

private:
   Client* m_pClient;		// client instance
   int m_iID;			// sector file id, for client internal use
};

}  // namespace sector

#endif
