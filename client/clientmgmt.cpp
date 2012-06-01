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
   Yunhong Gu, last updated 03/16/2010
*****************************************************************************/

#include "clientmgmt.h"
#include "common.h"
#include "sector.h"

using namespace std;
using namespace sector;

ClientMgmt::ClientMgmt():
m_iID(0)
{
   CGuard::createMutex(m_CLock);
   CGuard::createMutex(m_FSLock);
   CGuard::createMutex(m_DCLock);
}

ClientMgmt::~ClientMgmt()
{
   CGuard::releaseMutex(m_CLock);
   CGuard::releaseMutex(m_FSLock);
   CGuard::releaseMutex(m_DCLock);
}

Client* ClientMgmt::lookupClient(const int& id)
{
   Client* c = NULL;

   CGuard::enterCS(m_CLock);
   map<int, Client*>::iterator i = m_mClients.find(id);
   if (i != m_mClients.end())
      c = i->second;
   CGuard::leaveCS(m_CLock);

   return c;
}

FSClient* ClientMgmt::lookupFS(const int& id)
{
   FSClient* f = NULL;

   CGuard::enterCS(m_FSLock);
   map<int, FSClient*>::iterator i = m_mSectorFiles.find(id);
   if (i != m_mSectorFiles.end())
      f = i->second;
   CGuard::leaveCS(m_FSLock);

   return f;
}

DCClient* ClientMgmt::lookupDC(const int& id)
{
   DCClient* d = NULL;

   CGuard::enterCS(m_DCLock);
   map<int, DCClient*>::iterator i = m_mSphereProcesses.find(id);
   if (i != m_mSphereProcesses.end())
      d = i->second;
   CGuard::leaveCS(m_DCLock);

   return d;
}

int ClientMgmt::insertClient(Client* c)
{
   CGuard::enterCS(m_CLock);
   int id = m_iID ++;
   m_mClients[id] = c;
   CGuard::leaveCS(m_CLock);

   return id;
}

int ClientMgmt::insertFS(FSClient* f)
{
   CGuard::enterCS(m_CLock);
   int id = m_iID ++;
   CGuard::leaveCS(m_CLock);

   CGuard::enterCS(m_FSLock);
   m_mSectorFiles[id] = f;
   CGuard::leaveCS(m_FSLock);

   return id;
}

int ClientMgmt::insertDC(DCClient* d)
{
   CGuard::enterCS(m_CLock);
   int id = m_iID ++;
   CGuard::leaveCS(m_CLock);

   CGuard::enterCS(m_DCLock);
   m_mSphereProcesses[id] = d;
   CGuard::leaveCS(m_DCLock);

   return id;
}

int ClientMgmt::removeClient(const int& id)
{
   CGuard::enterCS(m_CLock);
   m_mClients.erase(id);
   CGuard::leaveCS(m_CLock);

   return 0;
}

int ClientMgmt::removeFS(const int& id)
{
   CGuard::enterCS(m_FSLock);
   m_mSectorFiles.erase(id);
   CGuard::leaveCS(m_FSLock);

   return 0;
}

int ClientMgmt::removeDC(const int& id)
{
   CGuard::enterCS(m_DCLock);
   m_mSphereProcesses.erase(id);
   CGuard::leaveCS(m_DCLock);

   return 0;
}


int Sector::init()
{
   Client* c = new Client;
   int r = c->init();

   if (r >= 0)
   {
      m_iID = Client::g_ClientMgmt.insertClient(c);
   }
   else
   {
      delete c;
   }

   return r;
}

#define FIND_CLIENT_OR_ERROR(c) \
   Client* c = Client::g_ClientMgmt.lookupClient(m_iID); \
   if (NULL == c) \
      return SectorError::E_INVALID;

int Sector::login(const string& server, const int& port,
                  const string& username, const string& password, const char* cert)
{
   FIND_CLIENT_OR_ERROR(c)
   return c->login(server, port, username, password, cert);
}

int Sector::logout()
{
   FIND_CLIENT_OR_ERROR(c)
   return c->logout();
}

int Sector::close()
{
   FIND_CLIENT_OR_ERROR(c)
   Client::g_ClientMgmt.removeClient(m_iID);
   c->close();
   delete c;
   return 0;
}

int Sector::list(const string& path, vector<SNode>& attr)
{
   FIND_CLIENT_OR_ERROR(c)
   return c->list(path, attr);
}

int Sector::list(const string& path, vector<SNode>& attr, bool includeReplica)
{
   FIND_CLIENT_OR_ERROR(c)
   return c->list(path, attr, includeReplica);
}

int Sector::stat(const string& path, SNode& attr)
{
   FIND_CLIENT_OR_ERROR(c)
   return c->stat(path, attr);
}

int Sector::mkdir(const string& path)
{
   FIND_CLIENT_OR_ERROR(c)
   return c->mkdir(path);
}

int Sector::move(const string& oldpath, const string& newpath)
{
   FIND_CLIENT_OR_ERROR(c)
   return c->move(oldpath, newpath);
}

int Sector::remove(const string& path)
{
   FIND_CLIENT_OR_ERROR(c)
   return c->remove(path);
}

int Sector::rmr(const string& path)
{
   FIND_CLIENT_OR_ERROR(c)
   return c->rmr(path);
}

int Sector::copy(const string& src, const string& dst)
{
   FIND_CLIENT_OR_ERROR(c)
   return c->copy(src, dst);
}

int Sector::utime(const string& path, const int64_t& ts)
{
   FIND_CLIENT_OR_ERROR(c)
   return c->utime(path, ts);
}

int Sector::sysinfo(SysStat& sys)
{
   FIND_CLIENT_OR_ERROR(c)
   return c->sysinfo(sys);
}

int Sector::debuginfo(std::string& dbg)
{
   FIND_CLIENT_OR_ERROR(c)
   return c->debuginfo(dbg);
}

int Sector::df(int64_t& availableSize, int64_t& totalSize)
{
   FIND_CLIENT_OR_ERROR(c)
   return c->df(availableSize, totalSize);
}

int Sector::shutdown(const int& type, const string& param)
{
   FIND_CLIENT_OR_ERROR(c)
   return c->shutdown(type, param);
}

int Sector::fsck(const string& path)
{
   FIND_CLIENT_OR_ERROR(c)
   return c->fsck(path);
}

int Sector::configLog(const char* log_path, bool screen, int level)
{
   FIND_CLIENT_OR_ERROR(c)
   return c->configLog(log_path, screen, level);
}

int Sector::setMaxCacheSize(const int64_t& ms)
{
   FIND_CLIENT_OR_ERROR(c)
   return c->setMaxCacheSize(ms);
}

SectorFile* Sector::createSectorFile()
{
   Client* c = Client::g_ClientMgmt.lookupClient(m_iID);

   if (NULL == c)
      return NULL;

   FSClient* f = c->createFSClient();
   SectorFile* sf = new SectorFile;

   sf->m_iID = Client::g_ClientMgmt.insertFS(f);

   return sf;
}

SphereProcess* Sector::createSphereProcess()
{
   Client* c = Client::g_ClientMgmt.lookupClient(m_iID);

   if (NULL == c)
      return NULL;

   DCClient* d = c->createDCClient();
   SphereProcess* sp = new SphereProcess;

   sp->m_iID = Client::g_ClientMgmt.insertDC(d);

   return sp;
}

int Sector::releaseSectorFile(SectorFile* sf)
{
   if (NULL == sf)
      return 0;

   Client* c = Client::g_ClientMgmt.lookupClient(m_iID);
   FSClient* f = Client::g_ClientMgmt.lookupFS(sf->m_iID);

   if ((NULL == c) || (NULL == f))
      return SectorError::E_INVALID;

   Client::g_ClientMgmt.removeFS(sf->m_iID);
   c->releaseFSClient(f);
   delete sf;
   return 0;
}

int Sector::releaseSphereProcess(SphereProcess* sp)
{
   if (NULL == sp)
      return 0;

   Client* c = Client::g_ClientMgmt.lookupClient(m_iID);
   DCClient* d = Client::g_ClientMgmt.lookupDC(sp->m_iID);

   if ((NULL == c) || (NULL == d))
      return SectorError::E_INVALID;

   Client::g_ClientMgmt.removeDC(sp->m_iID);
   c->releaseDCClient(d);
   delete sp;
   return 0;
}

#define FIND_FILE_OR_ERROR(f) \
   FSClient* f = Client::g_ClientMgmt.lookupFS(m_iID); \
   if (NULL == f) \
      return SectorError::E_INVALID;


int SectorFile::open(const string& filename, int mode, const SF_OPT* option)
{
   FIND_FILE_OR_ERROR(f)
   return f->open(filename, mode, option);
}

int64_t SectorFile::read(char* buf, const int64_t& offset, const int64_t& size, const int64_t& prefetch)
{
   FIND_FILE_OR_ERROR(f)
   return f->read(buf, offset, size, prefetch);
}

int64_t SectorFile::write(const char* buf, const int64_t& offset, const int64_t& size, const int64_t& buffer)
{
   FIND_FILE_OR_ERROR(f)
   return f->write(buf, offset, size, buffer);
}

int64_t SectorFile::read(char* buf, const int64_t& size)
{
   FIND_FILE_OR_ERROR(f)
   return f->read(buf, size);
}

int64_t SectorFile::write(const char* buf, const int64_t& size)
{
   FIND_FILE_OR_ERROR(f)
   return f->write(buf, size);
}

int64_t SectorFile::download(const char* localpath, const bool& cont)
{
   FIND_FILE_OR_ERROR(f)
   return f->download(localpath, cont);
}

int64_t SectorFile::upload(const char* localpath, const bool& cont)
{
   FIND_FILE_OR_ERROR(f)
   return f->upload(localpath, cont);
}

int SectorFile::flush()
{
   FIND_FILE_OR_ERROR(f)
   return f->flush();
}

int SectorFile::close()
{
   FIND_FILE_OR_ERROR(f)
   return f->close();
}

int64_t SectorFile::seekp(int64_t off, int pos)
{
   FIND_FILE_OR_ERROR(f)
   return f->seekp(off, pos);
}

int64_t SectorFile::seekg(int64_t off, int pos)
{
   FIND_FILE_OR_ERROR(f)
   return f->seekg(off, pos);
}

int64_t SectorFile::tellp()
{
   FIND_FILE_OR_ERROR(f)
   return f->tellp();
}

int64_t SectorFile::tellg()
{
   FIND_FILE_OR_ERROR(f)
   return f->tellg();
}

bool SectorFile::eof()
{
   FIND_FILE_OR_ERROR(f)
   return f->eof();
}

#define FIND_SPHERE_OR_ERROR(d) \
   DCClient* d = Client::g_ClientMgmt.lookupDC(m_iID); \
   if (NULL == d) \
      return SectorError::E_INVALID;

int SphereProcess::close()
{
   FIND_SPHERE_OR_ERROR(d)
   return d->close();
}

int SphereProcess::loadOperator(const char* library)
{
   FIND_SPHERE_OR_ERROR(d)
   return d->loadOperator(library);
}

int SphereProcess::run(const SphereStream& input, SphereStream& output, const string& op, const int& rows, const char* param, const int& size, const int& type)
{
   FIND_SPHERE_OR_ERROR(d)
   return d->run(input, output, op, rows, param, size, type);
}

int SphereProcess::run_mr(const SphereStream& input, SphereStream& output, const string& mr, const int& rows, const char* param, const int& size)
{
   FIND_SPHERE_OR_ERROR(d)
   return d->run_mr(input, output, mr, rows, param, size);
}

int SphereProcess::read(SphereResult*& res, const bool& inorder, const bool& wait)
{
   FIND_SPHERE_OR_ERROR(d)
   return d->read(res, inorder, wait);
}

int SphereProcess::checkProgress()
{
   FIND_SPHERE_OR_ERROR(d)
   return d->checkProgress();
}

int SphereProcess::checkMapProgress()
{
   FIND_SPHERE_OR_ERROR(d)
   return d->checkMapProgress();
}

int SphereProcess::checkReduceProgress()
{
   FIND_SPHERE_OR_ERROR(d)
   return d->checkReduceProgress();
}

int SphereProcess::waitForCompletion()
{
   FIND_SPHERE_OR_ERROR(d)
   return d->waitForCompletion();
}

void SphereProcess::setMinUnitSize(int size)
{
   DCClient* d = Client::g_ClientMgmt.lookupDC(m_iID);
   if (NULL != d)
     d->setMinUnitSize(size);
}

void SphereProcess::setMaxUnitSize(int size)
{
   DCClient* d = Client::g_ClientMgmt.lookupDC(m_iID);
   if (NULL != d)
      d->setMaxUnitSize(size);
}

void SphereProcess::setProcNumPerNode(int num)
{
   DCClient* d = Client::g_ClientMgmt.lookupDC(m_iID);
   if (NULL != d)
      d->setProcNumPerNode(num);
}

void SphereProcess::setDataMoveAttr(bool move)
{
   DCClient* d = Client::g_ClientMgmt.lookupDC(m_iID);
   if (NULL != d)
      d->setDataMoveAttr(move);
}
