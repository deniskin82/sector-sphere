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
   Yunhong Gu, last updated 08/16/2010
*****************************************************************************/

#include <sector.h>

using namespace std;


map<int, string> SectorError::s_mErrorMsg;

int SectorError::init()
{
   s_mErrorMsg.clear();
   s_mErrorMsg[+E_UNKNOWN] = "unknown error.";
   s_mErrorMsg[+E_PERMISSION] = "permission is not allowed for the operation on the specified file/dir.";
   s_mErrorMsg[+E_EXIST] = "file/dir already exists.";
   s_mErrorMsg[+E_NOEXIST] = "file/dir does not exist.";
   s_mErrorMsg[+E_BUSY] = "file/dir is busy.";
   s_mErrorMsg[+E_LOCALFILE] = "a failure happens on the local file system.";
   s_mErrorMsg[+E_NOEMPTY] = "directory is not empty.";
   s_mErrorMsg[+E_NOTDIR] = "directory does not exist or not a directory.";
   s_mErrorMsg[+E_FILENOTOPEN] = "this file is not openned yet for IO operations.";
   s_mErrorMsg[+E_NOREPLICA] = "all replicas have been lost.";
   s_mErrorMsg[+E_SECURITY] = "security check (certificate/account/password/acl) failed.";
   s_mErrorMsg[+E_NOCERT] = "no certificate found or wrong certificate.";
   s_mErrorMsg[+E_ACCOUNT] = "the account does not exist.";
   s_mErrorMsg[+E_PASSWORD] = "the password is incorrect.";
   s_mErrorMsg[+E_ACL] = "the request is from an illegal IP address.";
   s_mErrorMsg[+E_INITCTX] = "failed to initialize SSL CTX.";
   s_mErrorMsg[+E_NOSECSERV] = "no response from security server.";
   s_mErrorMsg[+E_EXPIRED] = "client timeout and was kicked out by server.";
   s_mErrorMsg[+E_AUTHORITY] = "no authority to run the command.";
   s_mErrorMsg[+E_ADDR] = "invalid network address.";
   s_mErrorMsg[+E_GMP] = "unable to initailize GMP.";
   s_mErrorMsg[+E_DATACHN] = "unable to initialize data channel.";
   s_mErrorMsg[+E_CERTREFUSE] = "unable to retrieve master certificate";
   s_mErrorMsg[+E_MASTER] = "all masters have been lost";
   s_mErrorMsg[+E_CONNECTION] = "connection fails.";
   s_mErrorMsg[+E_BROKENPIPE] = "data connection has been lost";
   s_mErrorMsg[+E_TIMEOUT] = "message recv timeout";
   s_mErrorMsg[+E_RESOURCE] = "no enough resource (memory/disk) is available.";
   s_mErrorMsg[+E_NODISK] = "no enough disk space.";
   s_mErrorMsg[+E_VERSION] = "incompatible version between client and master.";
   s_mErrorMsg[+E_INVALID] = "at least one parameter is invalid.";
   s_mErrorMsg[+E_SUPPORT] = "the operation is not supported.";
   s_mErrorMsg[+E_CANCELED] = "operation was canceled.";
   s_mErrorMsg[+E_BUCKETFAIL] = "at least one bucket process has failed.";
   s_mErrorMsg[+E_NOPROCESS] = "no sphere process is running.";
   s_mErrorMsg[+E_MISSINGINPUT] = "at least one input file cannot be located.";
   s_mErrorMsg[+E_NOINDEX] = "missing index files.";
   s_mErrorMsg[+E_ALLSPEFAIL] = "all SPE has failed.";
   s_mErrorMsg[+E_NOBUCKET] = "cannot locate any bucket.";

   return s_mErrorMsg.size();
}

string SectorError::getErrorMsg(int ecode)
{
   map<int, string>::const_iterator i = s_mErrorMsg.find(ecode);
   if (i == s_mErrorMsg.end())
      return "unknown error.";

   return i->second;      
}
