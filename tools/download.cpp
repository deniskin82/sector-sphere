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
   Yunhong Gu, last updated 01/12/2010
*****************************************************************************/

#ifdef WIN32
   #include <windows.h>
#else
   #include <unistd.h>
   #include <sys/ioctl.h>
   #include <sys/time.h>
   #include <sys/stat.h>
   #include <sys/types.h>
#endif

#include <osportable.h>
#include <fstream>
#include <fcntl.h>
#include <stdio.h>
#include <string.h>
#include <errno.h>
#include <iostream>
#include <sector.h>
#include "../common/log.h"

using namespace std;

namespace
{
   inline logger::LogAggregate& log()
   {
      static logger::LogAggregate& myLogger = logger::getLogger( "Sector-Download" );
      static bool                  once     = false;

      if( !once )
      {
          once = true;
          myLogger.setLogLevel( logger::Debug );
      }

      return myLogger;
   }
}

void help(const char* argv0)
{
   cout << argv0 << " sector_file/dir local_dir [--e (will encrypt) |--smart (skip download if file is same/ resume download if file is smaller, based purely on size)]" << endl;
}

int download(const char* file, const char* dest, Sector& client, bool encryption)
{
   #ifndef WIN32
      timeval t1, t2;
   #else
      DWORD t1, t2;
   #endif

   #ifndef WIN32
      gettimeofday(&t1, 0);
   #else
      t1 = GetTickCount();
   #endif

   SNode attr;
   int rc;
   if ((rc = client.stat(file, attr)) < 0)
   {
      cerr << "ERROR: cannot locate file " << file << endl;
      log().error << "stat of source file " << file << " failed with rc = " << rc << std::endl;
      return -1;
   }

   if (attr.m_bIsDir)
   {
      rc = ::mkdir((string(dest) + "/" + file).c_str(), S_IRWXU);
      if( rc < 0 && errno == EEXIST )
      {
          log().debug << "directory " << dest << '/' << file << " already exists" << std::endl;
          return 1;
      }
      else if( rc < 0 )
      {
          int errno_save = errno;
          log().error << "Failed to create directory " << dest << '/' << file << ", errno = " << errno_save <<  std::endl;
          return -1;
      }
      else
          return 1;
   }

   const long long int size = attr.m_llSize;
   cout << "downloading " << file << " of " << size << " bytes" << endl;
   log().debug << "Downloading " << file << " of " << size << " bytes" << std::endl;

   SectorFile* f = client.createSectorFile();

   int mode = SF_MODE::READ;
   if (encryption)
      mode |= SF_MODE::SECURE;

   if ((rc = f->open(file, mode)) < 0)
   {
      cerr << "unable to locate file " << file << endl;
      log().error << "Failed to open sector file " << file << " with err = " << rc << std::endl;
      return -1;
   }

   int sn = strlen(file) - 1;
   for (; sn >= 0; sn --)
   {
      if (file[sn] == '/')
         break;
   }
   string localpath;
   if (dest[strlen(dest) - 1] != '/')
      localpath = string(dest) + string("/") + string(file + sn + 1);
   else
      localpath = string(dest) + string(file + sn + 1);

   log().debug << "Downloading " << file << " to " << localpath << std::endl;
   int64_t result = f->download(localpath.c_str(), true);

   f->close();
   client.releaseSectorFile(f);

   if (result >= 0)
   {
      float throughput = 0.0;
      #ifndef WIN32
         gettimeofday(&t2, 0);
         float span = (t2.tv_sec - t1.tv_sec) + (t2.tv_usec - t1.tv_usec) / 1000000.0;
      #else
         float span = (GetTickCount() - t1) / 1000.0;
      #endif
      if (span > 0.0)
         throughput = result * 8.0 / 1000000.0 / span;

      cout << "Downloading accomplished! " << "AVG speed " << throughput << " Mb/s." << endl << endl ;
      log().debug << "Download of file successful!" << std::endl;
      return 0;
   }

   cerr << "error happened during downloading " << file << endl;
   log().debug << "Failed to download file, res = " << result << std::endl;
   Utility::print_error(result);
   return -1;
}

int getFileList(const string& path, vector<string>& fl, Sector& client)
{
   SNode attr;
   if (client.stat(path, attr) < 0)
      return -1;

   fl.push_back(path);

   if (attr.m_bIsDir)
   {
      vector<SNode> subdir;
      client.list(path, subdir);

      for (vector<SNode>::iterator i = subdir.begin(); i != subdir.end(); ++ i)
      {
         if (i->m_bIsDir)
            getFileList(path + "/" + i->m_strName, fl, client);
         else
            fl.push_back(path + "/" + i->m_strName);
      }
   }

   return fl.size();
}

int main(int argc, char** argv)
{
   logger::config( "/tmp", "sector-download" );

   for( int arg = 0; arg < argc; ++arg )
      log().debug << argv[ arg ] << ' ';
   log().debug << std::endl;

   if (argc < 3)
   {
      help(argv[0]);
      log().error << "Invalid command-line syntax, exiting with rc = -1" << std::endl;
      return -1;
   }

   CmdLineParser clp;
   if (clp.parse(argc, argv) < 0)
   {
      help(argv[0]);
      log().error << "Invalid command-line syntax, exiting with rc = -1" << std::endl;
      return -1;
   }

   if (clp.m_vParams.size() < 2)
   {
      help(argv[0]);
      log().error << "Invalid command-line syntax, exiting with rc = -1" << std::endl;
      return -1;
   }

   bool encryption = false;
   bool resume = false;

   for (vector<string>::const_iterator i = clp.m_vSFlags.begin(); i != clp.m_vSFlags.end(); ++ i)
   {
      if (*i == "e")
         encryption = true;
      else if( *i == "smart" )
         resume = true;
      else
      {
         help(argv[0]);
         log().error << "Invalid command-line syntax, exiting with rc = -1" << std::endl;
         return -1;
      }
   }

   string newdir = *clp.m_vParams.rbegin();
   clp.m_vParams.erase(clp.m_vParams.begin() + clp.m_vParams.size() - 1);

   // check destination directory, which must exist
   SNode s;
   int r = LocalFS::stat(newdir, s);
   if ((r < 0) || !s.m_bIsDir)
   {
      cerr << "ERROR: destination directory does not exist.\n";
      log().error << "stat failed on destination directory, err = " << r << ", exiting with rc = -1" << std::endl;
      return -1;
   }

   // login to SectorFS
   Sector client;
   int rc = Utility::login(client);
   if (rc < 0)
   {
      cerr << "ERROR: failed to log in to sector\n";
      log().error << "Client login to master failed with err = " << rc << ", exiting with rc = -1" << std::endl;
      return -1;
   }

   // start downloading all files
   for (vector<string>::iterator i = clp.m_vParams.begin(); i != clp.m_vParams.end(); ++ i)
   {
      vector<string> fl;
      bool wc = WildCard::isWildCard(*i);
      if (!wc)
      {
         SNode attr;
         if ((rc = client.stat(*i, attr)) < 0)
         {
            cerr << "ERROR: source file does not exist.\n";
            log().error << "Failed to stat sector file " << *i << ", err = " << rc << std::endl;
            return -1;
         }
         getFileList(*i, fl, client);
      }
      else
      {
         string path = *i;
         string orig = path;
         size_t p = path.rfind('/');
         if (p == string::npos)
            path = "/";
         else
         {
            path = path.substr(0, p);
            orig = orig.substr(p + 1, orig.length() - p);
         }

         vector<SNode> filelist;
         int r = client.list(path, filelist);
         if (r < 0)
            cerr << "ERROR: " << r << " " << SectorError::getErrorMsg(r) << endl;

         for (vector<SNode>::iterator i = filelist.begin(); i != filelist.end(); ++ i)
         {
            if (WildCard::match(orig, i->m_strName))
               getFileList(path + "/" + i->m_strName, fl, client);
         }
      }

      string olddir;
      for (int j = i->length() - 1; j >= 0; -- j)
      {
         if (i->c_str()[j] != '/')
         {
            olddir = i->substr(0, j);
            break;
         }
      }
      size_t p = olddir.rfind('/');
      if (p == string::npos)
         olddir = "";
      else
         olddir = olddir.substr(0, p);

      for (vector<string>::iterator i = fl.begin(); i != fl.end(); ++ i)
      {
         string dst = *i;
         if (olddir.length() > 0)
            dst.replace(0, olddir.length(), newdir);
         else
            dst = newdir + "/" + dst;

         string localdir = dst.substr(0, dst.rfind('/'));

         // if localdir does not exist, create it
         if (LocalFS::stat(localdir, s) < 0)
         {
            for (unsigned int p = 1; p < localdir.length(); ++ p)
            {
               if (localdir.c_str()[p] == '/')
               {
                  string substr = localdir.substr(0, p);

                  if ((-1 == ::mkdir(substr.c_str(), S_IRWXU)) && (errno != EEXIST))
                  {
                     int errno_save = errno;
                     cerr << "ERROR: unable to create local directory " << substr << endl;
                     log().error << "Failed to create local directory " << substr << ", errno = " << errno_save << std::endl;
                     return -1;
                  }
               }
            }

            if ((-1 == ::mkdir(localdir.c_str(), S_IRWXU)) && (errno != EEXIST))
            {
               int errno_save = errno;
               cerr << "ERROR: unable to create local directory " << localdir << endl;
               log().error << "Failed to create local directory " << localdir << ", errno = " << errno_save << std::endl;
               break;
            }
         }

         if (!resume )
         {
            string fileName = *i;
            if( fileName.rfind( '/' ) != fileName.npos )
                fileName = fileName.substr( fileName.rfind( '/' ) + 1 );
            string destFile = localdir + '/' + fileName;
            if( LocalFS::stat( destFile, s ) >= 0 )
            {
                cout << "Destination directory already contains file '" << fileName << "', removing." << endl;
                log().debug << "Destination directory already contains file " << fileName << " and smart not specified, removing file" << std::endl;
                if( LocalFS::erase( destFile ) < 0 )
                {
                   int save_errno = errno;
                   cerr << "ERROR: could not remove destination file: " << strerror( save_errno ) << endl
                       << "NOT downloading file!" << endl;
                   log().error << "Failed to remove destination file " << fileName << ", aborting this file" << std::endl;
                   return -1;
                }
            }
         } 
         else
         {
            string fileName = *i;
            if( fileName.rfind( '/' ) != fileName.npos )
                fileName = fileName.substr( fileName.rfind( '/' ) + 1 );
            string destFile = localdir + '/' + fileName;
            if( LocalFS::stat( destFile, s ) >= 0 )
            {
                cout << "Destination directory already contains file '" << fileName << "', resuming partial download." << endl;
		log().debug << "Destination file already exists with size " << s.m_llSize << ", resuming partial download" << std::endl;
            }
         }

         if (download(i->c_str(), localdir.c_str(), client, encryption) < 0)
         {
            // calculate total available disk size
            int64_t availdisk = 0;
            LocalFS::get_dir_space(localdir, availdisk);
            if (availdisk <= 0)
            {
               // if no disk space svailable, no need to try any more
               cerr << "insufficient local disk space. quit.\n";
               log().error << "Insufficient disk space" << std::endl;
               Utility::logout(client);
            }
            return -1;
         }
      }
   }

   Utility::logout(client);
   log().debug << "Download completed successfully" << std::endl;
   return 0;
}
