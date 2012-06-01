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
   Yunhong Gu, last updated 04/01/2011
*****************************************************************************/

#ifndef WIN32
   #include <sys/time.h>
   #include <sys/types.h>
   #include <sys/stat.h>
   #include <unistd.h>
#endif
#include <stdio.h>
#include <iostream>
#include <sector.h>
#include "../common/log.h"

using namespace std;

namespace
{
   inline logger::LogAggregate& log() 
   {
      static logger::LogAggregate& myLogger = logger::getLogger( "Sector-Pipe" );
      static bool                  once     = false;

      if( !once )
      {
          once = true;
          myLogger.setLogLevel( logger::Debug );
      }

      return myLogger;
   }
}

int main(int argc, char** argv)
{
   logger::config( "/tmp", "sector-pipe" );

   for( int arg = 0; arg < argc; ++arg )
      log().debug << argv[ arg ] << ' ';
   log().debug << std::endl;

   CmdLineParser clp;
   if ((clp.parse(argc, argv) < 0) || (clp.m_mDFlags.size() != 1))
   {
      cerr << "usage #1: <your_application> | sector_pipe -d dst_file" << endl;
      cerr << "usage #2: sector_pipe -s src_file | <your_application>" << endl;
      log().error << "Invalid command-line syntax, exiting with rc = 0" << std::endl;
      return 0;
   }

   string option = clp.m_mDFlags.begin()->first;

   Sector client;
   int rc = Utility::login(client);
   if (rc < 0)
   {
      cerr << "ERROR: failed to log in to sector\n";
      log().error << "Client login to master failed with err = " << rc << ", exiting with rc = -1" << std::endl;
      return -1;
   }

   timeval t1, t2;
   gettimeofday(&t1, 0);
   int64_t total_size = 0;

   SectorFile* f = client.createSectorFile();

   if (option == "d")
   {
      if ((rc = f->open(argv[2], SF_MODE::WRITE | SF_MODE::APPEND)) < 0)
      {
         cerr << "ERROR: unable to open destination file." << endl;
         log().error << "Failed to create destination file with err = " << rc << ", exiting with rc = -1" << std::endl;
         return -1;
      }

      int size = 1000000;
      char* buf = new char[size];
      int read_size = size;
      int written = 0;

      // read data from sdtin and write to Sector
      while(true)
      {
         read_size = read(0, buf, size);
         if (read_size <= 0)
            break;
         written = f->write(buf, read_size);
         total_size += written;
         if( written != read_size )
         {
            cerr << "ERROR: write failed (returned " << written << " instead of " << read_size << ")" << endl;
            log().error << "Partial write of " << read_size << " bytes to destination file failed, write returned " << written 
               << ", exiting with rc = -1" << std::endl;
            return -1;
         }
      }

      delete [] buf;
   }
   else if (option == "s")
   {
      if ((rc = f->open(argv[2], SF_MODE::READ)) < 0)
      {
         cerr << "ERROR: unable to open source file." << endl;
         log().error << "Failed to open source file with err = " << rc << ", exiting with rc = -1" << std::endl;
         return -1;
      }

      int size = 1000000;
      char* buf = new char[size];
      int read_size = size;
      int written = 0;

      // read data from Sector and write to stdout
      while(!f->eof())
      {
         read_size = f->read(buf, size);
         if (read_size <= 0)
            break;
         total_size += read_size;
         written = write(1, buf, read_size);
         if( written != read_size )
         {
            cerr << "ERROR: write to stdout failed (returned " << written << " instead of " << read_size << ")" << endl;
            log().error << "Partial write of " << read_size << " bytes to stdout failed, write returned " << written 
               << ", exiting with rc = -1" << std::endl;
            return -1;
         }
      }

      delete [] buf;
   }

   f->close();
   client.releaseSectorFile(f);

   gettimeofday(&t2, 0);
   double throughput = total_size * 8.0 / 1000000.0 / ((t2.tv_sec - t1.tv_sec) + (t2.tv_usec - t1.tv_usec) / 1000000.0);
   cerr << "Piped " << total_size << " bytes at average speed of " << throughput << " MB/s." << endl << endl;

   Utility::logout(client);

   log().debug << "Successfully piped " << total_size << " bytes at " << throughput << " MB/s" << std::endl;
   return 0;
}
