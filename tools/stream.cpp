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
   Yunhong Gu, last updated 03/16/2011
*****************************************************************************/

#include <sector.h>
#include <probot.h>
#include <cstdlib>
#include <iostream>
#ifndef WIN32
   #include <sys/time.h>
#else
   #include <time.h>
#endif

using namespace std;

void help()
{
   cout << "stream -i input [-o output] -c command [-b buckets] [-p parameters] [-f files]" << endl;
   cout << endl;
   cout << "-i: input file or directory" << endl;
   cout << "-o: output file or directory (optional)" << endl;
   cout << "-b: number of buckets (optional)" << endl;
   cout << "-c: command or program" << endl;
   cout << "-p: parameters (optional)" << endl;
   cout << "-f: file to upload to Sector servers (optional)" << endl;
}

int main(int argc, char** argv)
{
   CmdLineParser clp;
   if (clp.parse(argc, argv) < 0)
   {
      help();
      return 0;
   }
   
   string inpath = "";
   string outpath = "";
   string tmpdir = "";
   string cmd = "";
   string parameter = "";
   int bucket = 0;
   string upload = "";

   for (map<string, string>::const_iterator i = clp.m_mDFlags.begin(); i != clp.m_mDFlags.end(); ++ i)
   {
      if (i->first == "i")
         inpath = i->second;
      else if (i->first == "o")
         outpath = i->second;
      else if (i->first == "c")
         cmd = i->second;
      else if (i->first == "p")
         parameter = i->second;
      else if (i->first == "b")
         bucket = atoi(i->second.c_str());
      else if (i->first == "f")
         upload = i->second;
      else
      {
         help();
         return 0;
      }
   }

   if ((inpath.length() == 0) || (cmd.length() == 0))
   {
      help();
      return 0;
   }

   if (bucket > 0)
   {
      if (outpath.length() == 0)
      {
         help();
         return 0;
      }

      tmpdir = outpath + "/temp";
   }

   PRobot pr;
   pr.setCmd(cmd);
   pr.setParam(parameter);
   pr.setCmdFlag(upload.length() != 0);
   if (bucket <= 0)
      pr.setOutput(outpath);
   else
      pr.setOutput(tmpdir);
   pr.generate();
   pr.compile();

   //if pr.fail()
   //{
   //   cerr << "unable to create UDFs." << endl;
   //   return -1;
   //}

   Sector client;
   if (Utility::login(client) < 0)
      return -1;

   vector<string> files;
   files.insert(files.end(), inpath);

   SphereStream input;
   if (input.init(files) < 0)
   {
      cerr << "unable to locate input data files. quit.\n";
      Utility::logout(client);
      return -1;
   }

   if (client.mkdir(outpath) == SectorError::E_PERMISSION)
   {
      cerr << "unable to create output path " << outpath << endl;
      Utility::logout(client);
      return -1;
   }

   SphereStream output;
   output.init(0);

   SphereProcess* myproc = client.createSphereProcess();

   if (myproc->loadOperator((string("/tmp/") + cmd + ".so").c_str()) < 0)
   {
      cerr << "unable to create Sphere UDF for " << cmd << endl;
      Utility::logout(client);
      return -1;
   }

   if (upload.length() > 0)
   {
      if (myproc->loadOperator(upload.c_str()) < 0)
      {
         cout << "failed to find/upload " << upload << endl;
         Utility::logout(client);
         return -1;
      }
   }

   timeval t;
   gettimeofday(&t, 0);
   cout << "start time " << t.tv_sec << endl;

   int result = myproc->run(input, output, cmd, 0);
   if (result < 0)
   {
      Utility::print_error(result);
      return -1;
   }

   timeval t1, t2;
   gettimeofday(&t1, 0);
   t2 = t1;
   while (true)
   {
      SphereResult* res;

      if (myproc->read(res) <= 0)
      {
         if (myproc->checkProgress() < 0)
         {
            cerr << "all SPEs failed\n";
            break;
         }

         if (myproc->checkProgress() == 100)
            break;
      }
      else if ((outpath.length() == 0) && (res->m_iDataLen > 0))
      {
         // part of result has been returned, display it

         cout << "RESULT " << res->m_strOrigFile << endl;
         cout << res->m_pcData << endl;
      }

      gettimeofday(&t2, 0);
      if (t2.tv_sec - t1.tv_sec > 60)
      {
         cout << "PROGRESS: " << myproc->checkProgress() << "%" << endl;
         t1 = t2;
      }
   }


   // If no buckets defined, stop
   // otherwise parse all temporary files in tmpdir and generate buckets

   if (bucket > 0)
   {
      SphereStream input2;
      vector<string> tmpdata;
      tmpdata.push_back(tmpdir);
      input2.init(tmpdata);

      SphereStream output2;
      output2.setOutputPath(outpath, "stream_result");
      output2.init(bucket);

      result = myproc->run(input2, output2, "streamhash", 0);
      if (result < 0)
      {
         Utility::print_error(result);
         return -1;
      }

      gettimeofday(&t1, 0);
      t2 = t1;
      while (true)
      {
         SphereResult* res = NULL;

         if (myproc->read(res) <= 0)
         {
            if (myproc->checkProgress() < 0)
            {
               cerr << "all SPEs failed\n";
               break;
            }

            if (myproc->checkProgress() == 100)
               break;
         }
         else
         {
            delete res;
         }

         gettimeofday(&t2, 0);
         if (t2.tv_sec - t1.tv_sec > 60)
         {
            cout << "PROGRESS: " << myproc->checkProgress() << "%" << endl;
            t1 = t2;
         }
      }

      client.rmr(tmpdir);
   }


   gettimeofday(&t, 0);
   cout << "mission accomplished " << t.tv_sec << endl;

   myproc->close();
   client.releaseSphereProcess(myproc);

   Utility::logout(client);

   return 0;
}
