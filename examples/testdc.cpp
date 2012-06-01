#include <sector.h>
#include <sys/time.h>
#include <iostream>
#include <cmath>

using namespace std;

int main(int argc, char** /*argv*/)
{
   if (1 != argc)
   {
      cout << "usage: testdc" << endl;
      return 0;
   }

   Sector client;
   if (Utility::login(client) < 0)
      return -1;

   // remove result of last run
   cout << "removing files from previous runs, please wait...\n";
   client.rmr("/test/sorted");
   cout << "done.\n";

   SysStat sys;
   client.sysinfo(sys);
   const int fn = sys.m_llTotalSlaves;
   const int32_t N = (int)log2(fn * 50.0f);
   const int rn = (int)pow(2.0f, N);

   vector<string> files;
   files.push_back("test");

   SphereStream s;
   if (s.init(files) < 0)
   {
      cout << "unable to locate input data files. quit.\n";
      Utility::logout(client);
      return -1;
   }

   SphereStream temp;
   temp.setOutputPath("test/sorted", "stream_sort_bucket");
   temp.init(rn);

   SphereProcess* myproc = client.createSphereProcess();

   if (myproc->loadOperator("./funcs/sorthash.so") < 0)
   {
      cout << "no sorthash.so found\n";
      Utility::logout(client);
      return -1;
   }
   if (myproc->loadOperator("./funcs/sort.so") < 0)
   {
      cout << "no sort.so found\n";
      Utility::logout(client);
      return -1;
   }

   timeval t;
   gettimeofday(&t, 0);
   cout << "start time " << t.tv_sec << endl;

   int result = myproc->run(s, temp, "sorthash", 1, (char*)&N, 4);
   if (result < 0)
   {
      Utility::print_error(result);
      Utility::logout(client);
      return -1;
   }

   myproc->waitForCompletion();

   gettimeofday(&t, 0);
   cout << "stage 1 accomplished " << t.tv_sec << endl;

   SphereStream output;
   output.init(0);
   myproc->setProcNumPerNode(2);

   result = myproc->run(temp, output, "sort", 0, NULL, 0);
   if (result < 0)
   {
      Utility::print_error(result);
      Utility::logout(client);
      return -1;
   }

   myproc->waitForCompletion();

   gettimeofday(&t, 0);
   cout << "stage 2 accomplished " << t.tv_sec << endl;

   cout << "SPE COMPLETED " << endl;

   myproc->close();
   client.releaseSphereProcess(myproc);

   Utility::logout(client);

   return 0;
}
