#include <sector.h>
#include <sys/time.h>
#include <iostream>

using namespace std;

int main(int argc, char** /*argv*/)
{
   if (1 != argc)
   {
      cout << "usage: wordcount" << endl;
      return 0;
   }

   Sector client;
   if (Utility::login(client) < 0)
      return -1;

   vector<string> files;
   files.insert(files.end(), "/html");

   SphereStream s;
   if (s.init(files) < 0)
   {
      cout << "unable to locate input data files. quit.\n";
      Utility::logout(client);
      return -1;
   }

   SphereStream temp;
   temp.setOutputPath("/wordcount", "word_bucket");
   temp.init(256);

   SphereProcess* myproc = client.createSphereProcess();

   if (myproc->loadOperator("./funcs/wordbucket.so") < 0)
   {
      cout << "cannot find workbucket.so\n";
      Utility::logout(client);
      return -1;
   }

   timeval t;
   gettimeofday(&t, 0);
   cout << "start time " << t.tv_sec << endl;

   int result = myproc->run(s, temp, "wordbucket", 0);
   if (result < 0)
   {
      Utility::print_error(result);
      Utility::logout(client);
      return -1;
   }

   myproc->waitForCompletion();

   gettimeofday(&t, 0);
   cout << "stage 1 accomplished " << t.tv_sec << endl;

   for (vector<string>::iterator i = temp.m_vFiles.begin(); i != temp.m_vFiles.end(); ++ i)
      cout << *i << endl;

   for (vector<int64_t>::iterator i = temp.m_vSize.begin(); i != temp.m_vSize.end(); ++ i)
      cout << *i << endl;

/*
   //NOT FINISHED. PROCESS EACH BUCKET AND GENERATE INDEX

   SphereStream output;
   output.init(0);
   myproc->setProcNumPerNode(2);
   if (myproc->run(temp, output, "index", 0, NULL, 0) < 0)
   {
      cout << "failed to find any computing resources." << endl;
      return -1;
   }

   myproc->waitForCompletion();

   gettimeofday(&t, 0);
   cout << "stage 2 accomplished " << t.tv_sec << endl;
*/
   cout << "SPE COMPLETED " << endl;

   myproc->close();
   client.releaseSphereProcess(myproc);

   Utility::logout(client);

   return 0;
}
