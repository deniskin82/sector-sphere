#include <sector.h>
#include <sys/time.h>
#include <iostream>

using namespace std;

int main(int argc, char** argv)
{
   if (argc != 2)
   {
      cerr << "usage: text.idx text_file_name" << endl;
      return -1;
   }

   Sector client;
   if (Utility::login(client) < 0)
      return -1;

   vector<string> files;
   files.insert(files.end(), argv[1]);

   SphereStream input;
   if (input.init(files) < 0)
   {
      cout << "unable to locate input data files. quit.\n";
      Utility::logout(client);
      return -1;
   }

   SphereStream output;
   output.init(0);

   SphereProcess* myproc = client.createSphereProcess();

   if (myproc->loadOperator("./funcs/gen_idx.so") < 0)
   {
      cout << "cannot find gen_idx.so.\n";
      Utility::logout(client);
      return -1;
   }

   timeval t;
   gettimeofday(&t, 0);
   cout << "start time " << t.tv_sec << endl;

   int result = myproc->run(input, output, "gen_idx", 0);
   if (result < 0)
   {
      Utility::print_error(result);
      Utility::logout(client);
      return -1;
   }

   myproc->waitForCompletion();

   gettimeofday(&t, 0);
   cout << "mission accomplished " << t.tv_sec << endl;

   myproc->close();
   client.releaseSphereProcess(myproc);

   Utility::logout(client);

   return 0;
}
