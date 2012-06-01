// Generate and verify a test file

#include <cstdlib>
#include <fstream>
#include <iostream>
#include <string>

using namespace std;

int64_t gen_file(const char* path, int64_t size)
{
   cout << "generating file " << path << " " << size << endl;

   int64_t count = size / 8;

   fstream file(path, ios::in | ios::out | ios::trunc);
   if (file.bad())
   {
      cout << "unable to open file " << path << endl;
      return -1;
   }

   for (int64_t i = 0; i < count; ++ i)
      file.write((char*)&count, 8);

   file.close();
   return count * 8;
}

int64_t check_file(const char* path)
{
   fstream file(path, ios::in | ios::out | ios::trunc);
   if (file.bad())
      return -1;
   file.seekg(0, ios_base::end);
   int64_t size = file.tellg();
   file.seekg(0);

   cout << "checking file " << path << " " << size << endl;

   int64_t count = size / 8;
   for (int64_t i = 0; i < count; ++ i)
   {
      int64_t val = -1;
      file.read((char*)&val, 8);
      if (val != i)
      {
         cout << "Verification Failed: " << " val = " << val << " at " << i << endl;
         break;
      }
   }

   file.close();
   return 0;
}

int main(int argc, char** argv)
{
   string flag = argv[1];
   string path = argv[2];
   int64_t size = -1;
   if (argc == 4)
      size = atoll(argv[3]);

   if (flag == "gen")
     return gen_file(path.c_str(), size);

   return check_file(path.c_str());
}
