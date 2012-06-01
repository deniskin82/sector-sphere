// stress testing the master

#include <sector.h>
#include <iostream>

using namespace std;

int main()
{
   for (int i = 0; i < 1000000; ++ i)
   {
      Sector* client = new Sector;;
      Utility::login(*client);

      SNode s;
      client->stat("/dst", s);

      Utility::logout(*client);
      delete client;
      cout << "test " << i << endl;
   }

   return 0;
}
