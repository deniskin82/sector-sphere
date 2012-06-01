#include <fcntl.h>
#include <stdio.h>
#include <stdlib.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>
#include <sys/ioctl.h>
#include <dirent.h>
#include <string.h>
#include <errno.h>
#include <sys/time.h>
#include <iostream>
#include <sector.h>

using namespace std;

int main()
{
   Sector client;

   Session s;
   s.loadInfo("../conf/client.conf");

   if (client.init(s.m_ClientConf.m_strMasterIP, s.m_ClientConf.m_iMasterPort) < 0)
      return -1;
   if (client.login(s.m_ClientConf.m_strUserName, s.m_ClientConf.m_strPassword, s.m_ClientConf.m_strCertificate.c_str()) < 0)
      return -1;

   SectorFile* f = client.createSectorFile();

   // reserve enough space to upload the file
   //f->reserveWriteSpace(s.st_size);

   string testfile = "/test/haha2.data";

   int result = f->open(testfile, SF_MODE::READ | SF_MODE::WRITE | SF_MODE::TRUNC | SF_MODE::HiRELIABLE);
   if (result < 0)
   {
      cout << "ERROR: code " << result << " " << SectorError::getErrorMsg(result) << endl;
      return -1;
   }

sleep(30);

   string msg = "this is a test";
   f->write(msg.c_str(), msg.length() + 1);

   SNode attr;
   client.stat(testfile, attr);

   char buf [1024];
   f->read(buf, msg.length() + 1);

   cout << "TEST " << buf << endl;

   f->close();
   client.releaseSectorFile(f);


   client.logout();
   client.close();

   return 0;
}
