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
   Yunhong Gu, last updated 03/07/2011
*****************************************************************************/


#ifndef WIN32
   #include <unistd.h>
   #include <sys/time.h>
#endif
#include <stdio.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <errno.h>
#include <string.h>
#include <common.h>
#include "crypto.h"

Crypto::Crypto():
m_CoderType(INIT)
{
   memset(m_pcKey, 0, 16);
   memset(m_pcIV, 0, 8);
}

Crypto::~Crypto()
{
}

int Crypto::generateKey(unsigned char key[16], unsigned char iv[8])
{
   srand(CTimer::getTime());

   for (int i = 0; i < 16; ++ i)
      key[i] = int(255.0 * (double(rand()) / RAND_MAX));

   for (int i = 0; i < 8; ++ i)
      iv[i] = int(255.0 * (double(rand()) / RAND_MAX));

   //for (int i = 0; i < 16; i++)
   //   printf("%d \t", key[i]);
   //for (int i = 0; i < 8; i++)
   //   printf ("%d \t", iv[i]);

   return 0;
}

int Crypto::initEnc(unsigned char key[16], unsigned char iv[8])
{
   memcpy(m_pcKey, key, 16);
   memcpy(m_pcIV, iv, 8);

   EVP_CIPHER_CTX_init(&m_CTX);
   EVP_EncryptInit(&m_CTX, EVP_bf_cbc(), m_pcKey, m_pcIV);

   m_CoderType = ENC;

   return 0;
}

int Crypto::initDec(unsigned char key[16], unsigned char iv[8])
{
   memcpy(m_pcKey, key, 16);
   memcpy(m_pcIV, iv, 8);

   EVP_CIPHER_CTX_init(&m_CTX);
   EVP_DecryptInit(&m_CTX, EVP_bf_cbc(), m_pcKey, m_pcIV);

   m_CoderType = DEC;

   return 0;
}

int Crypto::release()
{
   EVP_CIPHER_CTX_cleanup(&m_CTX);
   m_CoderType = INIT;
   return 0;
}

int Crypto::encrypt(unsigned char* input, int insize, unsigned char* output, int& outsize)
{
   if (ENC != m_CoderType)
      return -1;

   unsigned char* ip = input;
   unsigned char* op = output;

   int len = 0;
   for (int ts = insize; ts > 0; )
   {
      int unitsize = (ts < g_iEncBlockSize) ? ts : g_iEncBlockSize;

      if (EVP_EncryptUpdate(&m_CTX, op, &len, ip, unitsize) != 1)
      {
         printf ("error in encrypt update\n");
         return -1;
      }

      ip += unitsize;
      op += len;
      ts -= unitsize;
   }

   // the last block, padding
   if (EVP_EncryptFinal(&m_CTX, op, &len) != 1)
   {
       printf ("error in encrypt final\n");
       return -1;
   }
   op += len;

   outsize = op - output;
   return 0;
}

int Crypto::decrypt(unsigned char* input, int insize, unsigned char* output, int& outsize)
{
   if (DEC != m_CoderType)
      return -1;

   unsigned char* ip = input;
   unsigned char* op = output;

   int len = 0;
   for (int ts = insize; ts > 0; )
   {
      int unitsize = (ts < g_iDecBlockSize) ? ts : g_iDecBlockSize;

      int len;
      if (EVP_DecryptUpdate(&m_CTX, op, &len, ip, unitsize) != 1)
      {
         printf("error in decrypt update\n");
         return -1;
      }

      ip += unitsize;
      op += len;
      ts -= unitsize;
   }

   // decrypt last block
   if (EVP_DecryptFinal(&m_CTX, op, &len) != 1)
   {
      printf("error in decrypt final\n");
      return -1;
   }
   op += len;

   outsize = op - output;
   return 0;
}

