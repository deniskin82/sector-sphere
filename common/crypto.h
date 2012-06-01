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

#ifndef __CRYPTO_H__
#define __CRYPTO_H__

#include <openssl/evp.h>

class Crypto
{
enum Type {INIT, ENC, DEC};

public:
   Crypto();
   ~Crypto();

public:
   static int generateKey(unsigned char key[16], unsigned char iv[8]);

   int initEnc(unsigned char key[16], unsigned char iv[8]);
   int initDec(unsigned char key[16], unsigned char iv[8]);
   int release();

   int encrypt(unsigned char* input, int insize, unsigned char* output, int& outsize);
   int decrypt(unsigned char* input, int insize, unsigned char* output, int& outsize);

private:
   unsigned char m_pcKey[16];
   unsigned char m_pcIV[8];
   EVP_CIPHER_CTX m_CTX;
   Type m_CoderType;

   static const int g_iEncBlockSize = 1024;
   static const int g_iDecBlockSize = 1024;
};

#endif
