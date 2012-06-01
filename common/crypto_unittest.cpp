/*****************************************************************************
Copyright 2011 VeryCloud LLC

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
   bdl62, last updated 05/21/2011
*****************************************************************************/

#include <assert.h>
#include <cstring>
#include <iostream>

#include "crypto.h"

using namespace std;

int test1()
{
   unsigned char enc_key[16];
   unsigned char enc_iv[8];
   unsigned char dec_key[16];
   unsigned char dec_iv[8];

   Crypto::generateKey(enc_key, enc_iv);
   memcpy(dec_key, enc_key, 16);
   memcpy(dec_iv, enc_iv, 8);

   Crypto enc, dec;
   enc.initEnc(enc_key, enc_iv);
   dec.initDec(dec_key, dec_iv);

   int num = 12345;
   unsigned char output[1024];

   int size = 1024;
   enc.encrypt((unsigned char*)&num, 4, output, size);
   assert(size < 1024);

   int transformed[1024 / 4];   // The output buffer must be large enough to hold immediate result.
   transformed[0] = 67890;      // Although the final result will be only an integer.
   int size2 = 1024;
   dec.decrypt(output, size, (unsigned char*)transformed, size2);
   assert(num == transformed[0]);

   enc.release();
   dec.release();

   return 0;
}

int main()
{
   test1();

   return 0;
}
