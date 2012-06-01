#ifndef STR_FAST_CONCATENATE_H
#define STR_FAST_CONCATENATE_H

#include <string>
#include <string.h>

namespace {
  inline size_t stringLength( const char& )
    { return 1; }

  inline size_t stringLength( const char* p )
    { return strlen( p ); }

  inline size_t stringLength( const std::string& s )
    { return s.length(); }

  inline void append( std::string& s, const char& c )
    { s.append( 1, c ); }

  inline void append( std::string& strDest, const std::string& strSrc )
    { strDest.append( strSrc ); }

  inline void append( std::string& strDest, const char* strSrc )
    { strDest.append( strSrc ); }
}

template< typename T1, typename T2 > inline
std::string& concatenate( std::string& result, const T1& str1, const T2& str2 ) {
  result.reserve( result.length() + stringLength( str1 ) + stringLength( str2 ) );
  append( result, str1 );
  append( result, str2 );
  return result;
}


template< typename T1, typename T2, typename T3 > inline
std::string& concatenate( std::string& result, const T1& str1, const T2& str2, const T3& str3 ) {
  result.reserve( result.length() + stringLength( str1 ) + stringLength( str2 ) + stringLength( str3 ) );
  append( result, str1 );
  append( result, str2 );
  append( result, str3 );
  return result;
}


template< typename T1, typename T2, typename T3, typename T4 > inline
std::string& concatenate( std::string& result, const T1& str1, const T2& str2, const T3& str3, const T4& str4 ) {
  result.reserve( result.length() + stringLength( str1 ) + stringLength( str2 ) + stringLength( str3 ) + stringLength( str4 ) );
  append( result, str1 );
  append( result, str2 );
  append( result, str3 );
  append( result, str4 );
  return result;
}


template< typename T1, typename T2, typename T3, typename T4, typename T5 > inline
std::string& concatenate( std::string& result, const T1& str1, const T2& str2, const T3& str3, const T4& str4, const T5& str5 ) {
  result.reserve( result.length() + stringLength( str1 ) + stringLength( str2 ) + stringLength( str3 ) +
                  stringLength( str4 ) + stringLength( str5 ) );
  append( result, str1 );
  append( result, str2 );
  append( result, str3 );
  append( result, str4 );
  append( result, str5 );
  return result;
}


template< typename T1, typename T2, typename T3, typename T4, typename T5, typename T6 > inline
std::string& concatenate( std::string& result, const T1& str1, const T2& str2, const T3& str3, const T4& str4, const T5& str5, const T6& str6 ) {
  result.reserve( result.length() + stringLength( str1 ) + stringLength( str2 ) + stringLength( str3 ) +
                  stringLength( str4 ) + stringLength( str5 ) + stringLength( str6 ) );
  append( result, str1 );
  append( result, str2 );
  append( result, str3 );
  append( result, str4 );
  append( result, str5 );
  append( result, str6 );
  return result;
}


template< typename T1, typename T2, typename T3, typename T4, typename T5, typename T6, typename T7 > inline
std::string& concatenate( std::string& result, const T1& str1, const T2& str2, const T3& str3, const T4& str4, const T5& str5, const T6& str6, const T7& str7 ) {
  result.reserve( result.length() + stringLength( str1 ) + stringLength( str2 ) + stringLength( str3 ) +
                  stringLength( str4 ) + stringLength( str5 ) + stringLength( str6 ) + stringLength( str7 ) );
  append( result, str1 );
  append( result, str2 );
  append( result, str3 );
  append( result, str4 );
  append( result, str5 );
  append( result, str6 );
  append( result, str7 );
  return result;
}

template< typename T1, typename T2, typename T3, typename T4, typename T5, typename T6, typename T7, typename T8 > inline
std::string& concatenate( std::string& result, const T1& str1, const T2& str2, const T3& str3, const T4& str4, const T5& str5, const T6& str6, const T7& str7, const T8& str8 ) {
  result.reserve( result.length() + stringLength( str1 ) + stringLength( str2 ) + stringLength( str3 ) +
                  stringLength( str4 ) + stringLength( str5 ) + stringLength( str6 ) + stringLength( str7 ) +
                  stringLength( str8 ) );
  append( result, str1 );
  append( result, str2 );
  append( result, str3 );
  append( result, str4 );
  append( result, str5 );
  append( result, str6 );
  append( result, str7 );
  append( result, str8 );
  return result;
}

#endif
