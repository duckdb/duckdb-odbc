
#ifndef WIDECHAR_HPP
#define WIDECHAR_HPP

#pragma once

#include "duckdb_odbc.hpp"

namespace duckdb {
namespace widechar {

DUCKDB_API std::vector<SQLCHAR> utf16_to_utf8_lenient(const SQLWCHAR *in_buf, std::size_t in_buf_len,
                                           const SQLWCHAR **first_invalid_char = nullptr);

DUCKDB_API std::vector<SQLWCHAR> utf8_to_utf16_lenient(const SQLCHAR *in_buf, std::size_t in_buf_len,
                                                       const SQLCHAR **first_invalid_char = nullptr);

DUCKDB_API std::size_t utf16_length(const SQLWCHAR *buf);

DUCKDB_API std::vector<SQLCHAR> utf8_alloc_out_vec(SQLSMALLINT utf16_len);

DUCKDB_API void utf16_write_str(SQLRETURN ret, const std::vector<SQLCHAR> &utf8_vec, SQLWCHAR *utf16_str_out,
                                SQLLEN buffer_len_chars, SQLSMALLINT *len_out_chars);

DUCKDB_API void utf16_write_str(SQLRETURN ret, const std::vector<SQLCHAR> &utf8_vec, SQLWCHAR *utf16_str_out,
                                SQLLEN buffer_len_chars, SQLINTEGER *len_out_chars);


struct utf16_conv {
public:
  SQLWCHAR *utf16_str = 0;
  std::size_t utf16_len = 0;
  std::vector<SQLCHAR> utf8_vec;
  SQLCHAR *utf8_str = 0;

  DUCKDB_API utf16_conv(SQLWCHAR *utf16_str, SQLLEN utf16_str_len);
  DUCKDB_API utf16_conv(const utf16_conv &) = delete;
  DUCKDB_API utf16_conv(utf16_conv &&other);

  DUCKDB_API utf16_conv &operator=(const utf16_conv &) = delete;
  DUCKDB_API utf16_conv &operator=(utf16_conv &&other);
  
  DUCKDB_API std::size_t utf8_len();
  DUCKDB_API SQLSMALLINT utf8_len_smallint();
  DUCKDB_API SQLINTEGER utf8_len_int();
  DUCKDB_API std::string to_utf8_str();
};

} // namespace widechar
} // namespace duckdb

#endif // WIDECHAR_HPP 