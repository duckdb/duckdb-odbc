
#ifndef WIDECHAR_HPP
#define WIDECHAR_HPP

#pragma once

#include <cstddef>
#include <cstdint>
#include <string>
#include <vector>

#include "duckdb/common/winapi.hpp"

namespace duckdb {
namespace widechar {

DUCKDB_API std::vector<uint8_t> utf16_to_utf8_lenient(const uint16_t *in_buf, std::size_t in_buf_len,
                                           const uint16_t **first_invalid_char = nullptr);

DUCKDB_API std::vector<uint16_t> utf8_to_utf16_lenient(const uint8_t *in_buf, std::size_t in_buf_len,
                                                       const uint8_t **first_invalid_char = nullptr);

DUCKDB_API std::size_t utf16_length(const uint16_t *buf);

DUCKDB_API std::vector<uint8_t> utf8_alloc_out_vec(int16_t utf16_len);

DUCKDB_API void utf16_write_str(int16_t ret, const std::vector<uint8_t> &utf8_vec, uint16_t *utf16_str_out,
                                int16_t *len_out);

DUCKDB_API void utf16_write_str(int16_t ret, const std::vector<uint8_t> &utf8_vec, uint16_t *utf16_str_out,
                                int32_t *len_out);


struct utf16_conv {
public:
  uint16_t *utf16_str = 0;
  std::size_t utf16_len = 0;
  std::vector<uint8_t> utf8_vec;
  uint8_t *utf8_str = 0;

  DUCKDB_API utf16_conv(uint16_t *utf16_str, int64_t utf16_str_len);
  DUCKDB_API utf16_conv(const utf16_conv &) = delete;
  DUCKDB_API utf16_conv(utf16_conv &&other);

  DUCKDB_API utf16_conv &operator=(const utf16_conv &) = delete;
  DUCKDB_API utf16_conv &operator=(utf16_conv &&other);
  
  DUCKDB_API std::size_t utf8_len();
  DUCKDB_API int16_t utf8_len_i16();
  DUCKDB_API int32_t utf8_len_i32();
  DUCKDB_API std::string to_utf8_str();
};

} // namespace widechar
} // namespace duckdb

#endif // WIDECHAR_HPP 