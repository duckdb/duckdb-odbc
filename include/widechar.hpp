
#ifndef WIDECHAR_HPP
#define WIDECHAR_HPP

#pragma once

#include <cstddef>
#include <cstdint>
#include <vector>

namespace duckdb {

std::vector<uint8_t> utf16_to_utf8_lenient(const uint16_t *in_buf, std::size_t in_buf_len, const uint16_t** first_invalid_char);

std::vector<uint16_t> utf8_to_utf16_lenient(const uint8_t *in_buf, std::size_t in_buf_len, const uint8_t** first_invalid_char);

} // namespace duckdb

#endif // WIDECHAR_HPP 