
#include "widechar.hpp"

#include <limits>

#define UTF_CPP_CPLUSPLUS 199711L
#include "utf8.h"

#include "duckdb_odbc.hpp"

#include "widechar_utf16.hpp"

namespace duckdb {
namespace widechar {

static const uint32_t invalid_char_replacement = 0xfffd;
static const std::size_t utf8_byte_len_coef = 3;

std::vector<SQLCHAR> utf16_to_utf8_lenient(const SQLWCHAR *in_buf, std::size_t in_buf_len,
                                           const SQLWCHAR **first_invalid_char) {
	std::vector<SQLCHAR> res;
	auto res_bi = std::back_inserter(res);

	const SQLWCHAR *in_buf_end = in_buf + in_buf_len;
	const SQLWCHAR *first_invalid_found = utf16_find_invalid(in_buf, in_buf_end);

	if (first_invalid_found == in_buf_end) {
		utf8::unchecked::utf16to8(in_buf, in_buf_end, res_bi);
		if (first_invalid_char != nullptr) {
			*first_invalid_char = nullptr;
		}
	} else {
		std::vector<SQLWCHAR> replaced;
		auto replaced_bi = std::back_inserter(replaced);
		utf16_replace_invalid(in_buf, in_buf_end, replaced_bi, invalid_char_replacement);
		utf8::unchecked::utf16to8(replaced.begin(), replaced.end(), res_bi);
		if (first_invalid_char != nullptr) {
			*first_invalid_char = first_invalid_found;
		}
	}

	return res;
}

std::vector<SQLWCHAR> utf8_to_utf16_lenient(const SQLCHAR *in_buf, std::size_t in_buf_len,
                                            const SQLCHAR **first_invalid_char) {
	std::vector<SQLWCHAR> res;
	auto res_bi = std::back_inserter(res);

	const SQLCHAR *in_buf_end = in_buf + in_buf_len;
	const SQLCHAR *first_invalid_found = utf8::find_invalid(in_buf, in_buf_end);

	if (first_invalid_found == in_buf_end) {
		utf8::unchecked::utf8to16(in_buf, in_buf_end, res_bi);
		if (first_invalid_char != nullptr) {
			*first_invalid_char = nullptr;
		}
	} else {
		std::vector<SQLCHAR> replaced;
		auto replaced_bi = std::back_inserter(replaced);
		utf8::replace_invalid(in_buf, in_buf_end, replaced_bi, invalid_char_replacement);
		utf8::unchecked::utf8to16(replaced.begin(), replaced.end(), res_bi);
		if (first_invalid_char != nullptr) {
			*first_invalid_char = first_invalid_found;
		}
	}

	return res;
}

std::size_t utf16_length(const SQLWCHAR *buf) {
	if (!buf) {
		return 0;
	}

	const SQLWCHAR *ptr = buf;
	while (*ptr != 0) {
		ptr++;
	}

	return static_cast<std::size_t>(ptr - buf);
}

std::vector<SQLCHAR> utf8_alloc_out_vec(SQLSMALLINT utf16_len) {
	std::vector<SQLCHAR> vec;
	std::size_t utf8_len_sizet = static_cast<std::size_t>(utf16_len) * utf8_byte_len_coef;
	std::size_t utf8_len_max = static_cast<std::size_t>(std::numeric_limits<SQLSMALLINT>::max());
	vec.resize(std::min(utf8_len_sizet, utf8_len_max));
	return vec;
}

template <typename INT_TYPE>
static void utf16_write_str_internal(SQLRETURN ret, const std::vector<SQLCHAR> &utf8_vec, SQLWCHAR *utf16_str_out,
                                     SQLLEN buffer_len_chars, INT_TYPE *len_out_chars) {
	// DB operation failed
	if (!SQL_SUCCEEDED(ret)) {
		return;
	}

	// No data to write
	if (utf8_vec.size() == 0 || buffer_len_chars <= 0) {
		if (len_out_chars != nullptr) {
			*len_out_chars = 0;
		}
		return;
	}

	// Find out the first NULL terminator
	std::size_t len_utf8 = std::strlen(reinterpret_cast<const char *>(utf8_vec.data()));

	// Write out empty string
	if (len_utf8 == 0 || buffer_len_chars == 1) {
		if (utf16_str_out != nullptr) {
			utf16_str_out[0] = 0;
		}
		if (len_out_chars != nullptr) {
			*len_out_chars = 0;
		}
		return;
	}

	// Convert and write UTF-16 string
	auto utf16_vec = duckdb::widechar::utf8_to_utf16_lenient(utf8_vec.data(), len_utf8);
	std::size_t len_chars = std::min(utf16_vec.size(), static_cast<std::size_t>(buffer_len_chars - 1));
	if (utf16_str_out != nullptr) {
		std::memcpy(utf16_str_out, utf16_vec.data(), len_chars * sizeof(SQLWCHAR));
		utf16_str_out[len_chars] = 0;
	}
	if (len_out_chars != nullptr) {
		*len_out_chars = static_cast<INT_TYPE>(len_chars);
	}
}

void utf16_write_str(SQLRETURN ret, const std::vector<SQLCHAR> &utf8_vec, SQLWCHAR *utf16_str_out,
                     SQLLEN buffer_len_chars, SQLSMALLINT *len_out_chars) {
	return utf16_write_str_internal(ret, utf8_vec, utf16_str_out, buffer_len_chars, len_out_chars);
}

void utf16_write_str(SQLRETURN ret, const std::vector<SQLCHAR> &utf8_vec, SQLWCHAR *utf16_str_out,
                     SQLLEN buffer_len_chars, SQLINTEGER *len_out_chars) {
	return utf16_write_str_internal(ret, utf8_vec, utf16_str_out, buffer_len_chars, len_out_chars);
}

utf16_conv::utf16_conv(SQLWCHAR *utf16_str, SQLLEN utf16_str_len) {
	this->utf16_str = utf16_str;
	if (utf16_str_len == SQL_NTS) {
		this->utf16_len = utf16_length(utf16_str);
	} else {
		this->utf16_len = static_cast<std::size_t>(utf16_str_len);
	}
	if (utf16_str == nullptr) {
		this->utf8_vec = std::vector<SQLCHAR>();
		this->utf8_vec.push_back(0);
		this->utf8_str = nullptr;
	} else {
		this->utf8_vec = utf16_to_utf8_lenient(utf16_str, this->utf16_len);
		this->utf8_vec.push_back(0);
		this->utf8_str = this->utf8_vec.data();
	}
}

utf16_conv::utf16_conv(utf16_conv &&other)
    : utf16_str(other.utf16_str), utf16_len(other.utf16_len), utf8_vec(std::move(other.utf8_vec)),
      utf8_str(other.utf8_str) {
}

utf16_conv &utf16_conv::operator=(utf16_conv &&other) {
	this->utf16_str = other.utf16_str;
	this->utf16_len = other.utf16_len;
	this->utf8_vec = std::move(other.utf8_vec);
	this->utf8_str = other.utf8_str;
	return *this;
}

std::size_t utf16_conv::utf8_len() {
	return this->utf8_vec.size() - 1;
}

template <typename INT_TYPE>
static INT_TYPE utf16_conv_utf8_len_internal(utf16_conv &conv) {
	std::size_t len = conv.utf8_len();
	std::size_t max = static_cast<std::size_t>(std::numeric_limits<INT_TYPE>::max());
	return static_cast<INT_TYPE>(len > max ? max : len);
}

SQLSMALLINT utf16_conv::utf8_len_smallint() {
	return utf16_conv_utf8_len_internal<SQLSMALLINT>(*this);
}

SQLINTEGER utf16_conv::utf8_len_int() {
	return utf16_conv_utf8_len_internal<int32_t>(*this);
}

std::string utf16_conv::to_utf8_str() {
	auto data = reinterpret_cast<const char *>(this->utf8_vec.data());
	auto len = this->utf8_vec.size() - 1;
	return std::string(data, len);
}

} // namespace widechar
} // namespace duckdb
