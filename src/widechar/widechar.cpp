
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

std::vector<uint8_t> utf16_to_utf8_lenient(const uint16_t *in_buf, std::size_t in_buf_len,
                                           const uint16_t **first_invalid_char) {
	std::vector<uint8_t> res;
	auto res_bi = std::back_inserter(res);

	const uint16_t *in_buf_end = in_buf + in_buf_len;
	const uint16_t *first_invalid_found = utf16_find_invalid(in_buf, in_buf_end);

	if (first_invalid_found == in_buf_end) {
		utf8::unchecked::utf16to8(in_buf, in_buf_end, res_bi);
		if (first_invalid_char != nullptr) {
			*first_invalid_char = nullptr;
		}
	} else {
		std::vector<uint16_t> replaced;
		auto replaced_bi = std::back_inserter(replaced);
		utf16_replace_invalid(in_buf, in_buf_end, replaced_bi, invalid_char_replacement);
		utf8::unchecked::utf16to8(replaced.begin(), replaced.end(), res_bi);
		if (first_invalid_char != nullptr) {
			*first_invalid_char = first_invalid_found;
		}
	}

	return res;
}

std::vector<uint16_t> utf8_to_utf16_lenient(const uint8_t *in_buf, std::size_t in_buf_len,
                                            const uint8_t **first_invalid_char) {
	std::vector<uint16_t> res;
	auto res_bi = std::back_inserter(res);

	const uint8_t *in_buf_end = in_buf + in_buf_len;
	const uint8_t *first_invalid_found = utf8::find_invalid(in_buf, in_buf_end);

	if (first_invalid_found == in_buf_end) {
		utf8::unchecked::utf8to16(in_buf, in_buf_end, res_bi);
		if (first_invalid_char != nullptr) {
			*first_invalid_char = nullptr;
		}
	} else {
		std::vector<uint8_t> replaced;
		auto replaced_bi = std::back_inserter(replaced);
		utf8::replace_invalid(in_buf, in_buf_end, replaced_bi, invalid_char_replacement);
		utf8::unchecked::utf8to16(replaced.begin(), replaced.end(), res_bi);
		if (first_invalid_char != nullptr) {
			*first_invalid_char = first_invalid_found;
		}
	}

	return res;
}

std::size_t utf16_length(const uint16_t *buf) {
	if (!buf) {
		return 0;
	}

	const uint16_t *ptr = buf;
	while (*ptr != 0) {
		ptr++;
	}

	return static_cast<std::size_t>(ptr - buf);
}

std::vector<uint8_t> utf8_alloc_out_vec(int16_t utf16_len) {
	std::vector<uint8_t> vec;
	std::size_t utf8_len_sizet = static_cast<std::size_t>(utf16_len) * utf8_byte_len_coef;
	std::size_t utf8_len_max = static_cast<std::size_t>(std::numeric_limits<int16_t>::max());
	vec.resize(std::min(utf8_len_sizet, utf8_len_max));
	return vec;
}

template <typename LEN_INT_TYPE>
static void utf16_write_str_internal(SQLRETURN ret, const std::vector<uint8_t> &utf8_vec, uint16_t *utf16_str_out,
                                     LEN_INT_TYPE *len_out) {
	if (SQL_SUCCEEDED(ret)) {
		std::size_t len = 0;
		if (len_out != nullptr) {
			len = static_cast<std::size_t>(*len_out);
		} else {
			len = std::strlen(reinterpret_cast<const char *>(utf8_vec.data()));
		}
		auto vec = utf8_to_utf16_lenient(utf8_vec.data(), len);
		vec.push_back(0);
		if (utf16_str_out != nullptr) {
			std::memcpy(static_cast<void *>(utf16_str_out), static_cast<void *>(vec.data()),
			            vec.size() * sizeof(SQLWCHAR));
		}
		if (len_out != nullptr) {
			*len_out = static_cast<LEN_INT_TYPE>(vec.size() - 1);
		}
	}
}

void utf16_write_str(SQLRETURN ret, const std::vector<uint8_t> &utf8_vec, uint16_t *utf16_str_out, int16_t *len_out) {
	return utf16_write_str_internal(ret, utf8_vec, utf16_str_out, len_out);
}

void utf16_write_str(SQLRETURN ret, const std::vector<uint8_t> &utf8_vec, uint16_t *utf16_str_out, int32_t *len_out) {
	return utf16_write_str_internal(ret, utf8_vec, utf16_str_out, len_out);
}

utf16_conv::utf16_conv(uint16_t *utf16_str, int64_t utf16_str_len) {
	this->utf16_str = utf16_str;
	if (utf16_str_len < 0) {
		this->utf16_len = utf16_length(utf16_str);
	} else {
		this->utf16_len = static_cast<std::size_t>(utf16_str_len);
	}
	if (utf16_str == nullptr) {
		this->utf8_vec = std::vector<uint8_t>();
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

int16_t utf16_conv::utf8_len_i16() {
	return utf16_conv_utf8_len_internal<int16_t>(*this);
}

int32_t utf16_conv::utf8_len_i32() {
	return utf16_conv_utf8_len_internal<int32_t>(*this);
}

std::string utf16_conv::to_utf8_str() {
	auto data = reinterpret_cast<const char *>(this->utf8_vec.data());
	auto len = this->utf8_vec.size() - 1;
	return std::string(data, len);
}

} // namespace widechar
} // namespace duckdb
