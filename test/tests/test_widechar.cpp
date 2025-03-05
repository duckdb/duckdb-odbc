
#include "odbc_test_common.h"
#define UTF_CPP_CPLUSPLUS 199711L
#include "utf8.h"
#include "widechar.hpp"

using namespace odbc_test;

static const uint32_t invalid_char_replacement = 0xfffd;
static const std::vector<uint8_t> hello_bg_utf8 = {0xd0, 0x97, 0xd0, 0xb4, 0xd1, 0x80, 0xd0, 0xb0, 0xd0,
                                                   0xb2, 0xd0, 0xb5, 0xd0, 0xb9, 0xd1, 0x82, 0xd0, 0xb5};
static const std::vector<uint8_t> invalid_utf8_continuation = {0x48, 0x80, 0x65};
static const std::vector<uint8_t> incomplete_utf8_sequence = {0xe0, 0xa0};
static const std::vector<uint8_t> invalid_overlong_utf8 = {0xf4, 0x90, 0x80, 0x80};
static const std::vector<uint16_t> hello_bg_utf16 = {0x0417, 0x0434, 0x0440, 0x0430, 0x0432,
                                                     0x0435, 0x0439, 0x0442, 0x0435};
static const std::vector<uint16_t> invalid_utf16_surrogate = {0xdc00, 0xd800};
static const std::vector<uint16_t> incomplete_utf16_surrogate = {0xd800};
static const std::vector<uint16_t> valid_utf16_surrogate = {0xd800, 0xdc00};

// The 2 following templates are copied from src/common/widechar.cpp to be able to test them without
// exposing utf.h header from include/widechar.hpp. Please keep them in sync with the implementation
// in src/common/widechar.cpp.

template <typename u16bit_iterator>
static u16bit_iterator utf16_find_invalid(u16bit_iterator start, u16bit_iterator end) {
	while (start != end) {
		utf8::utfchar32_t cp = utf8::internal::mask16(*start++);
		if (utf8::internal::is_lead_surrogate(cp)) { // Take care of surrogate pairs first
			if (start != end) {
				const utf8::utfchar32_t trail_surrogate = utf8::internal::mask16(*start++);
				if (!utf8::internal::is_trail_surrogate(trail_surrogate)) {
					return start - 1;
				}
			} else {
				return start - 1;
			}
		} else if (utf8::internal::is_trail_surrogate(cp)) { // Lone trail surrogate
			return start;
		}
	}
	return end;
}

template <typename u16bit_iterator, typename output_iterator>
static output_iterator utf16_replace_invalid(u16bit_iterator start, u16bit_iterator end, output_iterator out,
                                             utf8::utfchar32_t replacement) {
	while (start != end) {
		uint16_t char1 = *start++;
		utf8::utfchar32_t cp = utf8::internal::mask16(char1);
		if (utf8::internal::is_lead_surrogate(cp)) { // Take care of surrogate pairs first
			if (start != end) {
				uint16_t char2 = *start++;
				const utf8::utfchar32_t trail_surrogate = utf8::internal::mask16(char2);
				if (utf8::internal::is_trail_surrogate(trail_surrogate)) {
					*out++ = char1;
					*out++ = char2;
				} else {
					*out++ = replacement;
					*out++ = char2;
				}
			} else {
				*out++ = replacement;
			}
		} else if (utf8::internal::is_trail_surrogate(cp)) { // Lone trail surrogate
			*out++ = replacement;
		} else {
			*out++ = char1;
		}
	}
	return out;
}

TEST_CASE("Test the utility function utf16_find_invalid used in utf16 -> utf8 conversion", "[odbc]") {
	{
		auto res = utf16_find_invalid(invalid_utf16_surrogate.begin(), invalid_utf16_surrogate.end());
		REQUIRE(res != invalid_utf16_surrogate.end());
	}
	{
		auto res = utf16_find_invalid(incomplete_utf16_surrogate.begin(), incomplete_utf16_surrogate.end());
		REQUIRE(res != incomplete_utf16_surrogate.end());
	}
	{
		auto res = utf16_find_invalid(valid_utf16_surrogate.begin(), valid_utf16_surrogate.end());
		REQUIRE(res == valid_utf16_surrogate.end());
	}
	{
		std::vector<uint16_t> in_buf;
		auto in_buf_bi = std::back_inserter(in_buf);
		std::copy(hello_bg_utf16.begin(), hello_bg_utf16.end(), in_buf_bi);
		std::copy(invalid_utf16_surrogate.begin(), invalid_utf16_surrogate.end(), in_buf_bi);
		std::copy(hello_bg_utf16.begin(), hello_bg_utf16.end(), in_buf_bi);
		auto res = utf16_find_invalid(in_buf.begin(), in_buf.end());
		REQUIRE((res - in_buf.begin()) == hello_bg_utf16.size() + 1);
	}
	{
		std::vector<uint16_t> in_buf;
		auto in_buf_bi = std::back_inserter(in_buf);
		std::copy(hello_bg_utf16.begin(), hello_bg_utf16.end(), in_buf_bi);
		std::copy(incomplete_utf16_surrogate.begin(), incomplete_utf16_surrogate.end(), in_buf_bi);
		std::copy(hello_bg_utf16.begin(), hello_bg_utf16.end(), in_buf_bi);
		auto res = utf16_find_invalid(in_buf.begin(), in_buf.end());
		REQUIRE((res - in_buf.begin()) == hello_bg_utf16.size() + 1);
	}
	{
		std::vector<uint16_t> in_buf;
		auto in_buf_bi = std::back_inserter(in_buf);
		std::copy(hello_bg_utf16.begin(), hello_bg_utf16.end(), in_buf_bi);
		std::copy(valid_utf16_surrogate.begin(), valid_utf16_surrogate.end(), in_buf_bi);
		std::copy(hello_bg_utf16.begin(), hello_bg_utf16.end(), in_buf_bi);
		auto res = utf16_find_invalid(in_buf.begin(), in_buf.end());
		REQUIRE(res == in_buf.end());
	}
}

TEST_CASE("Test the utility function utf16_replace_invalid used in utf16 -> utf8 conversion", "[odbc]") {
	{
		std::vector<uint16_t> replaced;
		auto replaced_bi = std::back_inserter(replaced);
		utf16_replace_invalid(invalid_utf16_surrogate.begin(), invalid_utf16_surrogate.end(), replaced_bi,
		                      invalid_char_replacement);
		REQUIRE(replaced.size() == 2);
		REQUIRE(replaced[0] == invalid_char_replacement);
		REQUIRE(replaced[1] == invalid_char_replacement);
	}
	{
		std::vector<uint16_t> replaced;
		auto replaced_bi = std::back_inserter(replaced);
		utf16_replace_invalid(incomplete_utf16_surrogate.begin(), incomplete_utf16_surrogate.end(), replaced_bi,
		                      invalid_char_replacement);
		REQUIRE(replaced.size() == 1);
		REQUIRE(replaced[0] == invalid_char_replacement);
	}
	{
		std::vector<uint16_t> replaced;
		auto replaced_bi = std::back_inserter(replaced);
		utf16_replace_invalid(valid_utf16_surrogate.begin(), valid_utf16_surrogate.end(), replaced_bi,
		                      invalid_char_replacement);
		REQUIRE(replaced == valid_utf16_surrogate);
	}
	{
		std::vector<uint16_t> in_buf;
		auto in_buf_bi = std::back_inserter(in_buf);
		std::copy(hello_bg_utf16.begin(), hello_bg_utf16.end(), in_buf_bi);
		std::copy(invalid_utf16_surrogate.begin(), invalid_utf16_surrogate.end(), in_buf_bi);
		std::copy(hello_bg_utf16.begin(), hello_bg_utf16.end(), in_buf_bi);
		std::vector<uint16_t> replaced;
		auto replaced_bi = std::back_inserter(replaced);
		utf16_replace_invalid(in_buf.begin(), in_buf.end(), replaced_bi, invalid_char_replacement);
		REQUIRE(std::equal(hello_bg_utf16.begin(), hello_bg_utf16.end(), replaced.begin()));
		REQUIRE(std::equal(hello_bg_utf16.begin(), hello_bg_utf16.end(), replaced.begin() + 11));
		REQUIRE(replaced[9] == invalid_char_replacement);
		REQUIRE(replaced[10] == invalid_char_replacement);
	}
	{
		std::vector<uint16_t> in_buf;
		auto in_buf_bi = std::back_inserter(in_buf);
		std::copy(hello_bg_utf16.begin(), hello_bg_utf16.end(), in_buf_bi);
		std::copy(incomplete_utf16_surrogate.begin(), incomplete_utf16_surrogate.end(), in_buf_bi);
		std::copy(hello_bg_utf16.begin(), hello_bg_utf16.end(), in_buf_bi);
		std::vector<uint16_t> replaced;
		auto replaced_bi = std::back_inserter(replaced);
		utf16_replace_invalid(in_buf.begin(), in_buf.end(), replaced_bi, invalid_char_replacement);
		REQUIRE(std::equal(hello_bg_utf16.begin(), hello_bg_utf16.end(), replaced.begin()));
		REQUIRE(std::equal(hello_bg_utf16.begin(), hello_bg_utf16.end(), replaced.begin() + 10));
		REQUIRE(replaced[9] == invalid_char_replacement);
	}
	{
		std::vector<uint16_t> in_buf;
		auto in_buf_bi = std::back_inserter(in_buf);
		std::copy(hello_bg_utf16.begin(), hello_bg_utf16.end(), in_buf_bi);
		std::copy(valid_utf16_surrogate.begin(), valid_utf16_surrogate.end(), in_buf_bi);
		std::copy(hello_bg_utf16.begin(), hello_bg_utf16.end(), in_buf_bi);
		std::vector<uint16_t> replaced;
		auto replaced_bi = std::back_inserter(replaced);
		utf16_replace_invalid(in_buf.begin(), in_buf.end(), replaced_bi, invalid_char_replacement);
		REQUIRE(replaced == in_buf);
	}
}

TEST_CASE("Test the lenient conversion from  utf16 to utf8", "[odbc]") {
	{
		auto res = duckdb::utf16_to_utf8_lenient(hello_bg_utf16.data(), hello_bg_utf16.size(), nullptr);
		REQUIRE(res == hello_bg_utf8);
	}
	{
		const uint16_t val = 42;
		const uint16_t *ptr = &val;
		auto res = duckdb::utf16_to_utf8_lenient(hello_bg_utf16.data(), hello_bg_utf16.size(), &ptr);
		REQUIRE(utf8::is_valid(res.begin(), res.end()));
		REQUIRE(res == hello_bg_utf8);
		REQUIRE(ptr == nullptr);
	}
	{
		const uint16_t *ptr = nullptr;
		auto res = duckdb::utf16_to_utf8_lenient(invalid_utf16_surrogate.data(), invalid_utf16_surrogate.size(), &ptr);
		REQUIRE(utf8::is_valid(res.begin(), res.end()));
		REQUIRE(ptr != nullptr);
		REQUIRE(ptr - invalid_utf16_surrogate.data() == 1);
	}
	{
		const uint16_t *ptr = nullptr;
		auto res =
		    duckdb::utf16_to_utf8_lenient(incomplete_utf16_surrogate.data(), incomplete_utf16_surrogate.size(), &ptr);
		REQUIRE(utf8::is_valid(res.begin(), res.end()));
		REQUIRE(ptr != nullptr);
		REQUIRE(ptr - incomplete_utf16_surrogate.data() == 0);
	}
	{
		const uint16_t *ptr = nullptr;
		auto res = duckdb::utf16_to_utf8_lenient(valid_utf16_surrogate.data(), valid_utf16_surrogate.size(), &ptr);
		REQUIRE(utf8::is_valid(res.begin(), res.end()));
		REQUIRE(ptr == nullptr);
	}
	{
		const uint16_t *ptr = nullptr;
		std::vector<uint16_t> in_buf;
		auto in_buf_bi = std::back_inserter(in_buf);
		std::copy(hello_bg_utf16.begin(), hello_bg_utf16.end(), in_buf_bi);
		std::copy(invalid_utf16_surrogate.begin(), invalid_utf16_surrogate.end(), in_buf_bi);
		std::copy(hello_bg_utf16.begin(), hello_bg_utf16.end(), in_buf_bi);
		auto res = duckdb::utf16_to_utf8_lenient(in_buf.data(), in_buf.size(), &ptr);
		REQUIRE(utf8::is_valid(res.begin(), res.end()));
		REQUIRE(ptr != nullptr);
		REQUIRE((ptr - in_buf.data()) == 10);
		REQUIRE(std::equal(hello_bg_utf8.begin(), hello_bg_utf8.end(), res.begin()));
		REQUIRE(std::equal(hello_bg_utf8.begin(), hello_bg_utf8.end(), res.begin() + 24));
		REQUIRE(res[hello_bg_utf8.size() + 0] == 0xef);
		REQUIRE(res[hello_bg_utf8.size() + 1] == 0xbf);
		REQUIRE(res[hello_bg_utf8.size() + 2] == 0xbd);
		REQUIRE(res[hello_bg_utf8.size() + 3] == 0xef);
		REQUIRE(res[hello_bg_utf8.size() + 4] == 0xbf);
		REQUIRE(res[hello_bg_utf8.size() + 5] == 0xbd);
	}
	{
		const uint16_t *ptr = nullptr;
		std::vector<uint16_t> in_buf;
		auto in_buf_bi = std::back_inserter(in_buf);
		std::copy(hello_bg_utf16.begin(), hello_bg_utf16.end(), in_buf_bi);
		std::copy(incomplete_utf16_surrogate.begin(), incomplete_utf16_surrogate.end(), in_buf_bi);
		std::copy(hello_bg_utf16.begin(), hello_bg_utf16.end(), in_buf_bi);
		auto res = duckdb::utf16_to_utf8_lenient(in_buf.data(), in_buf.size(), &ptr);
		REQUIRE(utf8::is_valid(res.begin(), res.end()));
		REQUIRE(ptr != nullptr);
		REQUIRE((ptr - in_buf.data()) == 10);
		REQUIRE(std::equal(hello_bg_utf8.begin(), hello_bg_utf8.end(), res.begin()));
		REQUIRE(std::equal(hello_bg_utf8.begin(), hello_bg_utf8.end(), res.begin() + 21));
		REQUIRE(res[hello_bg_utf8.size() + 0] == 0xef);
		REQUIRE(res[hello_bg_utf8.size() + 1] == 0xbf);
		REQUIRE(res[hello_bg_utf8.size() + 2] == 0xbd);
	}
	{
		const uint16_t *ptr = nullptr;
		std::vector<uint16_t> in_buf;
		auto in_buf_bi = std::back_inserter(in_buf);
		std::copy(hello_bg_utf16.begin(), hello_bg_utf16.end(), in_buf_bi);
		std::copy(valid_utf16_surrogate.begin(), valid_utf16_surrogate.end(), in_buf_bi);
		std::copy(hello_bg_utf16.begin(), hello_bg_utf16.end(), in_buf_bi);
		auto res = duckdb::utf16_to_utf8_lenient(in_buf.data(), in_buf.size(), &ptr);
		REQUIRE(utf8::is_valid(res.begin(), res.end()));
		REQUIRE(ptr == nullptr);
		REQUIRE(std::equal(hello_bg_utf8.begin(), hello_bg_utf8.end(), res.begin()));
		REQUIRE(std::equal(hello_bg_utf8.begin(), hello_bg_utf8.end(), res.begin() + 22));
		REQUIRE(res[hello_bg_utf8.size() + 0] == 0xf0);
		REQUIRE(res[hello_bg_utf8.size() + 1] == 0x90);
		REQUIRE(res[hello_bg_utf8.size() + 2] == 0x80);
		REQUIRE(res[hello_bg_utf8.size() + 3] == 0x80);
	}
}

TEST_CASE("Test the lenient conversion from  utf8 to utf16", "[odbc]") {
	{
		auto res = duckdb::utf8_to_utf16_lenient(hello_bg_utf8.data(), hello_bg_utf8.size(), nullptr);
		REQUIRE(res == hello_bg_utf16);
	}
	{
		const uint8_t val = 42;
		const uint8_t *ptr = &val;
		auto res = duckdb::utf8_to_utf16_lenient(hello_bg_utf8.data(), hello_bg_utf8.size(), &ptr);
		REQUIRE(res == hello_bg_utf16);
		REQUIRE(ptr == nullptr);
	}
	{
		const uint8_t *ptr = nullptr;
		auto res =
		    duckdb::utf8_to_utf16_lenient(invalid_utf8_continuation.data(), invalid_utf8_continuation.size(), &ptr);
		REQUIRE(ptr != nullptr);
		REQUIRE(ptr - invalid_utf8_continuation.data() == 1);
	}
	{
		const uint8_t *ptr = nullptr;
		auto res =
		    duckdb::utf8_to_utf16_lenient(incomplete_utf8_sequence.data(), incomplete_utf8_sequence.size(), &ptr);
		REQUIRE(ptr != nullptr);
		REQUIRE(ptr - incomplete_utf8_sequence.data() == 0);
	}
	{
		const uint8_t *ptr = nullptr;
		auto res = duckdb::utf8_to_utf16_lenient(invalid_overlong_utf8.data(), invalid_overlong_utf8.size(), &ptr);
		REQUIRE(ptr != nullptr);
		REQUIRE(ptr - invalid_overlong_utf8.data() == 0);
	}
	{
		const uint8_t *ptr = nullptr;
		std::vector<uint8_t> in_buf;
		auto in_buf_bi = std::back_inserter(in_buf);
		std::copy(hello_bg_utf8.begin(), hello_bg_utf8.end(), in_buf_bi);
		std::copy(invalid_utf8_continuation.begin(), invalid_utf8_continuation.end(), in_buf_bi);
		std::copy(hello_bg_utf8.begin(), hello_bg_utf8.end(), in_buf_bi);
		auto res = duckdb::utf8_to_utf16_lenient(in_buf.data(), in_buf.size(), &ptr);
		REQUIRE(ptr != nullptr);
		REQUIRE((ptr - in_buf.data()) == 19);
		REQUIRE(std::equal(hello_bg_utf16.begin(), hello_bg_utf16.end(), res.begin()));
		REQUIRE(std::equal(hello_bg_utf16.begin(), hello_bg_utf16.end(), res.begin() + 12));
		REQUIRE(res[hello_bg_utf16.size()] == 0x48);
		REQUIRE(res[hello_bg_utf16.size() + 1] == invalid_char_replacement);
		REQUIRE(res[hello_bg_utf16.size() + 2] == 0x65);
	}
	{
		const uint8_t *ptr = nullptr;
		std::vector<uint8_t> in_buf;
		auto in_buf_bi = std::back_inserter(in_buf);
		std::copy(hello_bg_utf8.begin(), hello_bg_utf8.end(), in_buf_bi);
		std::copy(incomplete_utf8_sequence.begin(), incomplete_utf8_sequence.end(), in_buf_bi);
		std::copy(hello_bg_utf8.begin(), hello_bg_utf8.end(), in_buf_bi);
		auto res = duckdb::utf8_to_utf16_lenient(in_buf.data(), in_buf.size(), &ptr);
		REQUIRE(ptr != nullptr);
		REQUIRE((ptr - in_buf.data()) == 18);
		REQUIRE(std::equal(hello_bg_utf16.begin(), hello_bg_utf16.end(), res.begin()));
		REQUIRE(std::equal(hello_bg_utf16.begin(), hello_bg_utf16.end(), res.begin() + 10));
		REQUIRE(res[hello_bg_utf16.size()] == invalid_char_replacement);
	}
	{
		const uint8_t *ptr = nullptr;
		std::vector<uint8_t> in_buf;
		auto in_buf_bi = std::back_inserter(in_buf);
		std::copy(hello_bg_utf8.begin(), hello_bg_utf8.end(), in_buf_bi);
		std::copy(invalid_overlong_utf8.begin(), invalid_overlong_utf8.end(), in_buf_bi);
		std::copy(hello_bg_utf8.begin(), hello_bg_utf8.end(), in_buf_bi);
		auto res = duckdb::utf8_to_utf16_lenient(in_buf.data(), in_buf.size(), &ptr);
		REQUIRE(ptr != nullptr);
		REQUIRE((ptr - in_buf.data()) == 18);
		REQUIRE(std::equal(hello_bg_utf16.begin(), hello_bg_utf16.end(), res.begin()));
		REQUIRE(std::equal(hello_bg_utf16.begin(), hello_bg_utf16.end(), res.begin() + 10));
		REQUIRE(res[hello_bg_utf16.size()] == invalid_char_replacement);
	}
}
