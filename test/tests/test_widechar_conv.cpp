
#include <cstring>
#include "odbc_test_common.h"
#define UTF_CPP_CPLUSPLUS 199711L
#include "utf8.h"
#include "widechar.hpp"
#include "../../src/widechar/widechar_utf16.hpp"

using namespace duckdb::widechar;
using namespace odbc_test;

static const uint32_t invalid_char_replacement = 0xfffd;
static const std::vector<SQLCHAR> hello_bg_utf8 = {0xd0, 0x97, 0xd0, 0xb4, 0xd1, 0x80, 0xd0, 0xb0, 0xd0,
                                                   0xb2, 0xd0, 0xb5, 0xd0, 0xb9, 0xd1, 0x82, 0xd0, 0xb5};
static const std::vector<SQLCHAR> invalid_utf8_continuation = {0x48, 0x80, 0x65};
static const std::vector<SQLCHAR> incomplete_utf8_sequence = {0xe0, 0xa0};
static const std::vector<SQLCHAR> invalid_overlong_utf8 = {0xf4, 0x90, 0x80, 0x80};
static const std::vector<SQLWCHAR> hello_bg_utf16 = {0x0417, 0x0434, 0x0440, 0x0430, 0x0432,
                                                     0x0435, 0x0439, 0x0442, 0x0435};
static const std::vector<SQLWCHAR> invalid_utf16_surrogate = {0xdc00, 0xd800};
static const std::vector<SQLWCHAR> incomplete_utf16_surrogate = {0xd800};
static const std::vector<SQLWCHAR> valid_utf16_surrogate = {0xd800, 0xdc00};

TEST_CASE("Test the utility function utf16_find_invalid used in utf16 -> utf8 conversion", "[odbc_widechar]") {
	SECTION("invalid_utf16_surrogate") {
		auto res = utf16_find_invalid(invalid_utf16_surrogate.begin(), invalid_utf16_surrogate.end());
		REQUIRE(res != invalid_utf16_surrogate.end());
	}
	SECTION("incomplete_utf16_surrogate") {
		auto res = utf16_find_invalid(incomplete_utf16_surrogate.begin(), incomplete_utf16_surrogate.end());
		REQUIRE(res != incomplete_utf16_surrogate.end());
	}
	SECTION("valid_utf16_surrogate") {
		auto res = utf16_find_invalid(valid_utf16_surrogate.begin(), valid_utf16_surrogate.end());
		REQUIRE(res == valid_utf16_surrogate.end());
	}
	SECTION("hello invalid_utf16_surrogate") {
		std::vector<SQLWCHAR> in_buf;
		auto in_buf_bi = std::back_inserter(in_buf);
		std::copy(hello_bg_utf16.begin(), hello_bg_utf16.end(), in_buf_bi);
		std::copy(invalid_utf16_surrogate.begin(), invalid_utf16_surrogate.end(), in_buf_bi);
		std::copy(hello_bg_utf16.begin(), hello_bg_utf16.end(), in_buf_bi);
		auto res = utf16_find_invalid(in_buf.begin(), in_buf.end());
		REQUIRE((res - in_buf.begin()) == hello_bg_utf16.size() + 1);
	}
	SECTION("hello incomplete_utf16_surrogate") {
		std::vector<SQLWCHAR> in_buf;
		auto in_buf_bi = std::back_inserter(in_buf);
		std::copy(hello_bg_utf16.begin(), hello_bg_utf16.end(), in_buf_bi);
		std::copy(incomplete_utf16_surrogate.begin(), incomplete_utf16_surrogate.end(), in_buf_bi);
		std::copy(hello_bg_utf16.begin(), hello_bg_utf16.end(), in_buf_bi);
		auto res = utf16_find_invalid(in_buf.begin(), in_buf.end());
		REQUIRE((res - in_buf.begin()) == hello_bg_utf16.size() + 1);
	}
	SECTION("hello valid_utf16_surrogate") {
		std::vector<SQLWCHAR> in_buf;
		auto in_buf_bi = std::back_inserter(in_buf);
		std::copy(hello_bg_utf16.begin(), hello_bg_utf16.end(), in_buf_bi);
		std::copy(valid_utf16_surrogate.begin(), valid_utf16_surrogate.end(), in_buf_bi);
		std::copy(hello_bg_utf16.begin(), hello_bg_utf16.end(), in_buf_bi);
		auto res = utf16_find_invalid(in_buf.begin(), in_buf.end());
		REQUIRE(res == in_buf.end());
	}
}

TEST_CASE("Test the utility function utf16_replace_invalid used in utf16 -> utf8 conversion", "[odbc_widechar]") {
	SECTION("invalid_utf16_surrogate") {
		std::vector<SQLWCHAR> replaced;
		auto replaced_bi = std::back_inserter(replaced);
		utf16_replace_invalid(invalid_utf16_surrogate.begin(), invalid_utf16_surrogate.end(), replaced_bi,
		                      invalid_char_replacement);
		REQUIRE(replaced.size() == 2);
		REQUIRE(replaced[0] == invalid_char_replacement);
		REQUIRE(replaced[1] == invalid_char_replacement);
	}
	SECTION("incomplete_utf16_surrogate") {
		std::vector<SQLWCHAR> replaced;
		auto replaced_bi = std::back_inserter(replaced);
		utf16_replace_invalid(incomplete_utf16_surrogate.begin(), incomplete_utf16_surrogate.end(), replaced_bi,
		                      invalid_char_replacement);
		REQUIRE(replaced.size() == 1);
		REQUIRE(replaced[0] == invalid_char_replacement);
	}
	SECTION("valid_utf16_surrogate") {
		std::vector<SQLWCHAR> replaced;
		auto replaced_bi = std::back_inserter(replaced);
		utf16_replace_invalid(valid_utf16_surrogate.begin(), valid_utf16_surrogate.end(), replaced_bi,
		                      invalid_char_replacement);
		REQUIRE(replaced == valid_utf16_surrogate);
	}
	SECTION("hello invalid_utf16_surrogate") {
		std::vector<SQLWCHAR> in_buf;
		auto in_buf_bi = std::back_inserter(in_buf);
		std::copy(hello_bg_utf16.begin(), hello_bg_utf16.end(), in_buf_bi);
		std::copy(invalid_utf16_surrogate.begin(), invalid_utf16_surrogate.end(), in_buf_bi);
		std::copy(hello_bg_utf16.begin(), hello_bg_utf16.end(), in_buf_bi);
		std::vector<SQLWCHAR> replaced;
		auto replaced_bi = std::back_inserter(replaced);
		utf16_replace_invalid(in_buf.begin(), in_buf.end(), replaced_bi, invalid_char_replacement);
		REQUIRE(std::equal(hello_bg_utf16.begin(), hello_bg_utf16.end(), replaced.begin()));
		REQUIRE(std::equal(hello_bg_utf16.begin(), hello_bg_utf16.end(), replaced.begin() + 11));
		REQUIRE(replaced[9] == invalid_char_replacement);
		REQUIRE(replaced[10] == invalid_char_replacement);
	}
	SECTION("hello incomplete_utf16_surrogate") {
		std::vector<SQLWCHAR> in_buf;
		auto in_buf_bi = std::back_inserter(in_buf);
		std::copy(hello_bg_utf16.begin(), hello_bg_utf16.end(), in_buf_bi);
		std::copy(incomplete_utf16_surrogate.begin(), incomplete_utf16_surrogate.end(), in_buf_bi);
		std::copy(hello_bg_utf16.begin(), hello_bg_utf16.end(), in_buf_bi);
		std::vector<SQLWCHAR> replaced;
		auto replaced_bi = std::back_inserter(replaced);
		utf16_replace_invalid(in_buf.begin(), in_buf.end(), replaced_bi, invalid_char_replacement);
		REQUIRE(std::equal(hello_bg_utf16.begin(), hello_bg_utf16.end(), replaced.begin()));
		REQUIRE(std::equal(hello_bg_utf16.begin(), hello_bg_utf16.end(), replaced.begin() + 10));
		REQUIRE(replaced[9] == invalid_char_replacement);
	}
	SECTION("hello valid_utf16_surrogate") {
		std::vector<SQLWCHAR> in_buf;
		auto in_buf_bi = std::back_inserter(in_buf);
		std::copy(hello_bg_utf16.begin(), hello_bg_utf16.end(), in_buf_bi);
		std::copy(valid_utf16_surrogate.begin(), valid_utf16_surrogate.end(), in_buf_bi);
		std::copy(hello_bg_utf16.begin(), hello_bg_utf16.end(), in_buf_bi);
		std::vector<SQLWCHAR> replaced;
		auto replaced_bi = std::back_inserter(replaced);
		utf16_replace_invalid(in_buf.begin(), in_buf.end(), replaced_bi, invalid_char_replacement);
		REQUIRE(replaced == in_buf);
	}
}

TEST_CASE("Test utf16_to_utf8_lenient function", "[odbc_widechar]") {
	SECTION("hello no invalid") {
		auto res = utf16_to_utf8_lenient(hello_bg_utf16.data(), hello_bg_utf16.size());
		REQUIRE(res == hello_bg_utf8);
	}
	SECTION("hello") {
		const SQLWCHAR val = 42;
		const SQLWCHAR *ptr = &val;
		auto res = utf16_to_utf8_lenient(hello_bg_utf16.data(), hello_bg_utf16.size(), &ptr);
		REQUIRE(utf8::is_valid(res.begin(), res.end()));
		REQUIRE(res == hello_bg_utf8);
		REQUIRE(ptr == nullptr);
	}
	SECTION("invalid_utf16_surrogate") {
		const SQLWCHAR *ptr = nullptr;
		auto res = utf16_to_utf8_lenient(invalid_utf16_surrogate.data(), invalid_utf16_surrogate.size(), &ptr);
		REQUIRE(utf8::is_valid(res.begin(), res.end()));
		REQUIRE(ptr != nullptr);
		REQUIRE(ptr - invalid_utf16_surrogate.data() == 1);
	}
	SECTION("incomplete_utf16_surrogate") {
		const SQLWCHAR *ptr = nullptr;
		auto res = utf16_to_utf8_lenient(incomplete_utf16_surrogate.data(), incomplete_utf16_surrogate.size(), &ptr);
		REQUIRE(utf8::is_valid(res.begin(), res.end()));
		REQUIRE(ptr != nullptr);
		REQUIRE(ptr - incomplete_utf16_surrogate.data() == 0);
	}
	SECTION("valid_utf16_surrogate") {
		const SQLWCHAR *ptr = nullptr;
		auto res = utf16_to_utf8_lenient(valid_utf16_surrogate.data(), valid_utf16_surrogate.size(), &ptr);
		REQUIRE(utf8::is_valid(res.begin(), res.end()));
		REQUIRE(ptr == nullptr);
	}
	SECTION("hello invalid_utf16_surrogate") {
		const SQLWCHAR *ptr = nullptr;
		std::vector<SQLWCHAR> in_buf;
		auto in_buf_bi = std::back_inserter(in_buf);
		std::copy(hello_bg_utf16.begin(), hello_bg_utf16.end(), in_buf_bi);
		std::copy(invalid_utf16_surrogate.begin(), invalid_utf16_surrogate.end(), in_buf_bi);
		std::copy(hello_bg_utf16.begin(), hello_bg_utf16.end(), in_buf_bi);
		auto res = utf16_to_utf8_lenient(in_buf.data(), in_buf.size(), &ptr);
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
	SECTION("hello incomplete_utf16_surrogate") {
		const SQLWCHAR *ptr = nullptr;
		std::vector<SQLWCHAR> in_buf;
		auto in_buf_bi = std::back_inserter(in_buf);
		std::copy(hello_bg_utf16.begin(), hello_bg_utf16.end(), in_buf_bi);
		std::copy(incomplete_utf16_surrogate.begin(), incomplete_utf16_surrogate.end(), in_buf_bi);
		std::copy(hello_bg_utf16.begin(), hello_bg_utf16.end(), in_buf_bi);
		auto res = utf16_to_utf8_lenient(in_buf.data(), in_buf.size(), &ptr);
		REQUIRE(utf8::is_valid(res.begin(), res.end()));
		REQUIRE(ptr != nullptr);
		REQUIRE((ptr - in_buf.data()) == 10);
		REQUIRE(std::equal(hello_bg_utf8.begin(), hello_bg_utf8.end(), res.begin()));
		REQUIRE(std::equal(hello_bg_utf8.begin(), hello_bg_utf8.end(), res.begin() + 21));
		REQUIRE(res[hello_bg_utf8.size() + 0] == 0xef);
		REQUIRE(res[hello_bg_utf8.size() + 1] == 0xbf);
		REQUIRE(res[hello_bg_utf8.size() + 2] == 0xbd);
	}
	SECTION("hello valid_utf16_surrogate") {
		const SQLWCHAR *ptr = nullptr;
		std::vector<SQLWCHAR> in_buf;
		auto in_buf_bi = std::back_inserter(in_buf);
		std::copy(hello_bg_utf16.begin(), hello_bg_utf16.end(), in_buf_bi);
		std::copy(valid_utf16_surrogate.begin(), valid_utf16_surrogate.end(), in_buf_bi);
		std::copy(hello_bg_utf16.begin(), hello_bg_utf16.end(), in_buf_bi);
		auto res = utf16_to_utf8_lenient(in_buf.data(), in_buf.size(), &ptr);
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

TEST_CASE("Test utf8_to_utf16_lenient function", "[odbc_widechar]") {
	SECTION("hello no invalid") {
		auto res = utf8_to_utf16_lenient(hello_bg_utf8.data(), hello_bg_utf8.size());
		REQUIRE(res == hello_bg_utf16);
	}
	SECTION("hello") {
		const SQLCHAR val = 42;
		const SQLCHAR *ptr = &val;
		auto res = utf8_to_utf16_lenient(hello_bg_utf8.data(), hello_bg_utf8.size(), &ptr);
		REQUIRE(res == hello_bg_utf16);
		REQUIRE(ptr == nullptr);
	}
	SECTION("invalid_utf8_continuation") {
		const SQLCHAR *ptr = nullptr;
		auto res = utf8_to_utf16_lenient(invalid_utf8_continuation.data(), invalid_utf8_continuation.size(), &ptr);
		REQUIRE(ptr != nullptr);
		REQUIRE(ptr - invalid_utf8_continuation.data() == 1);
	}
	SECTION("incomplete_utf8_sequence") {
		const SQLCHAR *ptr = nullptr;
		auto res = utf8_to_utf16_lenient(incomplete_utf8_sequence.data(), incomplete_utf8_sequence.size(), &ptr);
		REQUIRE(ptr != nullptr);
		REQUIRE(ptr - incomplete_utf8_sequence.data() == 0);
	}
	SECTION("invalid_overlong_utf8") {
		const SQLCHAR *ptr = nullptr;
		auto res = utf8_to_utf16_lenient(invalid_overlong_utf8.data(), invalid_overlong_utf8.size(), &ptr);
		REQUIRE(ptr != nullptr);
		REQUIRE(ptr - invalid_overlong_utf8.data() == 0);
	}
	SECTION("hello invalid_utf8_continuation") {
		const SQLCHAR *ptr = nullptr;
		std::vector<SQLCHAR> in_buf;
		auto in_buf_bi = std::back_inserter(in_buf);
		std::copy(hello_bg_utf8.begin(), hello_bg_utf8.end(), in_buf_bi);
		std::copy(invalid_utf8_continuation.begin(), invalid_utf8_continuation.end(), in_buf_bi);
		std::copy(hello_bg_utf8.begin(), hello_bg_utf8.end(), in_buf_bi);
		auto res = utf8_to_utf16_lenient(in_buf.data(), in_buf.size(), &ptr);
		REQUIRE(ptr != nullptr);
		REQUIRE((ptr - in_buf.data()) == 19);
		REQUIRE(std::equal(hello_bg_utf16.begin(), hello_bg_utf16.end(), res.begin()));
		REQUIRE(std::equal(hello_bg_utf16.begin(), hello_bg_utf16.end(), res.begin() + 12));
		REQUIRE(res[hello_bg_utf16.size()] == 0x48);
		REQUIRE(res[hello_bg_utf16.size() + 1] == invalid_char_replacement);
		REQUIRE(res[hello_bg_utf16.size() + 2] == 0x65);
	}
	SECTION("hello incomplete_utf8_sequence") {
		const SQLCHAR *ptr = nullptr;
		std::vector<SQLCHAR> in_buf;
		auto in_buf_bi = std::back_inserter(in_buf);
		std::copy(hello_bg_utf8.begin(), hello_bg_utf8.end(), in_buf_bi);
		std::copy(incomplete_utf8_sequence.begin(), incomplete_utf8_sequence.end(), in_buf_bi);
		std::copy(hello_bg_utf8.begin(), hello_bg_utf8.end(), in_buf_bi);
		auto res = utf8_to_utf16_lenient(in_buf.data(), in_buf.size(), &ptr);
		REQUIRE(ptr != nullptr);
		REQUIRE((ptr - in_buf.data()) == 18);
		REQUIRE(std::equal(hello_bg_utf16.begin(), hello_bg_utf16.end(), res.begin()));
		REQUIRE(std::equal(hello_bg_utf16.begin(), hello_bg_utf16.end(), res.begin() + 10));
		REQUIRE(res[hello_bg_utf16.size()] == invalid_char_replacement);
	}
	SECTION("hello invalid_overlong_utf8") {
		const SQLCHAR *ptr = nullptr;
		std::vector<SQLCHAR> in_buf;
		auto in_buf_bi = std::back_inserter(in_buf);
		std::copy(hello_bg_utf8.begin(), hello_bg_utf8.end(), in_buf_bi);
		std::copy(invalid_overlong_utf8.begin(), invalid_overlong_utf8.end(), in_buf_bi);
		std::copy(hello_bg_utf8.begin(), hello_bg_utf8.end(), in_buf_bi);
		auto res = utf8_to_utf16_lenient(in_buf.data(), in_buf.size(), &ptr);
		REQUIRE(ptr != nullptr);
		REQUIRE((ptr - in_buf.data()) == 18);
		REQUIRE(std::equal(hello_bg_utf16.begin(), hello_bg_utf16.end(), res.begin()));
		REQUIRE(std::equal(hello_bg_utf16.begin(), hello_bg_utf16.end(), res.begin() + 10));
		REQUIRE(res[hello_bg_utf16.size()] == invalid_char_replacement);
	}
}

TEST_CASE("Test utf16_length function", "[odbc_widechar]") {
	REQUIRE(utf16_length(nullptr) == 0);
	std::vector<SQLWCHAR> empty;
	empty.push_back(0);
	REQUIRE(utf16_length(empty.data()) == 0);
	std::vector<SQLWCHAR> vec;
	std::copy(hello_bg_utf16.begin(), hello_bg_utf16.end(), std::back_inserter(vec));
	vec.push_back(0);
	REQUIRE(utf16_length(vec.data()) == hello_bg_utf16.size());
}

TEST_CASE("Test utf8_alloc_out_vec function", "[odbc_widechar]") {
	std::vector<SQLCHAR> empty_vec = utf8_alloc_out_vec(0);
	REQUIRE(empty_vec.size() == 0);
	std::vector<SQLCHAR> small_vec = utf8_alloc_out_vec(100);
	REQUIRE(small_vec.size() == 300);
	std::vector<SQLCHAR> large_vec = utf8_alloc_out_vec(30000);
	REQUIRE(large_vec.size() == (std::numeric_limits<SQLSMALLINT>::max)());
}

// This test mimics the intended usage of widechar helper functions
// inside a '*W' ODBC function that calls to a non-wide internal function
TEST_CASE("Test utf16_write_str function", "[odbc_widechar]") {
	// allocate output buffer to pass to internal API intead of the
	// client-provided wide buffer
	std::vector<SQLCHAR> vec = utf8_alloc_out_vec(100);

	// emulate a call to a non-wide internal function that fills the non-wide
	// output buffer and sets its length
	std::memcpy(reinterpret_cast<void *>(vec.data()), reinterpret_cast<const void *>(hello_bg_utf8.data()),
	            hello_bg_utf8.size());
	vec[hello_bg_utf8.size()] = 0;

	SECTION("check with non-empty string") {
		// original client-provided widechar buffer, we need to write there converted API call result
		std::vector<SQLWCHAR> out;
		out.resize(100);
		// len is the total number of characters (excluding the null-termination character) available to return
		SQLSMALLINT len = -1;
		utf16_write_str(SQL_SUCCESS, vec, out.data(), out.size(), &len);
		REQUIRE(len == hello_bg_utf16.size());
		REQUIRE(out[len] == 0);
		REQUIRE(std::equal(hello_bg_utf16.begin(), hello_bg_utf16.end(), out.data()));
	}

	SECTION("check with non-empty string and shorter buffer_len") {
		// original client-provided widechar buffer, we need to write there converted API call result
		std::vector<SQLWCHAR> out;
		out.resize(100);
		// len is the total number of characters (excluding the null-termination character) available to return
		SQLLEN buf_len = hello_bg_utf16.size() - 1;
		SQLSMALLINT len = -1;
		utf16_write_str(SQL_SUCCESS, vec, out.data(), buf_len, &len);
		REQUIRE(len == buf_len - 1);
		REQUIRE(out[len] == 0);
		REQUIRE(std::equal(hello_bg_utf16.begin(), hello_bg_utf16.end() - 2, out.data()));
	}

	SECTION("check with empty string") {
		std::vector<SQLCHAR> empty;
		empty.push_back(0);
		std::vector<SQLWCHAR> out_empty;
		out_empty.push_back(42);
		SQLSMALLINT len = 42;
		utf16_write_str(SQL_SUCCESS, empty, out_empty.data(), 1, &len);
		REQUIRE(out_empty[0] == 0);
		REQUIRE(len == 0);
	}

	SECTION("check with no data") {
		std::vector<SQLCHAR> nodata;
		std::vector<SQLWCHAR> out_untouched;
		out_untouched.push_back(42);
		SQLSMALLINT len = -1;
		utf16_write_str(SQL_SUCCESS, nodata, out_untouched.data(), 0, &len);
		REQUIRE(out_untouched[0] == 42);
		REQUIRE(len == 0);
	}
}

TEST_CASE("Test utf16_conv utility", "[odbc_widechar]") {
	std::vector<SQLWCHAR> in_buf;
	std::copy(hello_bg_utf16.begin(), hello_bg_utf16.end(), std::back_inserter(in_buf));
	in_buf.push_back(0);

	auto conv = utf16_conv(in_buf.data(), SQL_NTS);
	REQUIRE(conv.utf16_len == hello_bg_utf16.size());
	REQUIRE(conv.utf8_len() == hello_bg_utf8.size());
	std::vector<SQLCHAR> utf8_vec;
	std::copy(conv.utf8_str, conv.utf8_str + conv.utf8_len(), std::back_inserter(utf8_vec));
	REQUIRE(std::equal(utf8_vec.begin(), utf8_vec.end(), hello_bg_utf8.begin()));
}
