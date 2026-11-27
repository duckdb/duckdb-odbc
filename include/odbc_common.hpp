#ifndef ODBC_COMMON_HPP
#define ODBC_COMMON_HPP

// Shared foundation of the driver: platform headers, DuckDB's stable C++ API, and the small utilities the driver used
// to borrow from DuckDB's internal headers.

#if defined(_WIN32)
#ifndef NOMINMAX
#define NOMINMAX
#endif
#ifndef _WINSOCKAPI_
#define _WINSOCKAPI_
#endif
#include <windows.h>
#undef CreateDirectory
#undef MoveFile
#undef RemoveDirectory
// an access right, which would shadow cxx::StatementType::DELETE
#undef DELETE
#endif

#include "duckdb_cpp/duckdb_cpp.hpp"

// the standard headers the driver relies on, which DuckDB's internal headers used to pull in
#include <algorithm>
#include <array>
#include <atomic>
#include <cctype>
#include <cmath>
#include <cstdarg>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <deque>
#include <functional>
#include <iterator>
#include <limits>
#include <map>
#include <memory>
#include <mutex>
#include <set>
#include <sstream>
#include <stdexcept>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

#ifndef DUCKDB_API
#define DUCKDB_API
#endif

#if defined(ODBC_DEBUG) || defined(D_ASSERT_IS_ENABLED)
#include <cassert>
#define D_ASSERT(condition) assert(condition)
#else
#define D_ASSERT(condition) ((void)0)
#endif

namespace duckdb_odbc {

//! DuckDB's stable C++ API.
namespace cxx = ::duckdb::cxx;

using std::make_shared;
using std::shared_ptr;
using std::string;
using std::unique_ptr;
using std::vector;

template <class T, class... ARGS>
unique_ptr<T> make_uniq(ARGS &&...args) { // NOLINT: mimic std style
	return std::unique_ptr<T>(new T(std::forward<ARGS>(args)...));
}

template <class T>
using unsafe_unique_array = std::unique_ptr<T[]>;

template <class T>
unsafe_unique_array<T> make_unsafe_uniq_array(size_t n) { // NOLINT: mimic std style
	return unsafe_unique_array<T>(new T[n]);
}

//! The database path of an in-memory database.
static constexpr const char *IN_MEMORY_PATH = ":memory:";
//! The schema unqualified names resolve to.
static constexpr const char *DEFAULT_SCHEMA = "main";

struct EnumClassHash {
	template <typename T>
	size_t operator()(T t) const {
		return static_cast<size_t>(t);
	}
};

using idx_t = uint64_t;
using row_t = int64_t;
using data_t = uint8_t;
using data_ptr_t = data_t *;
using const_data_ptr_t = const data_t *;

template <class T>
T Load(const_data_ptr_t ptr) {
	T result;
	std::memcpy(&result, ptr, sizeof(T));
	return result;
}

template <class T>
void Store(const T &value, data_ptr_t ptr) {
	std::memcpy(ptr, &value, sizeof(T));
}

template <class T>
const T &MaxValue(const T &left, const T &right) {
	return left < right ? right : left;
}

template <class T>
const T &MinValue(const T &left, const T &right) {
	return left < right ? left : right;
}

// The DuckDB types the driver works with, all from the stable C++ API.
using Factory = cxx::Factory;
using Value = cxx::Value;
using LogicalType = cxx::LogicalType;
using LogicalTypeId = cxx::LogicalTypeId;
using QueryResult = cxx::QueryResult;
using DataChunk = cxx::DataChunk;
using cxx::date_t;
using cxx::dtime_t;
using cxx::interval_t;
using cxx::timestamp_ms_t;
using cxx::timestamp_ns_t;
using cxx::timestamp_s_t;
using cxx::timestamp_t;
using cxx::timestamp_tz_t;

//===--------------------------------------------------------------------===//
// Strings
//===--------------------------------------------------------------------===//
struct StringUtil {
	static string Lower(const string &str) {
		string result(str);
		std::transform(result.begin(), result.end(), result.begin(),
		               [](unsigned char c) { return static_cast<char>(std::tolower(c)); });
		return result;
	}
	static bool Contains(const string &haystack, const string &needle) {
		return haystack.find(needle) != string::npos;
	}
	static string Upper(const string &str);
	static bool CIEquals(const string &left, const string &right);
	static string Replace(string source, const string &from, const string &to);
	static string Format(const char *format, ...);
	//! "Did you mean" suggestions: the candidates closest to `input` by edit distance, prefixed by `message_prefix`.
	static string CandidatesErrorMessage(const vector<string> &candidates, const string &input,
	                                     const string &message_prefix, idx_t max_candidates = 5);
};

struct CaseInsensitiveHash {
	size_t operator()(const string &str) const {
		return std::hash<string>()(StringUtil::Lower(str));
	}
};

struct CaseInsensitiveEquals {
	bool operator()(const string &left, const string &right) const {
		return StringUtil::CIEquals(left, right);
	}
};

template <class T>
using case_insensitive_map_t = std::unordered_map<string, T, CaseInsensitiveHash, CaseInsensitiveEquals>;

//===--------------------------------------------------------------------===//
// Temporal values
//===--------------------------------------------------------------------===//
struct Interval {
	static constexpr int32_t MONTHS_PER_YEAR = 12;
	static constexpr int32_t DAYS_PER_MONTH = 30;
	static constexpr int64_t HOURS_PER_DAY = 24;
	static constexpr int64_t MINS_PER_HOUR = 60;
	static constexpr int64_t SECS_PER_MINUTE = 60;
	static constexpr int64_t MSECS_PER_SEC = 1000;
	static constexpr int64_t MICROS_PER_MSEC = 1000;
	static constexpr int64_t MICROS_PER_SEC = 1000000;
	static constexpr int64_t MICROS_PER_MINUTE = MICROS_PER_SEC * SECS_PER_MINUTE;
	static constexpr int64_t MICROS_PER_HOUR = MICROS_PER_MINUTE * MINS_PER_HOUR;
	static constexpr int64_t MICROS_PER_DAY = MICROS_PER_HOUR * HOURS_PER_DAY;
	static constexpr int64_t NANOS_PER_MICRO = 1000;
};

struct Date {
	static constexpr int32_t INFINITY_DAYS = std::numeric_limits<int32_t>::max();
	static constexpr int32_t NINFINITY_DAYS = -std::numeric_limits<int32_t>::max();

	//! Throws std::invalid_argument for a date that does not exist.
	static date_t FromDate(int32_t year, int32_t month, int32_t day);
	static void Convert(date_t date, int32_t &year, int32_t &month, int32_t &day);
	static bool IsFinite(date_t date);
};

struct Time {
	//! Throws std::invalid_argument for a time of day that does not exist.
	static dtime_t FromTime(int32_t hour, int32_t minute, int32_t second, int32_t microseconds);
	static void Convert(dtime_t time, int32_t &hour, int32_t &minute, int32_t &second, int32_t &microseconds);
};

struct Timestamp {
	static constexpr int64_t INFINITY_VALUE = std::numeric_limits<int64_t>::max();
	static constexpr int64_t NINFINITY_VALUE = -std::numeric_limits<int64_t>::max();

	static bool IsFinite(timestamp_t timestamp);
	static bool IsFinite(int64_t value) {
		return IsFinite(timestamp_t {value});
	}
	//! Throws std::out_of_range when the result does not fit.
	static timestamp_t FromDatetime(date_t date, dtime_t time);
	static bool TryFromDate(date_t date, timestamp_t &result);
	//! Throws std::invalid_argument for an infinite timestamp.
	static void Convert(timestamp_t timestamp, int32_t &year, int32_t &month, int32_t &day, int32_t &hour,
	                    int32_t &minute, int32_t &second, int32_t &microseconds);
	static date_t GetDate(timestamp_t timestamp);
	//! Throws std::invalid_argument for an infinite timestamp.
	static dtime_t GetTime(timestamp_t timestamp);
	//! Throws std::out_of_range when the result does not fit.
	static timestamp_t FromEpochSeconds(int64_t seconds);
	static timestamp_t FromEpochMs(int64_t ms);
	static timestamp_t FromEpochNanoSeconds(int64_t ns);
};

//===--------------------------------------------------------------------===//
// Types and values
//===--------------------------------------------------------------------===//
//! What the driver needs to know of a result column or parameter's type. Copyable, unlike `LogicalType`, so that it can
//! live in descriptor records.
struct OdbcType {
	OdbcType() = default;
	explicit OdbcType(const LogicalType &type);

	bool IsUnsigned() const {
		return id == LogicalTypeId::UTINYINT || id == LogicalTypeId::USMALLINT || id == LogicalTypeId::UINTEGER ||
		       id == LogicalTypeId::UBIGINT || id == LogicalTypeId::UHUGEINT;
	}

	LogicalTypeId id = LogicalTypeId::INVALID;
	//! Width and scale, for DECIMAL only. A DECIMAL wider than 38 digits is stored as a BIGNUM, and its width is
	//! bounded only by the uint32_t range.
	uint32_t decimal_width = 0;
	uint32_t decimal_scale = 0;
	//! The type as DuckDB renders it, e.g. "DECIMAL(18,3)" or "INTEGER[]".
	string name;
	//! The name of the type's in-memory representation, e.g. "INT32" for INTEGER and DATE, which is what the driver
	//! reports as SQL_DESC_TYPE_NAME.
	string physical_type_name = "INVALID";
};

//! The name DuckDB gives a type id, e.g. "TIMESTAMP_TZ".
string TypeIdToString(LogicalTypeId id);

//! A value of type `id`, built through `factory`.
LogicalType CreateType(Factory &factory, LogicalTypeId id);

//! The value rendered as text, as a cast to VARCHAR does.
string ValueToString(Factory &factory, const Value &value);

//! A VARCHAR value with the given text.
Value CreateVarchar(Factory &factory, const string &str);

//===--------------------------------------------------------------------===//
// Errors
//===--------------------------------------------------------------------===//
//! The full message of an error, e.g. "Binder Error: ...".
inline string ErrorMessage(const std::exception &ex) {
	return ex.what();
}

//! The message of an error without its error-type prefix, when DuckDB raised it.
string RawErrorMessage(const std::exception &ex);

} // namespace duckdb_odbc

#endif // ODBC_COMMON_HPP
