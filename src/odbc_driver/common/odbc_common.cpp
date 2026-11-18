#include "odbc_common.hpp"

#include "duckdb.h"

#include <stdexcept>

namespace duckdb_odbc {

//===--------------------------------------------------------------------===//
// Strings
//===--------------------------------------------------------------------===//
string StringUtil::Upper(const string &str) {
	string result(str);
	std::transform(result.begin(), result.end(), result.begin(),
	               [](unsigned char c) { return static_cast<char>(std::toupper(c)); });
	return result;
}

bool StringUtil::CIEquals(const string &left, const string &right) {
	if (left.size() != right.size()) {
		return false;
	}
	for (idx_t i = 0; i < left.size(); i++) {
		if (std::tolower(static_cast<unsigned char>(left[i])) != std::tolower(static_cast<unsigned char>(right[i]))) {
			return false;
		}
	}
	return true;
}

string StringUtil::Replace(string source, const string &from, const string &to) {
	if (from.empty()) {
		return source;
	}
	idx_t pos = 0;
	while ((pos = source.find(from, pos)) != string::npos) {
		source.replace(pos, from.size(), to);
		pos += to.size();
	}
	return source;
}

string StringUtil::Format(const char *format, ...) {
	va_list args;
	va_start(args, format);
	va_list args_copy;
	va_copy(args_copy, args);
	int len = std::vsnprintf(nullptr, 0, format, args_copy);
	va_end(args_copy);
	string result;
	if (len > 0) {
		result.resize(static_cast<idx_t>(len) + 1);
		std::vsnprintf(&result[0], result.size(), format, args);
		result.resize(static_cast<idx_t>(len));
	}
	va_end(args);
	return result;
}

// Optimal string alignment distance, as DuckDB computes it for its own "did you mean" suggestions.
static idx_t LevenshteinDistance(const string &s1, const string &s2) {
	const idx_t len1 = s1.size();
	const idx_t len2 = s2.size();
	if (len1 == 0) {
		return len2;
	}
	if (len2 == 0) {
		return len1;
	}
	vector<idx_t> dist((len1 + 1) * (len2 + 1));
	auto score = [&](idx_t i, idx_t j) -> idx_t & {
		return dist[i * (len2 + 1) + j];
	};
	for (idx_t i = 0; i <= len1; i++) {
		score(i, 0) = i;
	}
	for (idx_t j = 0; j <= len2; j++) {
		score(0, j) = j;
	}
	for (idx_t i = 1; i <= len1; i++) {
		for (idx_t j = 1; j <= len2; j++) {
			idx_t cost = s1[i - 1] == s2[j - 1] ? 0 : 1;
			idx_t current = MinValue(score(i - 1, j) + 1, score(i, j - 1) + 1);
			current = MinValue(current, score(i - 1, j - 1) + cost);
			if (i > 1 && j > 1 && s1[i - 1] == s2[j - 2] && s1[i - 2] == s2[j - 1]) {
				current = MinValue(current, score(i - 2, j - 2) + 1);
			}
			score(i, j) = current;
		}
	}
	return score(len1, len2);
}

string StringUtil::CandidatesErrorMessage(const vector<string> &candidates, const string &input,
                                          const string &message_prefix, idx_t max_candidates) {
	static constexpr idx_t THRESHOLD = 5;
	vector<std::pair<string, idx_t>> scores;
	for (auto &candidate : candidates) {
		auto compared = input.size() < candidate.size() ? candidate.substr(0, input.size()) : candidate;
		scores.emplace_back(candidate, LevenshteinDistance(compared, input));
	}
	std::sort(scores.begin(), scores.end(), [](const std::pair<string, idx_t> &a, const std::pair<string, idx_t> &b) {
		if (a.second != b.second) {
			return a.second < b.second;
		}
		return Lower(a.first) < Lower(b.first);
	});
	if (scores.empty()) {
		return string();
	}
	// the closest candidate is always suggested, the others only when they are close enough
	string result = "\n" + message_prefix + ": ";
	for (idx_t i = 0; i < scores.size() && i < max_candidates; i++) {
		if (i > 0 && scores[i].second > THRESHOLD) {
			break;
		}
		if (i > 0) {
			result += ", ";
		}
		result += "\"" + scores[i].first + "\"";
	}
	return result;
}

//===--------------------------------------------------------------------===//
// Temporal values
//===--------------------------------------------------------------------===//
static duckdb_date ToCDate(date_t date) {
	duckdb_date result;
	result.days = date.days;
	return result;
}

static duckdb_time ToCTime(dtime_t time) {
	duckdb_time result;
	result.micros = time.micros;
	return result;
}

static duckdb_timestamp ToCTimestamp(timestamp_t timestamp) {
	duckdb_timestamp result;
	result.micros = timestamp.micros;
	return result;
}

static bool operator==(const duckdb_date_struct &left, const duckdb_date_struct &right) {
	return left.year == right.year && left.month == right.month && left.day == right.day;
}

static bool operator==(const duckdb_time_struct &left, const duckdb_time_struct &right) {
	return left.hour == right.hour && left.min == right.min && left.sec == right.sec && left.micros == right.micros;
}

// duckdb_to_timestamp returns the epoch for a timestamp it cannot create: tell it from a real one by converting back.
static bool TryToTimestamp(const duckdb_timestamp_struct &parts, timestamp_t &result) {
	auto timestamp = duckdb_to_timestamp(parts);
	auto converted = duckdb_from_timestamp(timestamp);
	if (!(converted.date == parts.date) || !(converted.time == parts.time)) {
		return false;
	}
	result.micros = timestamp.micros;
	return true;
}

// duckdb_to_date returns the epoch for a date that does not exist: tell it from a real one by converting back.
date_t Date::FromDate(int32_t year, int32_t month, int32_t day) {
	duckdb_date_struct parts;
	parts.year = year;
	parts.month = static_cast<int8_t>(month);
	parts.day = static_cast<int8_t>(day);
	auto date = duckdb_to_date(parts);
	if (month < 1 || month > 12 || day < 1 || day > 31 || !(duckdb_from_date(date) == parts)) {
		throw std::invalid_argument(StringUtil::Format("Date out of range: %d-%d-%d", year, month, day));
	}
	date_t result;
	result.days = date.days;
	return result;
}

void Date::Convert(date_t date, int32_t &year, int32_t &month, int32_t &day) {
	auto parts = duckdb_from_date(ToCDate(date));
	year = parts.year;
	month = parts.month;
	day = parts.day;
}

bool Date::IsFinite(date_t date) {
	return duckdb_is_finite_date(ToCDate(date));
}

dtime_t Time::FromTime(int32_t hour, int32_t minute, int32_t second, int32_t microseconds) {
	if (hour < 0 || hour > 23 || minute < 0 || minute > 59 || second < 0 || second > 59 || microseconds < 0 ||
	    microseconds >= Interval::MICROS_PER_SEC) {
		throw std::invalid_argument(
		    StringUtil::Format("Time out of range: %d:%d:%d.%d", hour, minute, second, microseconds));
	}
	duckdb_time_struct parts;
	parts.hour = static_cast<int8_t>(hour);
	parts.min = static_cast<int8_t>(minute);
	parts.sec = static_cast<int8_t>(second);
	parts.micros = microseconds;
	dtime_t result;
	result.micros = duckdb_to_time(parts).micros;
	return result;
}

void Time::Convert(dtime_t time, int32_t &hour, int32_t &minute, int32_t &second, int32_t &microseconds) {
	auto parts = duckdb_from_time(ToCTime(time));
	hour = parts.hour;
	minute = parts.min;
	second = parts.sec;
	microseconds = parts.micros;
}

bool Timestamp::IsFinite(timestamp_t timestamp) {
	return duckdb_is_finite_timestamp(ToCTimestamp(timestamp));
}

timestamp_t Timestamp::FromDatetime(date_t date, dtime_t time) {
	duckdb_timestamp_struct parts;
	parts.date = duckdb_from_date(ToCDate(date));
	parts.time = duckdb_from_time(ToCTime(time));
	timestamp_t result;
	if (!Date::IsFinite(date) || !TryToTimestamp(parts, result)) {
		throw std::out_of_range("Date and time not in timestamp range");
	}
	return result;
}

bool Timestamp::TryFromDate(date_t date, timestamp_t &result) {
	if (!Date::IsFinite(date)) {
		result.micros = date.days > 0 ? INFINITY_VALUE : NINFINITY_VALUE;
		return true;
	}
	duckdb_timestamp_struct parts;
	parts.date = duckdb_from_date(ToCDate(date));
	parts.time = duckdb_from_time(ToCTime(dtime_t {0}));
	return TryToTimestamp(parts, result);
}

void Timestamp::Convert(timestamp_t timestamp, int32_t &year, int32_t &month, int32_t &day, int32_t &hour,
                        int32_t &minute, int32_t &second, int32_t &microseconds) {
	if (!IsFinite(timestamp)) {
		throw std::invalid_argument("Can't get TIME of infinite TIMESTAMP");
	}
	auto parts = duckdb_from_timestamp(ToCTimestamp(timestamp));
	year = parts.date.year;
	month = parts.date.month;
	day = parts.date.day;
	hour = parts.time.hour;
	minute = parts.time.min;
	second = parts.time.sec;
	microseconds = parts.time.micros;
}

date_t Timestamp::GetDate(timestamp_t timestamp) {
	date_t result;
	if (!IsFinite(timestamp)) {
		result.days = timestamp.micros > 0 ? Date::INFINITY_DAYS : Date::NINFINITY_DAYS;
		return result;
	}
	result.days = duckdb_to_date(duckdb_from_timestamp(ToCTimestamp(timestamp)).date).days;
	return result;
}

dtime_t Timestamp::GetTime(timestamp_t timestamp) {
	if (!IsFinite(timestamp)) {
		throw std::invalid_argument("Can't get TIME of infinite TIMESTAMP");
	}
	dtime_t result;
	result.micros = duckdb_to_time(duckdb_from_timestamp(ToCTimestamp(timestamp)).time).micros;
	return result;
}

static bool TryMultiply(int64_t left, int64_t right, int64_t &result) {
	if (left != 0 && (std::abs(left) > std::numeric_limits<int64_t>::max() / std::abs(right))) {
		return false;
	}
	result = left * right;
	return true;
}

timestamp_t Timestamp::FromEpochSeconds(int64_t seconds) {
	timestamp_t result;
	if (!TryMultiply(seconds, Interval::MICROS_PER_SEC, result.micros)) {
		throw std::out_of_range("Could not convert Timestamp(S) to Timestamp(US)");
	}
	return result;
}

timestamp_t Timestamp::FromEpochMs(int64_t ms) {
	timestamp_t result;
	if (!TryMultiply(ms, Interval::MICROS_PER_MSEC, result.micros)) {
		throw std::out_of_range("Could not convert Timestamp(MS) to Timestamp(US)");
	}
	return result;
}

timestamp_t Timestamp::FromEpochNanoSeconds(int64_t ns) {
	timestamp_t result;
	result.micros = ns / Interval::NANOS_PER_MICRO;
	return result;
}

//===--------------------------------------------------------------------===//
// Types and values
//===--------------------------------------------------------------------===//
// The physical type DuckDB stores a type as, see LogicalType::GetInternalType.
static string PhysicalTypeName(const LogicalType &type) {
	switch (type.GetTypeId()) {
	case LogicalTypeId::BOOLEAN:
		return "BOOL";
	case LogicalTypeId::TINYINT:
		return "INT8";
	case LogicalTypeId::UTINYINT:
		return "UINT8";
	case LogicalTypeId::SMALLINT:
		return "INT16";
	case LogicalTypeId::USMALLINT:
		return "UINT16";
	case LogicalTypeId::SQLNULL:
	case LogicalTypeId::DATE:
	case LogicalTypeId::INTEGER:
		return "INT32";
	case LogicalTypeId::UINTEGER:
		return "UINT32";
	case LogicalTypeId::BIGINT:
	case LogicalTypeId::TIME:
	case LogicalTypeId::TIME_NS:
	case LogicalTypeId::TIMESTAMP:
	case LogicalTypeId::TIMESTAMP_SEC:
	case LogicalTypeId::TIMESTAMP_NS:
	case LogicalTypeId::TIMESTAMP_MS:
	case LogicalTypeId::TIME_TZ:
	case LogicalTypeId::TIMESTAMP_TZ:
	case LogicalTypeId::TIMESTAMP_TZ_NS:
		return "INT64";
	case LogicalTypeId::UBIGINT:
		return "UINT64";
	case LogicalTypeId::UHUGEINT:
		return "UINT128";
	case LogicalTypeId::HUGEINT:
	case LogicalTypeId::UUID:
		return "INT128";
	case LogicalTypeId::FLOAT:
		return "FLOAT";
	case LogicalTypeId::DOUBLE:
		return "DOUBLE";
	case LogicalTypeId::BIGNUM:
	case LogicalTypeId::VARCHAR:
	case LogicalTypeId::BLOB:
	case LogicalTypeId::BIT:
	case LogicalTypeId::TYPE:
	case LogicalTypeId::GEOMETRY:
		return "VARCHAR";
	case LogicalTypeId::INTERVAL:
		return "INTERVAL";
	case LogicalTypeId::UNION:
	case LogicalTypeId::STRUCT:
		return "STRUCT";
	case LogicalTypeId::LIST:
	case LogicalTypeId::MAP:
		return "LIST";
	case LogicalTypeId::ARRAY:
		return "ARRAY";
	case LogicalTypeId::DECIMAL: {
		auto width = type.GetDecimalWidth();
		if (width <= 4) {
			return "INT16";
		} else if (width <= 9) {
			return "INT32";
		} else if (width <= 18) {
			return "INT64";
		}
		return "INT128";
	}
	case LogicalTypeId::ENUM: {
		auto size = type.GetEnumSize();
		if (size <= std::numeric_limits<uint8_t>::max()) {
			return "UINT8";
		} else if (size <= std::numeric_limits<uint16_t>::max()) {
			return "UINT16";
		}
		return "UINT32";
	}
	default:
		return "INVALID";
	}
}

OdbcType::OdbcType(const LogicalType &type)
    : id(type.GetTypeId()), name(type.ToText()), physical_type_name(PhysicalTypeName(type)) {
	if (id == LogicalTypeId::DECIMAL) {
		decimal_width = type.GetDecimalWidth();
		decimal_scale = type.GetDecimalScale();
	}
}

string TypeIdToString(LogicalTypeId id) {
	switch (id) {
	case LogicalTypeId::INVALID:
		return "INVALID";
	case LogicalTypeId::SQLNULL:
		return "NULL";
	case LogicalTypeId::UNKNOWN:
		return "UNKNOWN";
	case LogicalTypeId::ANY:
		return "ANY";
	case LogicalTypeId::TYPE:
		return "TYPE";
	case LogicalTypeId::BOOLEAN:
		return "BOOLEAN";
	case LogicalTypeId::TINYINT:
		return "TINYINT";
	case LogicalTypeId::SMALLINT:
		return "SMALLINT";
	case LogicalTypeId::INTEGER:
		return "INTEGER";
	case LogicalTypeId::BIGINT:
		return "BIGINT";
	case LogicalTypeId::DATE:
		return "DATE";
	case LogicalTypeId::TIME:
		return "TIME";
	case LogicalTypeId::TIMESTAMP_SEC:
		return "TIMESTAMP_SEC";
	case LogicalTypeId::TIMESTAMP_MS:
		return "TIMESTAMP_MS";
	case LogicalTypeId::TIMESTAMP:
		return "TIMESTAMP";
	case LogicalTypeId::TIMESTAMP_NS:
		return "TIMESTAMP_NS";
	case LogicalTypeId::DECIMAL:
		return "DECIMAL";
	case LogicalTypeId::FLOAT:
		return "FLOAT";
	case LogicalTypeId::DOUBLE:
		return "DOUBLE";
	case LogicalTypeId::VARCHAR:
		return "VARCHAR";
	case LogicalTypeId::BLOB:
		return "BLOB";
	case LogicalTypeId::INTERVAL:
		return "INTERVAL";
	case LogicalTypeId::UTINYINT:
		return "UTINYINT";
	case LogicalTypeId::USMALLINT:
		return "USMALLINT";
	case LogicalTypeId::UINTEGER:
		return "UINTEGER";
	case LogicalTypeId::UBIGINT:
		return "UBIGINT";
	case LogicalTypeId::TIMESTAMP_TZ:
		return "TIMESTAMP_TZ";
	case LogicalTypeId::TIMESTAMP_TZ_NS:
		return "TIMESTAMP_TZ_NS";
	case LogicalTypeId::TIME_TZ:
		return "TIME_TZ";
	case LogicalTypeId::TIME_NS:
		return "TIME_NS";
	case LogicalTypeId::BIT:
		return "BIT";
	case LogicalTypeId::BIGNUM:
		return "BIGNUM";
	case LogicalTypeId::UHUGEINT:
		return "UHUGEINT";
	case LogicalTypeId::HUGEINT:
		return "HUGEINT";
	case LogicalTypeId::UUID:
		return "UUID";
	case LogicalTypeId::GEOMETRY:
		return "GEOMETRY";
	case LogicalTypeId::STRUCT:
		return "STRUCT";
	case LogicalTypeId::LIST:
		return "LIST";
	case LogicalTypeId::MAP:
		return "MAP";
	case LogicalTypeId::ENUM:
		return "ENUM";
	case LogicalTypeId::UNION:
		return "UNION";
	case LogicalTypeId::ARRAY:
		return "ARRAY";
	default:
		return "UNKNOWN";
	}
}

LogicalType CreateType(Factory &factory, LogicalTypeId id) {
	if (id == LogicalTypeId::SQLNULL) {
		// the type of an untyped NULL is not built by id
		return factory.ParseType("NULL");
	}
	return factory.CreateType(id);
}

string ValueToString(Factory &factory, const Value &value) {
	if (value.GetLogicalType().GetTypeId() == LogicalTypeId::VARCHAR) {
		return string(value.Get<cxx::varchar_t>().view());
	}
	auto text = value.Cast(factory, factory.CreateType(LogicalTypeId::VARCHAR));
	return string(text.Get<cxx::varchar_t>().view());
}

Value CreateVarchar(Factory &factory, const string &str) {
	cxx::ValidateUTF8(str);
	return Value::Create(factory, cxx::varchar_t(std::string_view(str)));
}

string RawErrorMessage(const std::exception &ex) {
	auto duckdb_ex = dynamic_cast<const cxx::Exception *>(&ex);
	if (duckdb_ex && !duckdb_ex->GetRawMessage().empty()) {
		return duckdb_ex->GetRawMessage();
	}
	return ex.what();
}

} // namespace duckdb_odbc
