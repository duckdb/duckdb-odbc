#include "odbc_utils.hpp"
#include "duckdb/common/string_util.hpp"
#include "sqlext.h"

#include <sql.h>

#include <algorithm>
#include <mutex>
#include <regex>

#ifndef _WIN32
#include <time.h>
#endif //!_WIN32

#include "duckdb/common/vector.hpp"

#include "handle_functions.hpp"
#include "widechar.hpp"

using duckdb::OdbcUtils;
using duckdb::vector;

SQLRETURN OdbcUtils::SetStringValueLength(const string &val_str, SQLLEN *str_len_or_ind_ptr) {
	if (str_len_or_ind_ptr) {
		// it fills the required lenght from string value
		*str_len_or_ind_ptr = val_str.size();
		return SQL_SUCCESS;
	}
	// there is no length pointer
	return SQL_ERROR;
}

bool OdbcUtils::IsCharType(SQLSMALLINT type) {
	switch (type) {
	case SQL_CHAR:
	case SQL_WCHAR:
	case SQL_VARCHAR:
	case SQL_WVARCHAR:
	case SQL_LONGVARCHAR:
	case SQL_WLONGVARCHAR:
	case SQL_BINARY:
		return true;
	default:
		return false;
	}
}

std::string OdbcUtils::GetStringAsIdentifier(const string &str) {
	string str_ret;

	std::regex regex_str_quoted("^(\".*\"|\'.*\')$");

	if (std::regex_match(str, regex_str_quoted)) {
		// removing quotes
		str_ret = str_ret.substr(1, str_ret.size() - 2);
		// removing leading and trailing blanks
		str_ret = std::regex_replace(str, std::regex("^ +| +$|( ) +"), "$1");
	} else {
		// removing trailing blanks
		str_ret = std::regex_replace(str, std::regex(" +$|( ) +"), "$1");
		str_ret = duckdb::StringUtil::Upper(str_ret);
	}

	return str_ret;
}

std::string OdbcUtils::ParseStringFilter(const string &filter_name, const string &filter_value,
                                         SQLUINTEGER sql_attr_metadata_id, const string &coalesce_str) {
	string filter;
	if (filter_value.empty()) {
		if (coalesce_str.empty()) {
			filter = "COALESCE(" + filter_name + ",'') LIKE  '%'";
		} else {
			filter = "COALESCE(" + filter_name + ",'" + coalesce_str + "') LIKE '" + coalesce_str + "'";
		}
	} else if (sql_attr_metadata_id == SQL_TRUE) {
		filter = filter_name + "=" + OdbcUtils::GetStringAsIdentifier(filter_value);
	} else {
		filter = filter_name + " LIKE '" + filter_value + "'";
	}
	// Handle escape character passed by Power Query SDK
	filter += " ESCAPE '\\'";
	return filter;
}

std::string OdbcUtils::GetQueryDuckdbColumns(const string &catalog_filter, const string &schema_filter,
                                             const string &table_filter, const string &column_filter) {
	string sql_duckdb_columns = R"(
        SELECT * EXCLUDE (mapping, data_type_no_typmod)
        FROM (
            SELECT NULL::VARCHAR AS "TABLE_CAT",
            SCHEMA_NAME AS "TABLE_SCHEM",
            TABLE_NAME AS "TABLE_NAME",
            COLUMN_NAME AS "COLUMN_NAME",
            MAP {
                'BOOLEAN': 1, -- SQL_CHAR
                'TINYINT': -6, -- SQL_TINYINT
                'UTINYINT': -6, -- SQL_TINYINT
                'SMALLINT': 5, -- SQL_SMALLINT
                'USMALLINT': 5, -- SQL_SMALLINT
                'INTEGER': 4, -- SQL_INTEGER
                'UINTEGER': 4, -- SQL_INTEGER
                'BIGINT': -5, -- SQL_BIGINT
                'UBIGINT': -5, -- SQL_BIGINT
                'HUGEINT': 2, -- SQL_NUMERIC
                'UHUGEINT': 2, -- SQL_NUMERIC
                'FLOAT': 6, -- SQL_FLOAT
                'DOUBLE': 8, -- SQL_DOUBLE
                'DATE': 91, -- SQL_TYPE_DATE
                'TIME': 92, -- SQL_TYPE_TIME
                'VARCHAR': 12, -- SQL_VARCHAR
                'BLOB': -3, -- SQL_VARBINARY
                'INTERVAL': 10, -- SQL_INTERVAL
                'DECIMAL': 2, -- SQL_NUMERIC
                'BIT': -7, -- SQL_BIT
                'LIST': 12 -- SQL_VARCHAR
            } AS mapping,
            STRING_SPLIT(data_type, '(')[1] AS data_type_no_typmod,
            CASE
                -- value 93 is SQL_TIMESTAMP
                -- TODO: investigate why map key with spaces and regexes don't work
                WHEN data_type LIKE 'TIMESTAMP%' THEN 93::SMALLINT
                WHEN mapping[data_type_no_typmod] IS NOT NULL THEN mapping[data_type_no_typmod]::SMALLINT
                ELSE data_type_id::SMALLINT
            END AS "DATA_TYPE",
            CASE
                WHEN data_type_no_typmod = 'DECIMAL' THEN 'NUMERIC'
                WHEN data_type LIKE 'TIMESTAMP%' THEN 'TIMESTAMP'
                ELSE data_type_no_typmod
            END AS "TYPE_NAME",
            CASE
                WHEN data_type='DATE' THEN 12
                WHEN data_type='TIME' THEN 15
                WHEN data_type LIKE 'TIMESTAMP%' THEN 26
                WHEN data_type='CHAR'
                    OR data_type='BOOLEAN' THEN 1
                WHEN data_type='VARCHAR'
                    OR data_type='BLOB' THEN character_maximum_length
                WHEN data_type LIKE '%INT%' THEN numeric_precision
                WHEN data_type like 'DECIMAL%' THEN numeric_precision
                WHEN data_type='FLOAT'
                    OR data_type='DOUBLE' THEN numeric_precision
                ELSE NULL
            END AS "COLUMN_SIZE",
            CASE
                WHEN data_type='DATE' THEN 4
                WHEN data_type LIKE 'TIMESTAMP%' THEN 8
                WHEN data_type LIKE 'TIME%' THEN 8 
                WHEN data_type='CHAR'
                    OR data_type='BOOLEAN' THEN 1
                WHEN data_type='VARCHAR'
                    OR data_type='BLOB' THEN 16
                WHEN data_type LIKE '%TINYINT' THEN 1
                WHEN data_type LIKE '%SMALLINT' THEN 2
                WHEN data_type LIKE '%INTEGER' THEN 4
                WHEN data_type LIKE '%BIGINT' THEN 8
                WHEN data_type='HUGEINT' THEN 16
                WHEN data_type like 'DECIMAL%' THEN 16
                WHEN data_type='FLOAT' THEN 4
                WHEN data_type='DOUBLE' THEN 8
                ELSE NULL
            END AS "BUFFER_LENGTH",
            numeric_scale::SMALLINT AS "DECIMAL_DIGITS",
            numeric_precision_radix::SMALLINT "NUM_PREC_RADIX",
            CASE is_nullable
                WHEN FALSE THEN 0::SMALLINT
                WHEN TRUE THEN 1::SMALLINT
                ELSE 2::SMALLINT
            END AS "NULLABLE",
            '' AS "REMARKS",
            column_default AS "COLUMN_DEF",
            CASE
                WHEN data_type LIKE 'TIMESTAMP%' THEN 93::SMALLINT
                WHEN mapping[data_type_no_typmod] IS NOT NULL THEN mapping[data_type_no_typmod]::SMALLINT
                ELSE data_type_id::SMALLINT
            END AS "SQL_DATA_TYPE",
            CASE
                WHEN data_type='DATE' then 1::SMALLINT
                WHEN data_type LIKE 'TIMESTAMP%' THEN 3::SMALLINT
                WHEN data_type LIKE 'TIME%' THEN 2::SMALLINT
                ELSE NULL::SMALLINT
            END AS "SQL_DATETIME_SUB",
            CASE
                WHEN data_type='%CHAR'
                     OR data_type='BLOB' THEN character_maximum_length
                ELSE NULL
            END AS "CHAR_OCTET_LENGTH",
            column_index AS "ORDINAL_POSITION",
            CASE is_nullable
                WHEN FALSE THEN 'NO'
                WHEN TRUE THEN 'YES'
                ELSE ''
            END AS "IS_NULLABLE"
            FROM duckdb_columns
        )
	)";

	sql_duckdb_columns += " WHERE ";
	if (!catalog_filter.empty()) {
		sql_duckdb_columns += catalog_filter + " AND ";
	}
	if (!schema_filter.empty()) {
		sql_duckdb_columns += schema_filter + " AND ";
	}
	if (!table_filter.empty()) {
		sql_duckdb_columns += table_filter + " AND ";
	}
	sql_duckdb_columns += column_filter;

	// clang-format off
	sql_duckdb_columns += R"(
		ORDER BY
			"TABLE_CAT",
			"TABLE_SCHEM",
			"TABLE_NAME",
			"ORDINAL_POSITION"
	)";
	// clang-format on

	return sql_duckdb_columns;
}

std::string OdbcUtils::GetQueryDuckdbTables(const string &catalog_filter, const string &schema_filter,
                                            const string &table_filter, const string &table_type_filter) {
	string sql_duckdb_tables = R"(
		SELECT
			table_catalog::VARCHAR "TABLE_CAT",
			table_schema::VARCHAR "TABLE_SCHEM",
			table_name::VARCHAR "TABLE_NAME",
			CASE
				WHEN table_type='BASE TABLE'
				THEN 'TABLE'::VARCHAR
				ELSE table_type::VARCHAR
			END "TABLE_TYPE",
			''::VARCHAR "REMARKS"
			FROM information_schema.tables
	)";

	sql_duckdb_tables += " WHERE " + catalog_filter + "\n AND " + schema_filter + "\n AND " + table_filter;

	if (!table_type_filter.empty()) {
		if (table_type_filter != "'%'") {
			sql_duckdb_tables += "\n AND table_type IN (" + table_type_filter + ") ";
		}
	}

	sql_duckdb_tables += "\n AND table_catalog NOT LIKE '__ducklake_%' ";

	sql_duckdb_tables += "\n ORDER BY TABLE_TYPE, TABLE_CATALOG, TABLE_SCHEMA, TABLE_NAME";

	return sql_duckdb_tables;
}

SQLUINTEGER OdbcUtils::SQLPointerToSQLUInteger(SQLPOINTER value) {
	return static_cast<SQLUINTEGER>(reinterpret_cast<SQLULEN>(value));
}

std::string OdbcUtils::ConvertSQLCHARToString(SQLCHAR *str, SQLINTEGER str_len) {
	if (!str) {
		return std::string();
	}

	if (str_len == SQL_NTS) {
		return std::string(reinterpret_cast<char *>(str));
	}

	const size_t len = str_len > 0 ? static_cast<size_t>(str_len) : 0;
	return std::string(reinterpret_cast<char *>(str), len);
}

LPCSTR OdbcUtils::ConvertStringToLPCSTR(const std::string &str) {
	return reinterpret_cast<LPCSTR>(const_cast<char *>(str.c_str()));
}

SQLCHAR *OdbcUtils::ConvertStringToSQLCHAR(const std::string &str) {
	return reinterpret_cast<SQLCHAR *>(const_cast<char *>(str.c_str()));
}

template <typename INT_TYPE>
static void WriteStringInternal(const SQLCHAR *utf8_data, size_t utf8_data_len, SQLWCHAR *out_buf, SQLLEN buf_len,
                                INT_TYPE *out_len) {
	auto utf16_vec = duckdb::widechar::utf8_to_utf16_lenient(utf8_data, utf8_data_len);
	size_t buf_len_even = static_cast<size_t>(buf_len);
	if ((buf_len % 2) != 0) {
		buf_len_even -= 1;
	}
	if (buf_len_even <= sizeof(SQLWCHAR)) {
		if (out_buf != nullptr && buf_len_even == sizeof(SQLWCHAR)) {
			out_buf[0] = 0;
		}
		if (out_len != nullptr) {
			*out_len = 0;
			return;
		}
	}
	size_t len_bytes = std::min(utf16_vec.size() * sizeof(SQLWCHAR), buf_len_even - sizeof(SQLWCHAR));
	size_t len_chars = len_bytes / sizeof(SQLWCHAR);
	if (out_buf != nullptr) {
		std::memcpy(out_buf, utf16_vec.data(), len_bytes);
		out_buf[len_chars] = 0;
	}
	if (out_len != nullptr) {
		*out_len = static_cast<INT_TYPE>(len_bytes);
	}
}

void duckdb::OdbcUtils::WriteString(const std::string &utf8_str, SQLWCHAR *out_buf, SQLLEN buf_len_bytes,
                                    SQLSMALLINT *out_len_bytes) {
	return WriteStringInternal(reinterpret_cast<const SQLCHAR *>(utf8_str.c_str()), utf8_str.length(), out_buf,
	                           buf_len_bytes, out_len_bytes);
}

void duckdb::OdbcUtils::WriteString(const std::string &utf8_str, SQLWCHAR *out_buf, SQLLEN buf_len_bytes,
                                    SQLINTEGER *out_len_bytes) {
	return WriteStringInternal(reinterpret_cast<const SQLCHAR *>(utf8_str.c_str()), utf8_str.length(), out_buf,
	                           buf_len_bytes, out_len_bytes);
}

void duckdb::OdbcUtils::WriteString(const std::vector<SQLCHAR> &utf8_vec, size_t utf8_vec_len, SQLWCHAR *out_buf,
                                    SQLLEN buf_len_bytes, SQLSMALLINT *out_len_bytes) {
	return WriteStringInternal(utf8_vec.data(), utf8_vec_len, out_buf, buf_len_bytes, out_len_bytes);
}

void duckdb::OdbcUtils::WriteString(const std::vector<SQLCHAR> &utf8_vec, size_t utf8_vec_len, SQLWCHAR *out_buf,
                                    SQLLEN buf_len_bytes, SQLINTEGER *out_len_bytes) {
	return WriteStringInternal(utf8_vec.data(), utf8_vec_len, out_buf, buf_len_bytes, out_len_bytes);
}

int64_t duckdb::OdbcUtils::GetUTCOffsetMicrosFromOS(HSTMT hstmt_ptr, int64_t utc_micros) {

	// Casting here to not complicate header dependencies
	auto hstmt = reinterpret_cast<duckdb::OdbcHandleStmt *>(hstmt_ptr);

#ifdef _WIN32

	// Convert microseconds to seconds for SYSTEMTIME
	int64_t seconds_since_epoch = utc_micros / 1000000;

	// Convert seconds to FILETIME
	FILETIME ft_utc;
	ULARGE_INTEGER utc_time;
	// Add Windows epoch offset
	utc_time.QuadPart = static_cast<uint64_t>(seconds_since_epoch) * 10000000 + 11644473600LL * 10000000;
	ft_utc.dwLowDateTime = utc_time.LowPart;
	ft_utc.dwHighDateTime = utc_time.HighPart;

	// Convert FILETIME to SYSTEMTIME
	SYSTEMTIME utc_system_time;
	auto res_fttt = FileTimeToSystemTime(&ft_utc, &utc_system_time);
	if (res_fttt == 0) {
		std::string msg = "FileTimeToSystemTime failed, input value: " + std::to_string(utc_micros) +
		                  ", error code: " + std::to_string(GetLastError());
		duckdb::SetDiagnosticRecord(hstmt, SQL_ERROR, "timezone", msg, duckdb::SQLStateType::ST_HY000,
		                            hstmt->dbc->GetDataSourceName());
		return 0;
	}

	// Get the default time zone information
	TIME_ZONE_INFORMATION tz_info;
	DWORD res_tzinfo = GetTimeZoneInformation(&tz_info);
	if (res_tzinfo == TIME_ZONE_ID_INVALID) {
		std::string msg = "GetTimeZoneInformation failed, input value: " + std::to_string(utc_micros) +
		                  ", error code: " + std::to_string(GetLastError());
		duckdb::SetDiagnosticRecord(hstmt, SQL_ERROR, "timezone", msg, duckdb::SQLStateType::ST_HY000,
		                            hstmt->dbc->GetDataSourceName());
		return 0;
	}

	// Convert UTC SYSTEMTIME to local SYSTEMTIME
	SYSTEMTIME local_system_time;
	auto res_tzspec = SystemTimeToTzSpecificLocalTime(&tz_info, &utc_system_time, &local_system_time);
	if (res_tzspec == 0) {
		std::string msg = "SystemTimeToTzSpecificLocalTime failed, input value: " + std::to_string(utc_micros) +
		                  ", error code: " + std::to_string(GetLastError());
		duckdb::SetDiagnosticRecord(hstmt, SQL_ERROR, "timezone", msg, duckdb::SQLStateType::ST_HY000,
		                            hstmt->dbc->GetDataSourceName());
		return 0;
	}

	// Convert local SYSTEMTIME back to FILETIME
	FILETIME ft_local;
	auto res_sttft = SystemTimeToFileTime(&local_system_time, &ft_local);
	if (res_sttft == 0) {
		std::string msg = "SystemTimeToFileTime failed, input value: " + std::to_string(utc_micros) +
		                  ", error code: " + std::to_string(GetLastError());
		duckdb::SetDiagnosticRecord(hstmt, SQL_ERROR, "timezone", msg, duckdb::SQLStateType::ST_HY000,
		                            hstmt->dbc->GetDataSourceName());
		return 0;
	}

	// Convert both FILETIMEs to ULARGE_INTEGER for arithmetic
	ULARGE_INTEGER local_time;
	local_time.LowPart = ft_local.dwLowDateTime;
	local_time.HighPart = ft_local.dwHighDateTime;

	// Calculate the offset in microseconds
	return static_cast<int64_t>(local_time.QuadPart - utc_time.QuadPart) / 10;

#else //!_WiN32

	// Convert microseconds to seconds
	time_t utc_seconds_since_epoch = utc_micros / 1000000;

	// Break down UTC time into a tm struct
	struct tm utc_time_struct;
	// Thread-safe UTC conversion
	auto res_gmt = gmtime_r(&utc_seconds_since_epoch, &utc_time_struct);
	if (res_gmt == nullptr) {
		std::string msg =
		    "gmtime_r failed, input value: " + std::to_string(utc_micros) + ", error code: " + std::to_string(errno);
		duckdb::SetDiagnosticRecord(hstmt, SQL_ERROR, "timezone", msg, duckdb::SQLStateType::ST_HY000,
		                            hstmt->dbc->GetDataSourceName());
		return 0;
	}

	// Convert UTC time to local time in the default time zone
	struct tm local_time_struct;
	// Thread-safe local time conversion
	auto res_local = localtime_r(&utc_seconds_since_epoch, &local_time_struct);
	if (res_local == nullptr) {
		std::string msg =
		    "localtime_r failed, input value: " + std::to_string(utc_micros) + ", error code: " + std::to_string(errno);
		duckdb::SetDiagnosticRecord(hstmt, SQL_ERROR, "timezone", msg, duckdb::SQLStateType::ST_HY000,
		                            hstmt->dbc->GetDataSourceName());
		return 0;
	}
	// DST confuses mktime
	local_time_struct.tm_isdst = 0;

	// Convert broken-down times to time_t (seconds since epoch)
	time_t local_time = mktime(&local_time_struct);
	if (local_time == -1) {
		std::string msg = "mktime local failed, input value: " + std::to_string(utc_micros) +
		                  ", error code: " + std::to_string(errno);
		duckdb::SetDiagnosticRecord(hstmt, SQL_ERROR, "timezone", msg, duckdb::SQLStateType::ST_HY000,
		                            hstmt->dbc->GetDataSourceName());
		return 0;
	}
	time_t utc_time = mktime(&utc_time_struct);
	if (utc_time == -1) {
		std::string msg =
		    "mktime utc failed, input value: " + std::to_string(utc_micros) + ", error code: " + std::to_string(errno);
		duckdb::SetDiagnosticRecord(hstmt, SQL_ERROR, "timezone", msg, duckdb::SQLStateType::ST_HY000,
		                            hstmt->dbc->GetDataSourceName());
		return 0;
	}

	// Calculate the offset in seconds
	int64_t offset_seconds = static_cast<int64_t>(difftime(local_time, utc_time));

	// Convert offset to microseconds
	return offset_seconds * 1000000;

#endif // _WIN32
}

static bool IsSpace(char c) {
	return c == ' ' || c == '\t' || c == '\n' || c == '\r' || c == '\f' || c == '\v';
}

std::string duckdb::OdbcUtils::TrimString(const std::string &str) {
	auto front = std::find_if_not(str.begin(), str.end(), IsSpace);
	auto back = std::find_if_not(str.rbegin(), str.rend(), IsSpace).base();
	return (back <= front ? std::string() : std::string(front, back));
}
