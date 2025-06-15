#ifndef ODBC_UTILS_HPP
#define ODBC_UTILS_HPP

// needs to be first because BOOL
#include "duckdb.hpp"

#include <cstdint>
#include <cstring>

#ifdef _WIN32
#define VC_EXTRALEAN
#define WIN32_LEAN_AND_MEAN
#define NOMINMAX
#include <Windows.h>
#undef CreateDirectory
#undef RemoveDirectory
#endif

#include <sql.h>
#include <sqltypes.h>
#include <string>

#include "duckdb/common/vector.hpp"

namespace duckdb {
struct OdbcUtils {
public:
	static std::string ReadString(const SQLPOINTER ptr, const SQLLEN len);

	template <typename INT_TYPE, typename CHAR_TYPE=SQLCHAR>
	static void WriteString(const std::string &s, CHAR_TYPE *out_buf, SQLLEN buf_len, INT_TYPE *out_len = nullptr) {
		INT_TYPE written_chars = 0;
		if (out_buf != nullptr) {
			written_chars = (INT_TYPE)snprintf((char *)out_buf, buf_len, "%s", s.c_str());
		}
		if (out_len != nullptr) {
			*out_len = written_chars;
		}
	}
	// overload for int to pass a null pointer
	static void WriteString(const std::string &s, SQLCHAR *out_buf, SQLLEN buf_len) {
		WriteString<int>(s, out_buf, buf_len, nullptr);
	}

	// overload for widechar and small int
	static void WriteString(const std::string &utf8_str, SQLWCHAR *out_buf, SQLLEN buf_len_bytes, SQLSMALLINT *out_len_bytes);

	// overload for widechar and int
	static void WriteString(const std::string &utf8_str, SQLWCHAR *out_buf, SQLLEN buf_len_bytes, SQLINTEGER *out_len_bytes);

	// overload for widechar and smallint with NULL-terminated vector input
	static void WriteString(const std::vector<SQLCHAR> &utf8_vec, std::size_t utf8_vec_len, SQLWCHAR *out_buf, SQLLEN buf_len_bytes, SQLSMALLINT *out_len_bytes);

	// overload for widechar and int with NULL-terminated vector input
	static void WriteString(const std::vector<SQLCHAR> &utf8_vec, std::size_t utf8_vec_len, SQLWCHAR *out_buf, SQLLEN buf_len_bytes, SQLINTEGER *out_len_bytes);

	template <typename VAL_INT_TYPE, typename LEN_INT_TYPE=SQLSMALLINT>
	static void StoreWithLength(VAL_INT_TYPE val, SQLPOINTER ptr, LEN_INT_TYPE *length_ptr) {
		size_t len = sizeof(VAL_INT_TYPE);
		if (ptr != nullptr) {
			std::memcpy(ptr, static_cast<void*>(&val), len);
		}
		if (length_ptr != nullptr) {
			*length_ptr = static_cast<LEN_INT_TYPE>(len);
		}
	}

	template <typename FIELD_TYPE>
	SQLRETURN IsValidPtrForSpecificedField(SQLPOINTER value_ptr, FIELD_TYPE target_field,
	                                       const vector<FIELD_TYPE> vec_field_ids) {
		for (auto field_id : vec_field_ids) {
			// target field doens't accept null_ptr
			if (field_id == target_field && value_ptr == nullptr) {
				return SQL_ERROR;
			}
		}
		return SQL_SUCCESS;
	}

	static bool IsCharType(SQLSMALLINT type);

	static SQLRETURN SetStringValueLength(const std::string &val_str, SQLLEN *str_len_or_ind_ptr);

	static std::string GetStringAsIdentifier(const std::string &str);
	static std::string ParseStringFilter(const std::string &filter_name, const std::string &filter_value,
	                                     SQLUINTEGER sql_attr_metadata_id, const std::string &coalesce_str = "");

	static std::string GetQueryDuckdbTables(const std::string &catalog_filter, const std::string &schema_filter, const std::string &table_filter,
	                                        const std::string &table_type_filter);
	static std::string GetQueryDuckdbColumns(const std::string &catalog_filter, const std::string &schema_filter,
	                                         const std::string &table_filter, const std::string &column_filter);

	static SQLUINTEGER SQLPointerToSQLUInteger(SQLPOINTER value);
	static std::string ConvertSQLCHARToString(SQLCHAR *str, SQLSMALLINT str_len);
	static LPCSTR ConvertStringToLPCSTR(const std::string &str);
	static SQLCHAR *ConvertStringToSQLCHAR(const std::string &str);

	static int64_t GetUTCOffsetMicrosFromOS(HSTMT hstmt, int64_t utc_micros);

	static std::string TrimString(const std::string& str);
};
} // namespace duckdb
#endif
