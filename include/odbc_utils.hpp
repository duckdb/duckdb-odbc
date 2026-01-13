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
#include <utility>
#include <vector>

#include "duckdb/common/vector.hpp"

namespace duckdb {
struct OdbcUtils {
public:

	// Returns number of bytes written to the buffer
	static size_t WriteStringUtf8(const std::string &s, SQLCHAR *out_buf, size_t buf_len);

	// Returns number of chars written to the buffer and the utf16 conversion of the input string
	static std::pair<size_t, std::vector<SQLWCHAR>> WriteStringUtf16(const std::string &utf8_str, SQLWCHAR *out_buf, size_t buf_len_bytes);

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
	static std::string ConvertSQLCHARToString(SQLCHAR *str, const SQLINTEGER str_len);
	static LPCSTR ConvertStringToLPCSTR(const std::string &str);
	static SQLCHAR *ConvertStringToSQLCHAR(const std::string &str);

	static int64_t GetUTCOffsetMicrosFromOS(HSTMT hstmt, int64_t utc_micros);

	static std::string TrimString(const std::string& str);
};
} // namespace duckdb
#endif
