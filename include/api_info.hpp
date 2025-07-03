#ifndef API_INFO_HPP
#define API_INFO_HPP

#include "duckdb.hpp"

#include <sql.h>
#include <sqltypes.h>
#include <sqlext.h>
#include <unordered_set>
#include <set>
#include "duckdb/common/vector.hpp"

#define NUM_FUNC_SUPPORTED 4000

namespace duckdb {

struct OdbcTypeInfo {
public:
	const char *type_name;
	const int data_type;
	const int column_size;
	const char *literal_prefix;
	const char *literal_suffix;
	const char *create_params;
	const int nullable;
	const int case_sensitive;
	const int searchable;
	const int unsigned_attribute;
	const int fixed_prec_scale;
	const int auto_unique_value;
	const char *local_type_name;
	const int minimum_scale;
	const int maximum_scale;
	const int sql_data_type;
	const int sql_datetime_sub;
	const int num_prec_radix;
	const int interval_precision;
};

struct ApiInfo {
private:
	// fill all supported functions in this array
	static const std::unordered_set<SQLUSMALLINT> BASE_SUPPORTED_FUNCTIONS;

	// fill ODBC3 supported functions in this array
	static const std::unordered_set<SQLUSMALLINT> ODBC3_EXTRA_SUPPORTED_FUNCTIONS;

	// static const std::unordered_set<SQLSMALLINT> ODBC_SUPPORTED_SQL_TYPES;

	static const vector<OdbcTypeInfo> ODBC_SUPPORTED_SQL_TYPES;

	static constexpr SQLINTEGER MAX_COLUMN_SIZE = (std::numeric_limits<SQLINTEGER>::max)();

	// MSDASQL provider uses these values to determine the max string size
	// when accessing DuckDB as linked ODBC source from MSSQL. Any values
	// over 8000 (max VARCHAR size in MSSQL without using VARCHAR(MAX))
	// causing MSDASQL to not be able read strings at all.
	static constexpr SQLINTEGER MAX_VARCHAR_COLUMN_SIZE = 8000;
	static constexpr SQLINTEGER MAX_VARBINARY_COLUMN_SIZE = MAX_VARCHAR_COLUMN_SIZE;

	static void SetFunctionSupported(SQLUSMALLINT *flags, int function_id);

public:
	static SQLRETURN GetFunctions(SQLHDBC connection_handle, SQLUSMALLINT function_id, SQLUSMALLINT *supported_ptr);

	static SQLRETURN GetFunctions30(SQLHDBC connection_handle, SQLUSMALLINT function_id, SQLUSMALLINT *supported_ptr);

	static SQLSMALLINT FindRelatedSQLType(duckdb::LogicalTypeId type_id);

	static void FindDataType(SQLSMALLINT data_type, vector<OdbcTypeInfo> &vec_types);

	static SQLLEN PointerSizeOf(SQLSMALLINT sql_type);

	static const vector<OdbcTypeInfo> &GetVectorTypesAddr();

	static void WriteInfoTypesToQueryString(const vector<OdbcTypeInfo> &vec_types, string &query);

	static bool IsNumericDescriptorField(SQLSMALLINT field_identifier);

	static bool IsNumericInfoType(SQLUSMALLINT info_type);

	static uint8_t GetTemporalPrecision(duckdb::LogicalTypeId type_id);

	static SQLINTEGER GetColumnSize(const duckdb::LogicalType &logical_type);

	static SQLINTEGER GetDisplaySize(const duckdb::LogicalType &logical_type);

}; // end ApiInfo struct

} // namespace duckdb

#endif // API_INFO_HPP
