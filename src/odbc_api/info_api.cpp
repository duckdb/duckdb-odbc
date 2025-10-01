#include "driver.hpp"
#include "duckdb_odbc.hpp"
#include "api_info.hpp"
#include "odbc_diagnostic.hpp"
#include "odbc_utils.hpp"
#include "handle_functions.hpp"
#include "statement_functions.hpp"
#include "widechar.hpp"

#include "duckdb/common/helper.hpp"

// From ODBC Spec (ODBCVER >= 0x0300 and ODBCVER >= 0x0400)
// https://github.com/microsoft/ODBC-Specification/blob/master/Windows/inc/sqlext.h
// Needed for use with Power Query SDK and Power BI
#define SQL_DTC_TRANSACTION_COST 1750
#define SQL_RETURN_ESCAPE_CLAUSE 180

using duckdb::ApiInfo;
using duckdb::OdbcTypeInfo;
using duckdb::OdbcUtils;
using duckdb::SQLStateType;
using duckdb::vector;
using std::ptrdiff_t;

//===--------------------------------------------------------------------===//
// SQLGetInfo
//===--------------------------------------------------------------------===//

template <typename CHAR_TYPE>
static SQLRETURN GetInfoInternal(SQLHDBC connection_handle, SQLUSMALLINT info_type, SQLPOINTER info_value_ptr,
                                 SQLSMALLINT buffer_length, SQLSMALLINT *string_length_ptr) {

	// https://docs.microsoft.com/en-us/sql/odbc/reference/syntax/sqlgetinfo-function?view=sql-server-ver15

	// verify numeric info value type and null value pointer
	if (duckdb::ApiInfo::IsNumericInfoType(info_type) && info_value_ptr == nullptr) {
		duckdb::OdbcHandleDbc *dbc = nullptr;
		SQLRETURN ret = ConvertConnection(connection_handle, dbc);
		if (ret != SQL_SUCCESS) {
			return ret;
		}

		return duckdb::SetDiagnosticRecord(dbc, SQL_ERROR, "SQLGetInfo",
		                                   "Invalid null value pointer for numeric info type.", SQLStateType::ST_HY024,
		                                   dbc->GetDataSourceName());
	}

	// Default strings: YES or NO
	std::string yes_str("Y");
	std::string no_str("N");

	switch (info_type) {
	case SQL_ACCESSIBLE_PROCEDURES: {
		duckdb::OdbcUtils::WriteString(no_str, reinterpret_cast<CHAR_TYPE *>(info_value_ptr), buffer_length,
		                               string_length_ptr);
		return SQL_SUCCESS;
	}
	case SQL_ACCESSIBLE_TABLES: {
		duckdb::OdbcUtils::WriteString(yes_str, reinterpret_cast<CHAR_TYPE *>(info_value_ptr), buffer_length,
		                               string_length_ptr);
		return SQL_SUCCESS;
	}
	case SQL_ACTIVE_ENVIRONMENTS: {
		duckdb::OdbcUtils::StoreWithLength<SQLUSMALLINT>(0, info_value_ptr, string_length_ptr);
		return SQL_SUCCESS;
	}
	case SQL_AGGREGATE_FUNCTIONS: {
		SQLUINTEGER mask =
		    SQL_AF_ALL | SQL_AF_AVG | SQL_AF_COUNT | SQL_AF_DISTINCT | SQL_AF_MAX | SQL_AF_MIN | SQL_AF_SUM;
		duckdb::OdbcUtils::StoreWithLength<SQLUINTEGER>(mask, info_value_ptr, string_length_ptr);
		return SQL_SUCCESS;
	}
	case SQL_ALTER_DOMAIN: {
		duckdb::OdbcUtils::StoreWithLength<SQLUINTEGER>(0, info_value_ptr, string_length_ptr);
		return SQL_SUCCESS;
	}
	case SQL_ALTER_TABLE: {
		// options suppoerd by the DuckDB's tables
		SQLUINTEGER mask = SQL_AT_ADD_COLUMN_COLLATION | SQL_AT_ADD_COLUMN_DEFAULT | SQL_AT_ADD_COLUMN_SINGLE |
		                   SQL_AT_ADD_CONSTRAINT | SQL_AT_ADD_TABLE_CONSTRAINT | SQL_AT_DROP_COLUMN_DEFAULT |
		                   SQL_AT_SET_COLUMN_DEFAULT;
		duckdb::OdbcUtils::StoreWithLength<SQLUINTEGER>(mask, info_value_ptr, string_length_ptr);
		return SQL_SUCCESS;
	}
	case SQL_ASYNC_DBC_FUNCTIONS: {
		duckdb::OdbcUtils::StoreWithLength<SQLUINTEGER>(SQL_ASYNC_DBC_NOT_CAPABLE, info_value_ptr, string_length_ptr);
		return SQL_SUCCESS;
	}
	case SQL_ASYNC_MODE: {
		duckdb::OdbcUtils::StoreWithLength<SQLUINTEGER>(SQL_AM_NONE, info_value_ptr, string_length_ptr);
		return SQL_SUCCESS;
	}
#ifdef SQL_ASYNC_NOTIFICATION
	case SQL_ASYNC_NOTIFICATION: {
		duckdb::OdbcUtils::StoreWithLength<SQLUINTEGER>(SQL_ASYNC_NOTIFICATION_NOT_CAPABLE, info_value_ptr,
		                                                string_length_ptr);
		return SQL_SUCCESS;
	}
#endif
	case SQL_BATCH_ROW_COUNT: {
		duckdb::OdbcUtils::StoreWithLength<SQLUINTEGER>(SQL_BRC_EXPLICIT, info_value_ptr, string_length_ptr);
		return SQL_SUCCESS;
	}
	case SQL_BATCH_SUPPORT: {
		SQLUINTEGER mask = SQL_BS_SELECT_EXPLICIT | SQL_BS_ROW_COUNT_EXPLICIT;
		duckdb::OdbcUtils::StoreWithLength<SQLUINTEGER>(mask, info_value_ptr, string_length_ptr);
		return SQL_SUCCESS;
	}
	case SQL_BOOKMARK_PERSISTENCE: {
		// we do not support bookmark, it's is implicit in DuckDB
		/* SQL_BP_CLOSE |
		 * SQL_BP_DELETE |
		 * SQL_BP_DROP |
		 * SQL_BP_OTHER_HSTMT |
		 * SQL_BP_TRANSACTION |
		 * SQL_BP_UPDATE */
		return SQL_SUCCESS;
	}
	case SQL_CATALOG_LOCATION: {
		duckdb::OdbcUtils::StoreWithLength<SQLUSMALLINT>(SQL_CL_START, info_value_ptr, string_length_ptr);
		return SQL_SUCCESS;
	}
	case SQL_CATALOG_NAME: {
		duckdb::OdbcUtils::WriteString(yes_str, reinterpret_cast<CHAR_TYPE *>(info_value_ptr), buffer_length,
		                               string_length_ptr);
		return SQL_SUCCESS;
	}
	case SQL_CATALOG_NAME_SEPARATOR: {
		std::string cat_separator(".");
		duckdb::OdbcUtils::WriteString(cat_separator, reinterpret_cast<CHAR_TYPE *>(info_value_ptr), buffer_length,
		                               string_length_ptr);
		return SQL_SUCCESS;
	}
	case SQL_CATALOG_TERM: {
		std::string empty_str("");
		duckdb::OdbcUtils::WriteString(empty_str, reinterpret_cast<CHAR_TYPE *>(info_value_ptr), buffer_length,
		                               string_length_ptr);
		return SQL_SUCCESS;
	}
	case SQL_CATALOG_USAGE: {
		SQLUINTEGER mask = SQL_CU_DML_STATEMENTS | SQL_CU_TABLE_DEFINITION;
		/* | SQL_CU_PRIVILEGE_DEFINITION |
		 * SQL_CU_INDEX_DEFINITION
		 * SQL_CU_PROCEDURE_INVOCATION |
		 */
		duckdb::OdbcUtils::StoreWithLength<SQLUINTEGER>(mask, info_value_ptr, string_length_ptr);
		return SQL_SUCCESS;
	}
	case SQL_COLLATION_SEQ: {
		std::string default_collation("UTF-8");
		duckdb::OdbcUtils::WriteString(default_collation, reinterpret_cast<CHAR_TYPE *>(info_value_ptr), buffer_length,
		                               string_length_ptr);
		return SQL_SUCCESS;
	}
	case SQL_COLUMN_ALIAS: {
		duckdb::OdbcUtils::WriteString(yes_str, reinterpret_cast<CHAR_TYPE *>(info_value_ptr), buffer_length,
		                               string_length_ptr);
		return SQL_SUCCESS;
	}
	case SQL_CONCAT_NULL_BEHAVIOR: {
		duckdb::OdbcUtils::StoreWithLength<SQLUSMALLINT>(SQL_CB_NON_NULL, info_value_ptr, string_length_ptr);
		return SQL_SUCCESS;
	}
	// basically, we used the same conversion rules from MonetBD, it's needed to be tested
	case SQL_CONVERT_TINYINT:
	case SQL_CONVERT_SMALLINT:
	case SQL_CONVERT_INTEGER:
	case SQL_CONVERT_BIGINT: {
		SQLUINTEGER mask = SQL_CVT_BIGINT | SQL_CVT_BIT | SQL_CVT_CHAR | SQL_CVT_DECIMAL | SQL_CVT_DOUBLE |
		                   SQL_CVT_FLOAT | SQL_CVT_INTEGER | SQL_CVT_LONGVARCHAR | SQL_CVT_NUMERIC | SQL_CVT_REAL |
		                   SQL_CVT_SMALLINT | SQL_CVT_TINYINT | SQL_CVT_VARCHAR;

		duckdb::OdbcUtils::StoreWithLength<SQLUINTEGER>(mask, info_value_ptr, string_length_ptr);
		return SQL_SUCCESS;
	}
	case SQL_CONVERT_BINARY:
	case SQL_CONVERT_LONGVARBINARY:
	case SQL_CONVERT_VARBINARY: {
		SQLUINTEGER mask = SQL_CVT_BINARY | SQL_CVT_CHAR | SQL_CVT_LONGVARBINARY | SQL_CVT_LONGVARCHAR |
		                   SQL_CVT_VARBINARY | SQL_CVT_VARCHAR;

		duckdb::OdbcUtils::StoreWithLength<SQLUINTEGER>(mask, info_value_ptr, string_length_ptr);
		return SQL_SUCCESS;
	}
	case SQL_CONVERT_BIT: {
		SQLUINTEGER mask = SQL_CVT_BIGINT | SQL_CVT_BIT | SQL_CVT_CHAR | SQL_CVT_INTEGER | SQL_CVT_LONGVARCHAR |
		                   SQL_CVT_SMALLINT | SQL_CVT_TINYINT | SQL_CVT_VARCHAR;

		duckdb::OdbcUtils::StoreWithLength<SQLUINTEGER>(mask, info_value_ptr, string_length_ptr);
		return SQL_SUCCESS;
	}
	case SQL_CONVERT_CHAR:
	case SQL_CONVERT_VARCHAR:
	case SQL_CONVERT_LONGVARCHAR: {
		SQLUINTEGER mask = SQL_CVT_BIGINT | SQL_CVT_BINARY | SQL_CVT_BIT | SQL_CVT_CHAR | SQL_CVT_DATE |
		                   SQL_CVT_DECIMAL | SQL_CVT_DOUBLE | SQL_CVT_FLOAT | SQL_CVT_INTEGER |
		                   SQL_CVT_INTERVAL_DAY_TIME | SQL_CVT_INTERVAL_YEAR_MONTH | SQL_CVT_LONGVARBINARY |
		                   SQL_CVT_LONGVARCHAR | SQL_CVT_NUMERIC | SQL_CVT_REAL | SQL_CVT_SMALLINT | SQL_CVT_TIME |
		                   SQL_CVT_TIMESTAMP | SQL_CVT_TINYINT | SQL_CVT_VARBINARY | SQL_CVT_VARCHAR;

		duckdb::OdbcUtils::StoreWithLength<SQLUINTEGER>(mask, info_value_ptr, string_length_ptr);
		return SQL_SUCCESS;
	}
	case SQL_CONVERT_GUID: {
		duckdb::OdbcUtils::StoreWithLength<SQLUINTEGER>(0, info_value_ptr, string_length_ptr);
		return SQL_SUCCESS;
	}
	case SQL_CONVERT_DATE: {
		SQLUINTEGER mask = SQL_CVT_CHAR | SQL_CVT_DATE | SQL_CVT_LONGVARCHAR | SQL_CVT_TIMESTAMP | SQL_CVT_VARCHAR;
		duckdb::OdbcUtils::StoreWithLength<SQLUINTEGER>(mask, info_value_ptr, string_length_ptr);
		return SQL_SUCCESS;
	}
	case SQL_CONVERT_DECIMAL:
	case SQL_CONVERT_NUMERIC: {
		SQLUINTEGER mask = SQL_CVT_BIGINT | SQL_CVT_CHAR | SQL_CVT_DECIMAL | SQL_CVT_DOUBLE | SQL_CVT_FLOAT |
		                   SQL_CVT_INTEGER | SQL_CVT_INTERVAL_DAY_TIME | SQL_CVT_LONGVARCHAR | SQL_CVT_NUMERIC |
		                   SQL_CVT_REAL | SQL_CVT_SMALLINT | SQL_CVT_TINYINT | SQL_CVT_VARCHAR;
		duckdb::OdbcUtils::StoreWithLength<SQLUINTEGER>(mask, info_value_ptr, string_length_ptr);
		return SQL_SUCCESS;
	}
	case SQL_CONVERT_DOUBLE:
	case SQL_CONVERT_REAL:
	case SQL_CONVERT_FLOAT: {
		SQLUINTEGER mask = SQL_CVT_BIGINT | SQL_CVT_CHAR | SQL_CVT_DECIMAL | SQL_CVT_DOUBLE | SQL_CVT_FLOAT |
		                   SQL_CVT_INTEGER | SQL_CVT_LONGVARCHAR | SQL_CVT_NUMERIC | SQL_CVT_REAL | SQL_CVT_SMALLINT |
		                   SQL_CVT_TINYINT | SQL_CVT_VARCHAR;
		duckdb::OdbcUtils::StoreWithLength<SQLUINTEGER>(mask, info_value_ptr, string_length_ptr);
		return SQL_SUCCESS;
	}
	case SQL_CONVERT_INTERVAL_DAY_TIME: {
		SQLUINTEGER mask = SQL_CVT_BIGINT | SQL_CVT_CHAR | SQL_CVT_INTEGER | SQL_CVT_INTERVAL_DAY_TIME |
		                   SQL_CVT_LONGVARCHAR | SQL_CVT_SMALLINT | SQL_CVT_TIME | SQL_CVT_TINYINT | SQL_CVT_VARCHAR;
		duckdb::OdbcUtils::StoreWithLength<SQLUINTEGER>(mask, info_value_ptr, string_length_ptr);
		return SQL_SUCCESS;
	}
	case SQL_CONVERT_INTERVAL_YEAR_MONTH: {
		SQLUINTEGER mask = SQL_CVT_BIGINT | SQL_CVT_CHAR | SQL_CVT_INTEGER | SQL_CVT_INTERVAL_YEAR_MONTH |
		                   SQL_CVT_LONGVARCHAR | SQL_CVT_SMALLINT | SQL_CVT_TINYINT | SQL_CVT_VARCHAR;
		duckdb::OdbcUtils::StoreWithLength<SQLUINTEGER>(mask, info_value_ptr, string_length_ptr);
		return SQL_SUCCESS;
	}
	case SQL_CONVERT_TIME: {
		SQLUINTEGER mask =
		    SQL_CVT_CHAR | SQL_CVT_INTERVAL_DAY_TIME | SQL_CVT_LONGVARCHAR | SQL_CVT_TIME | SQL_CVT_VARCHAR;
		duckdb::OdbcUtils::StoreWithLength<SQLUINTEGER>(mask, info_value_ptr, string_length_ptr);
		return SQL_SUCCESS;
	}
	case SQL_CONVERT_TIMESTAMP: {
		SQLUINTEGER mask =
		    SQL_CVT_CHAR | SQL_CVT_DATE | SQL_CVT_LONGVARCHAR | SQL_CVT_TIME | SQL_CVT_TIMESTAMP | SQL_CVT_VARCHAR;
		duckdb::OdbcUtils::StoreWithLength<SQLUINTEGER>(mask, info_value_ptr, string_length_ptr);
		return SQL_SUCCESS;
	}
		// end conversion rules

	case SQL_CONVERT_FUNCTIONS: {
		duckdb::OdbcUtils::StoreWithLength<SQLUINTEGER>(SQL_FN_CVT_CAST, info_value_ptr, string_length_ptr);
		return SQL_SUCCESS;
	}
	case SQL_CORRELATION_NAME: {
		duckdb::OdbcUtils::StoreWithLength<SQLUSMALLINT>(SQL_CN_ANY, info_value_ptr, string_length_ptr);
		return SQL_SUCCESS;
	}
	case SQL_CONVERT_WCHAR:
	case SQL_CONVERT_WLONGVARCHAR:
	case SQL_CONVERT_WVARCHAR:
	case SQL_CREATE_ASSERTION:
	case SQL_CREATE_CHARACTER_SET:
	case SQL_CREATE_COLLATION:
	case SQL_CREATE_DOMAIN:
	case SQL_CREATE_TRANSLATION: {
		// "0" means that the statement is not supported.
		duckdb::OdbcUtils::StoreWithLength<SQLUINTEGER>(0, info_value_ptr, string_length_ptr);
		return SQL_SUCCESS;
	}
	case SQL_CREATE_SCHEMA: {
		duckdb::OdbcUtils::StoreWithLength<SQLUINTEGER>(SQL_CS_CREATE_SCHEMA, info_value_ptr, string_length_ptr);
		return SQL_SUCCESS;
	}
	case SQL_CREATE_TABLE: {
		SQLUINTEGER mask = SQL_CT_COLUMN_CONSTRAINT | SQL_CT_COLUMN_DEFAULT | SQL_CT_CONSTRAINT_NAME_DEFINITION |
		                   SQL_CT_CREATE_TABLE | SQL_CT_LOCAL_TEMPORARY | SQL_CT_TABLE_CONSTRAINT |
		                   SQL_CT_COLUMN_COLLATION;
		duckdb::OdbcUtils::StoreWithLength<SQLUINTEGER>(mask, info_value_ptr, string_length_ptr);
		return SQL_SUCCESS;
	}
	case SQL_CREATE_VIEW: {
		SQLUINTEGER mask = SQL_CV_CREATE_VIEW | SQL_CV_CHECK_OPTION;
		duckdb::OdbcUtils::StoreWithLength<SQLUINTEGER>(mask, info_value_ptr, string_length_ptr);
		return SQL_SUCCESS;
	}
	case SQL_CURSOR_COMMIT_BEHAVIOR: {
		duckdb::OdbcUtils::StoreWithLength<SQLUSMALLINT>(SQL_CB_PRESERVE, info_value_ptr, string_length_ptr);
		return SQL_SUCCESS;
	}
	case SQL_CURSOR_ROLLBACK_BEHAVIOR: {
		duckdb::OdbcUtils::StoreWithLength<SQLUSMALLINT>(SQL_CB_CLOSE, info_value_ptr, string_length_ptr);
		return SQL_SUCCESS;
	}
	case SQL_CURSOR_SENSITIVITY: {
		duckdb::OdbcUtils::StoreWithLength<SQLUINTEGER>(SQL_INSENSITIVE, info_value_ptr, string_length_ptr);
		return SQL_SUCCESS;
	}
	case SQL_DATA_SOURCE_NAME: {
		duckdb::OdbcHandleDbc *dbc = nullptr;
		SQLRETURN ret = ConvertConnection(connection_handle, dbc);
		if (ret != SQL_SUCCESS) {
			return ret;
		}

		duckdb::OdbcUtils::WriteString(dbc->GetDataSourceName(), reinterpret_cast<CHAR_TYPE *>(info_value_ptr),
		                               buffer_length, string_length_ptr);
		return SQL_SUCCESS;
	}
	case SQL_DATA_SOURCE_READ_ONLY: {
		duckdb::OdbcUtils::WriteString(no_str, reinterpret_cast<CHAR_TYPE *>(info_value_ptr), buffer_length,
		                               string_length_ptr);
		return SQL_SUCCESS;
	}
	case SQL_DATABASE_NAME: {
		duckdb::OdbcHandleDbc *dbc = nullptr;
		SQLRETURN ret = ConvertConnection(connection_handle, dbc);
		if (ret != SQL_SUCCESS) {
			return ret;
		}

		std::string db_name = dbc->GetDatabaseName();
		if (db_name == IN_MEMORY_PATH) {
			db_name = "";
		}
		duckdb::OdbcUtils::WriteString(db_name, reinterpret_cast<CHAR_TYPE *>(info_value_ptr), buffer_length,
		                               string_length_ptr);
		return SQL_SUCCESS;
	}
	case SQL_DATETIME_LITERALS: {
		SQLUINTEGER mask = SQL_DL_SQL92_DATE | SQL_DL_SQL92_TIME | SQL_DL_SQL92_TIMESTAMP | SQL_DL_SQL92_INTERVAL_YEAR |
		                   SQL_DL_SQL92_INTERVAL_MONTH | SQL_DL_SQL92_INTERVAL_DAY | SQL_DL_SQL92_INTERVAL_HOUR |
		                   SQL_DL_SQL92_INTERVAL_MINUTE | SQL_DL_SQL92_INTERVAL_SECOND |
		                   SQL_DL_SQL92_INTERVAL_YEAR_TO_MONTH | SQL_DL_SQL92_INTERVAL_DAY_TO_HOUR |
		                   SQL_DL_SQL92_INTERVAL_DAY_TO_MINUTE | SQL_DL_SQL92_INTERVAL_DAY_TO_SECOND |
		                   SQL_DL_SQL92_INTERVAL_HOUR_TO_MINUTE | SQL_DL_SQL92_INTERVAL_HOUR_TO_SECOND |
		                   SQL_DL_SQL92_INTERVAL_MINUTE_TO_SECOND;

		duckdb::OdbcUtils::StoreWithLength<SQLUINTEGER>(mask, info_value_ptr, string_length_ptr);
		return SQL_SUCCESS;
	}
	case SQL_DRIVER_NAME:
	case SQL_DBMS_NAME: {
		std::string dbname = "DuckDB";
		duckdb::OdbcUtils::WriteString(dbname, reinterpret_cast<CHAR_TYPE *>(info_value_ptr), buffer_length,
		                               string_length_ptr);
		return SQL_SUCCESS;
	}
	case SQL_DBMS_VER: {
		SQLHDBC stmt;

		SQLRETURN ret = SQLAllocHandle(SQL_HANDLE_STMT, connection_handle, &stmt);
		if (!SQL_SUCCEEDED(ret)) {
			duckdb::FreeHandle(SQL_HANDLE_STMT, stmt);
			return ret;
		}

		ret = SQLExecDirect(stmt, (SQLCHAR *)"SELECT library_version FROM pragma_version()", SQL_NTS);
		if (!SQL_SUCCEEDED(ret)) {
			duckdb::FreeHandle(SQL_HANDLE_STMT, stmt);
			return ret;
		}

		ret = SQLFetch(stmt);
		if (!SQL_SUCCEEDED(ret)) {
			duckdb::FreeHandle(SQL_HANDLE_STMT, stmt);
			return ret;
		}

		if (string_length_ptr) {
			SQLLEN len_ptr;
			ret = SQLGetData(stmt, 1, SQL_C_CHAR, info_value_ptr, buffer_length, &len_ptr);
			*string_length_ptr = static_cast<SQLSMALLINT>(len_ptr);
		} else {
			ret = SQLGetData(stmt, 1, SQL_C_CHAR, info_value_ptr, buffer_length, nullptr);
		}
		if (!SQL_SUCCEEDED(ret)) {
			duckdb::FreeHandle(SQL_HANDLE_STMT, stmt);
			return ret;
		}

		duckdb::FreeHandle(SQL_HANDLE_STMT, stmt);
		return SQL_SUCCESS;
	}
	case SQL_DDL_INDEX: {
		duckdb::OdbcUtils::StoreWithLength<SQLUINTEGER>(0, info_value_ptr, string_length_ptr);
		return SQL_SUCCESS;
	}
	case SQL_DEFAULT_TXN_ISOLATION: {
		duckdb::OdbcUtils::StoreWithLength<SQLUINTEGER>(SQL_TXN_SERIALIZABLE, info_value_ptr, string_length_ptr);
		return SQL_SUCCESS;
	}
	case SQL_DESCRIBE_PARAMETER: {
		duckdb::OdbcUtils::WriteString(yes_str, reinterpret_cast<CHAR_TYPE *>(info_value_ptr), buffer_length,
		                               string_length_ptr);
		return SQL_SUCCESS;
	}
	case SQL_DM_VER: {
		std::string odbc_major = std::to_string(SQL_SPEC_MAJOR);
		std::string odbc_minor = std::to_string(SQL_SPEC_MINOR);
		// this doesn't seem to be so relevant
		std::string dm_build_version = ".####.####";

		std::string dm_version(odbc_major + "." + odbc_minor + dm_build_version);
		duckdb::OdbcUtils::WriteString(dm_version, reinterpret_cast<CHAR_TYPE *>(info_value_ptr), buffer_length,
		                               string_length_ptr);
		return SQL_SUCCESS;
	}
#ifdef SQL_DRIVER_AWARE_POOLING_SUPPORTED
	case SQL_DRIVER_AWARE_POOLING_SUPPORTED: {
		duckdb::OdbcUtils::StoreWithLength<SQLUINTEGER>(SQL_DRIVER_AWARE_POOLING_NOT_CAPABLE, info_value_ptr,
		                                                string_length_ptr);
		return SQL_SUCCESS;
	}
#endif
		// weird info types ("This information type is implemented by the Driver Manager alone.")
		// case SQL_DRIVER_HDBCSQL_DRIVER_HENV:
		// case SQL_DRIVER_HDESC:
		// case SQL_DRIVER_HSTMT:

	case SQL_DRIVER_ODBC_VER: {
		std::string driver_ver = "03.51";
		duckdb::OdbcUtils::WriteString(driver_ver, reinterpret_cast<CHAR_TYPE *>(info_value_ptr), buffer_length,
		                               string_length_ptr);
		return SQL_SUCCESS;
	}
	case SQL_DRIVER_VER: {
		std::string driver_ver = "03.51.0000";
		duckdb::OdbcUtils::WriteString(driver_ver, reinterpret_cast<CHAR_TYPE *>(info_value_ptr), buffer_length,
		                               string_length_ptr);
		return SQL_SUCCESS;
	}
	case SQL_DROP_ASSERTION: {
		duckdb::OdbcUtils::StoreWithLength<SQLUINTEGER>(0, info_value_ptr, string_length_ptr);
		return SQL_SUCCESS;
	}
	case SQL_DROP_CHARACTER_SET: {
		duckdb::OdbcUtils::StoreWithLength<SQLUINTEGER>(0, info_value_ptr, string_length_ptr);
		return SQL_SUCCESS;
	}
	case SQL_DROP_COLLATION: {
		duckdb::OdbcUtils::StoreWithLength<SQLUINTEGER>(0, info_value_ptr, string_length_ptr);
		return SQL_SUCCESS;
	}
	case SQL_DROP_DOMAIN: {
		duckdb::OdbcUtils::StoreWithLength<SQLUINTEGER>(0, info_value_ptr, string_length_ptr);
		return SQL_SUCCESS;
	}
	case SQL_DROP_SCHEMA: {
		SQLUINTEGER mask = SQL_DS_DROP_SCHEMA | SQL_DS_CASCADE | SQL_DS_RESTRICT;
		duckdb::OdbcUtils::StoreWithLength<SQLUINTEGER>(mask, info_value_ptr, string_length_ptr);
		return SQL_SUCCESS;
	}
	case SQL_DROP_TABLE: {
		SQLUINTEGER mask = SQL_DT_DROP_TABLE | SQL_DT_CASCADE | SQL_DT_RESTRICT;
		duckdb::OdbcUtils::StoreWithLength<SQLUINTEGER>(mask, info_value_ptr, string_length_ptr);
		return SQL_SUCCESS;
	}
	case SQL_DROP_TRANSLATION: {
		duckdb::OdbcUtils::StoreWithLength<SQLUINTEGER>(0, info_value_ptr, string_length_ptr);
		return SQL_SUCCESS;
	}
	case SQL_DROP_VIEW: {
		SQLUINTEGER mask = SQL_DV_DROP_VIEW | SQL_DV_CASCADE | SQL_DV_RESTRICT;
		duckdb::OdbcUtils::StoreWithLength<SQLUINTEGER>(mask, info_value_ptr, string_length_ptr);
		return SQL_SUCCESS;
	}
	case SQL_DYNAMIC_CURSOR_ATTRIBUTES1: {
		SQLUINTEGER mask = SQL_CA1_ABSOLUTE | SQL_CA1_NEXT | SQL_CA1_RELATIVE;
		duckdb::OdbcUtils::StoreWithLength<SQLUINTEGER>(mask, info_value_ptr, string_length_ptr);
		return SQL_SUCCESS;
	}
	case SQL_DYNAMIC_CURSOR_ATTRIBUTES2: {
		duckdb::OdbcUtils::StoreWithLength<SQLUINTEGER>(0, info_value_ptr, string_length_ptr);
		return SQL_SUCCESS;
	}
	case SQL_EXPRESSIONS_IN_ORDERBY: {
		duckdb::OdbcUtils::WriteString(yes_str, reinterpret_cast<CHAR_TYPE *>(info_value_ptr), buffer_length,
		                               string_length_ptr);
		return SQL_SUCCESS;
	}
	case SQL_FILE_USAGE: {
		duckdb::OdbcUtils::StoreWithLength<SQLUSMALLINT>(SQL_FILE_NOT_SUPPORTED, info_value_ptr, string_length_ptr);
		return SQL_SUCCESS;
	}
	case SQL_FORWARD_ONLY_CURSOR_ATTRIBUTES1: {
		duckdb::OdbcUtils::StoreWithLength<SQLUINTEGER>(SQL_CA1_NEXT, info_value_ptr, string_length_ptr);
		return SQL_SUCCESS;
	}
	case SQL_FORWARD_ONLY_CURSOR_ATTRIBUTES2: {
		duckdb::OdbcUtils::StoreWithLength<SQLUINTEGER>(0, info_value_ptr, string_length_ptr);
		return SQL_SUCCESS;
	}
	case SQL_GETDATA_EXTENSIONS: {
		SQLUINTEGER mask = SQL_GD_ANY_COLUMN | SQL_GD_ANY_ORDER | SQL_GD_BOUND | SQL_GD_BLOCK;
		duckdb::OdbcUtils::StoreWithLength<SQLUINTEGER>(mask, info_value_ptr, string_length_ptr);
		return SQL_SUCCESS;
	}
	case SQL_GROUP_BY: {
		duckdb::OdbcUtils::StoreWithLength<SQLUSMALLINT>(SQL_GB_NO_RELATION, info_value_ptr, string_length_ptr);
		return SQL_SUCCESS;
	}
	case SQL_IDENTIFIER_CASE: {
		duckdb::OdbcUtils::StoreWithLength<SQLUSMALLINT>(SQL_IC_LOWER, info_value_ptr, string_length_ptr);
		return SQL_SUCCESS;
	}
	case SQL_IDENTIFIER_QUOTE_CHAR: {
		std::string quote_char("\"");
		duckdb::OdbcUtils::WriteString(quote_char, reinterpret_cast<CHAR_TYPE *>(info_value_ptr), buffer_length,
		                               string_length_ptr);
		return SQL_SUCCESS;
	}
	case SQL_INDEX_KEYWORDS: {
		duckdb::OdbcUtils::StoreWithLength<SQLUINTEGER>(SQL_IK_NONE, info_value_ptr, string_length_ptr);
		return SQL_SUCCESS;
	}
	case SQL_INFO_SCHEMA_VIEWS: {
		duckdb::OdbcUtils::StoreWithLength<SQLUINTEGER>(0, info_value_ptr, string_length_ptr);
		return SQL_SUCCESS;
	}
	case SQL_INSERT_STATEMENT: {
		duckdb::OdbcUtils::StoreWithLength<SQLUINTEGER>(SQL_IS_INSERT_LITERALS, info_value_ptr, string_length_ptr);
		return SQL_SUCCESS;
	}
	case SQL_INTEGRITY: {
		duckdb::OdbcUtils::WriteString(no_str, reinterpret_cast<CHAR_TYPE *>(info_value_ptr), buffer_length,
		                               string_length_ptr);
		return SQL_SUCCESS;
	}
	case SQL_KEYSET_CURSOR_ATTRIBUTES1: {
		SQLUINTEGER mask = SQL_CA1_ABSOLUTE | SQL_CA1_NEXT | SQL_CA1_RELATIVE;
		duckdb::OdbcUtils::StoreWithLength<SQLUINTEGER>(mask, info_value_ptr, string_length_ptr);
		return SQL_SUCCESS;
	}
	case SQL_KEYSET_CURSOR_ATTRIBUTES2: {
		duckdb::OdbcUtils::StoreWithLength<SQLUINTEGER>(0, info_value_ptr, string_length_ptr);
		return SQL_SUCCESS;
	}
	case SQL_KEYWORDS: {
		SQLHSTMT hstmt;

		SQLRETURN ret = SQLAllocHandle(SQL_HANDLE_STMT, connection_handle, &hstmt);
		if (!SQL_SUCCEEDED(ret)) {
			duckdb::FreeHandle(SQL_HANDLE_STMT, hstmt);
			return ret;
		}
		ret = SQLExecDirect(
		    hstmt, (SQLCHAR *)"SELECT keyword_name FROM duckdb_keywords() WHERE keyword_category='reserved'", SQL_NTS);
		if (!SQL_SUCCEEDED(ret)) {
			duckdb::FreeHandle(SQL_HANDLE_STMT, hstmt);
			return ret;
		}

		const size_t keyword_size = 100;
		SQLCHAR *keyword = (SQLCHAR *)malloc(sizeof(SQLCHAR) * keyword_size);
		std::string reserved_keywords;
		while ((ret = SQLFetch(hstmt)) != SQL_NO_DATA) {
			if (!SQL_SUCCEEDED(ret)) {
				duckdb::FreeHandle(SQL_HANDLE_STMT, hstmt);
				free(keyword);
				return ret;
			}
			ret = SQLGetData(hstmt, 1, SQL_C_CHAR, keyword, keyword_size, nullptr);
			if (!SQL_SUCCEEDED(ret)) {
				duckdb::FreeHandle(SQL_HANDLE_STMT, hstmt);
				free(keyword);
				return ret;
			}
			reserved_keywords += std::string((char *)keyword) + ",";
		}
		if (reserved_keywords.empty()) {
			// remove last inserted comma
			reserved_keywords.pop_back();
		}
		free(keyword);
		duckdb::FreeHandle(SQL_HANDLE_STMT, hstmt);

		duckdb::OdbcUtils::WriteString(reserved_keywords, reinterpret_cast<CHAR_TYPE *>(info_value_ptr), buffer_length,
		                               string_length_ptr);
		return SQL_SUCCESS;
	}
	case SQL_LIKE_ESCAPE_CLAUSE: {
		duckdb::OdbcUtils::WriteString(yes_str, reinterpret_cast<CHAR_TYPE *>(info_value_ptr), buffer_length,
		                               string_length_ptr);
		return SQL_SUCCESS;
	}
	case SQL_MAX_ASYNC_CONCURRENT_STATEMENTS:
	case SQL_MAX_BINARY_LITERAL_LEN:
	case SQL_MAX_CHAR_LITERAL_LEN:
	case SQL_MAX_INDEX_SIZE:
	case SQL_MAX_ROW_SIZE:
	case SQL_MAX_STATEMENT_LEN: {
		duckdb::OdbcUtils::StoreWithLength<SQLUINTEGER>(0, info_value_ptr, string_length_ptr);
		return SQL_SUCCESS;
	}
	case SQL_MAX_CATALOG_NAME_LEN:
	case SQL_MAX_COLUMN_NAME_LEN:
	case SQL_MAX_COLUMNS_IN_GROUP_BY:
	case SQL_MAX_COLUMNS_IN_INDEX:
	case SQL_MAX_COLUMNS_IN_ORDER_BY:
	case SQL_MAX_COLUMNS_IN_SELECT:
	case SQL_MAX_COLUMNS_IN_TABLE:
	case SQL_MAX_CONCURRENT_ACTIVITIES:
	case SQL_MAX_CURSOR_NAME_LEN:
	case SQL_MAX_IDENTIFIER_LEN:
	case SQL_MAX_PROCEDURE_NAME_LEN:
	case SQL_MAX_SCHEMA_NAME_LEN:
	case SQL_MAX_TABLE_NAME_LEN:
	case SQL_MAX_TABLES_IN_SELECT:
	case SQL_MAX_USER_NAME_LEN: {
		duckdb::OdbcUtils::StoreWithLength<SQLUSMALLINT>(0, info_value_ptr, string_length_ptr);
		return SQL_SUCCESS;
	}
	case SQL_MAX_DRIVER_CONNECTIONS: {
		// Set in 1, maximum number of active connections
		duckdb::OdbcUtils::StoreWithLength<SQLUSMALLINT>(1, info_value_ptr, string_length_ptr);
		return SQL_SUCCESS;
	}
	case SQL_MAX_ROW_SIZE_INCLUDES_LONG: {
		duckdb::OdbcUtils::WriteString(yes_str, reinterpret_cast<CHAR_TYPE *>(info_value_ptr), buffer_length,
		                               string_length_ptr);
		return SQL_SUCCESS;
	}
	case SQL_MULT_RESULT_SETS: {
		// saying NO beucase of SQLite
		duckdb::OdbcUtils::WriteString(no_str, reinterpret_cast<CHAR_TYPE *>(info_value_ptr), buffer_length,
		                               string_length_ptr);
		return SQL_SUCCESS;
	}
	case SQL_MULTIPLE_ACTIVE_TXN: {
		duckdb::OdbcUtils::WriteString(yes_str, reinterpret_cast<CHAR_TYPE *>(info_value_ptr), buffer_length,
		                               string_length_ptr);
		return SQL_SUCCESS;
	}
	case SQL_NEED_LONG_DATA_LEN: {
		duckdb::OdbcUtils::WriteString(no_str, reinterpret_cast<CHAR_TYPE *>(info_value_ptr), buffer_length,
		                               string_length_ptr);
		return SQL_SUCCESS;
	}
	case SQL_NON_NULLABLE_COLUMNS: {
		duckdb::OdbcUtils::StoreWithLength<SQLUSMALLINT>(SQL_NNC_NON_NULL, info_value_ptr, string_length_ptr);
		return SQL_SUCCESS;
	}
	case SQL_NULL_COLLATION: {
		duckdb::OdbcUtils::StoreWithLength<SQLUSMALLINT>(SQL_NC_START, info_value_ptr, string_length_ptr);
		return SQL_SUCCESS;
	}
	case SQL_NUMERIC_FUNCTIONS: {
		SQLUINTEGER mask = SQL_FN_NUM_ABS | SQL_FN_NUM_ACOS | SQL_FN_NUM_ASIN | SQL_FN_NUM_ATAN | SQL_FN_NUM_ATAN2 |
		                   SQL_FN_NUM_CEILING | SQL_FN_NUM_COS | SQL_FN_NUM_COT | SQL_FN_NUM_DEGREES | SQL_FN_NUM_EXP |
		                   SQL_FN_NUM_FLOOR | SQL_FN_NUM_LOG | SQL_FN_NUM_LOG10 | SQL_FN_NUM_MOD | SQL_FN_NUM_PI |
		                   SQL_FN_NUM_POWER | SQL_FN_NUM_RADIANS | SQL_FN_NUM_ROUND | SQL_FN_NUM_SIGN | SQL_FN_NUM_SIN |
		                   SQL_FN_NUM_SQRT | SQL_FN_NUM_TAN;

		duckdb::OdbcUtils::StoreWithLength<SQLUINTEGER>(mask, info_value_ptr, string_length_ptr);
		return SQL_SUCCESS;
	}
	case SQL_ODBC_INTERFACE_CONFORMANCE: {
		duckdb::OdbcUtils::StoreWithLength<SQLUINTEGER>(SQL_OIC_CORE, info_value_ptr, string_length_ptr);
		return SQL_SUCCESS;
	}
	case SQL_ODBC_API_CONFORMANCE: {
		duckdb::OdbcUtils::StoreWithLength<SQLSMALLINT>(SQL_OAC_LEVEL1, info_value_ptr, string_length_ptr);
		return SQL_SUCCESS;
	}
	//  This is implemented only in the Driver Manager
	// case SQL_ODBC_VER:
	case SQL_OJ_CAPABILITIES: {
		SQLUINTEGER mask = SQL_OJ_LEFT | SQL_OJ_RIGHT | SQL_OJ_FULL | SQL_OJ_INNER | SQL_OJ_ALL_COMPARISON_OPS;
		duckdb::OdbcUtils::StoreWithLength<SQLUINTEGER>(mask, info_value_ptr, string_length_ptr);
		return SQL_SUCCESS;
	}
	case SQL_ORDER_BY_COLUMNS_IN_SELECT: {
		duckdb::OdbcUtils::WriteString(yes_str, reinterpret_cast<CHAR_TYPE *>(info_value_ptr), buffer_length,
		                               string_length_ptr);
		return SQL_SUCCESS;
	}
	case SQL_PARAM_ARRAY_ROW_COUNTS: {
		duckdb::OdbcUtils::StoreWithLength<SQLUINTEGER>(SQL_PARC_BATCH, info_value_ptr, string_length_ptr);
		return SQL_SUCCESS;
	}
	case SQL_PARAM_ARRAY_SELECTS: {
		duckdb::OdbcUtils::StoreWithLength<SQLUINTEGER>(SQL_PAS_BATCH, info_value_ptr, string_length_ptr);
		return SQL_SUCCESS;
	}
	case SQL_POS_OPERATIONS: {
		duckdb::OdbcUtils::StoreWithLength<SQLUINTEGER>(0, info_value_ptr, string_length_ptr);
		return SQL_SUCCESS;
	}
	case SQL_PROCEDURE_TERM: {
		duckdb::OdbcUtils::WriteString("", reinterpret_cast<CHAR_TYPE *>(info_value_ptr), buffer_length,
		                               string_length_ptr);
		return SQL_SUCCESS;
	}
	case SQL_PROCEDURES: {
		duckdb::OdbcUtils::WriteString(no_str, reinterpret_cast<CHAR_TYPE *>(info_value_ptr), buffer_length,
		                               string_length_ptr);
		return SQL_SUCCESS;
	}
	case SQL_QUOTED_IDENTIFIER_CASE: {
		duckdb::OdbcUtils::StoreWithLength<SQLUSMALLINT>(SQL_IC_SENSITIVE, info_value_ptr, string_length_ptr);
		return SQL_SUCCESS;
	}
	case SQL_ROW_UPDATES: {
		duckdb::OdbcUtils::WriteString(no_str, reinterpret_cast<CHAR_TYPE *>(info_value_ptr), buffer_length,
		                               string_length_ptr);
		return SQL_SUCCESS;
	}
	case SQL_SCHEMA_TERM: {
		duckdb::OdbcUtils::WriteString("schema", reinterpret_cast<CHAR_TYPE *>(info_value_ptr), buffer_length,
		                               string_length_ptr);
		return SQL_SUCCESS;
	}
	case SQL_SCHEMA_USAGE: {
		SQLUINTEGER mask = SQL_SU_DML_STATEMENTS | SQL_SU_TABLE_DEFINITION;
		duckdb::OdbcUtils::StoreWithLength<SQLUINTEGER>(mask, info_value_ptr, string_length_ptr);
		return SQL_SUCCESS;
	}
	case SQL_SCROLL_OPTIONS: {
		duckdb::OdbcUtils::StoreWithLength<SQLUINTEGER>(SQL_SCROLL_OPTIONS, info_value_ptr, string_length_ptr);
		return SQL_SUCCESS;
	}
	case SQL_SEARCH_PATTERN_ESCAPE: {
		duckdb::OdbcUtils::WriteString("\\", reinterpret_cast<CHAR_TYPE *>(info_value_ptr), buffer_length,
		                               string_length_ptr);
		return SQL_SUCCESS;
	}
	case SQL_SERVER_NAME: {
		duckdb::OdbcUtils::WriteString("", reinterpret_cast<CHAR_TYPE *>(info_value_ptr), buffer_length,
		                               string_length_ptr);
		return SQL_SUCCESS;
	}
	case SQL_SPECIAL_CHARACTERS: {
		duckdb::OdbcUtils::WriteString("!%&'()*+,-./;:<=>?@[]^{}|~", reinterpret_cast<CHAR_TYPE *>(info_value_ptr),
		                               buffer_length, string_length_ptr);
		return SQL_SUCCESS;
	}
	case SQL_SQL_CONFORMANCE: {
		duckdb::OdbcUtils::StoreWithLength<SQLUINTEGER>(SQL_SC_SQL92_ENTRY, info_value_ptr, string_length_ptr);
		return SQL_SUCCESS;
	}
	case SQL_SQL92_DATETIME_FUNCTIONS: {
		SQLUINTEGER mask = SQL_SDF_CURRENT_DATE | SQL_SDF_CURRENT_TIME | SQL_SDF_CURRENT_TIMESTAMP;
		duckdb::OdbcUtils::StoreWithLength<SQLUINTEGER>(mask, info_value_ptr, string_length_ptr);
		return SQL_SUCCESS;
	}
	case SQL_SQL92_FOREIGN_KEY_DELETE_RULE:
	case SQL_SQL92_FOREIGN_KEY_UPDATE_RULE:
	case SQL_SQL92_GRANT: {
		duckdb::OdbcUtils::StoreWithLength<SQLUINTEGER>(0, info_value_ptr, string_length_ptr);
		return SQL_SUCCESS;
	}
	case SQL_SQL92_NUMERIC_VALUE_FUNCTIONS: {
		SQLUINTEGER mask = SQL_SNVF_BIT_LENGTH | SQL_SNVF_EXTRACT | SQL_SNVF_OCTET_LENGTH | SQL_SNVF_POSITION;
		duckdb::OdbcUtils::StoreWithLength<SQLUINTEGER>(mask, info_value_ptr, string_length_ptr);
		return SQL_SUCCESS;
	}
	case SQL_SQL92_PREDICATES: {
		SQLUINTEGER mask = SQL_SP_BETWEEN | SQL_SP_COMPARISON | SQL_SP_EXISTS | SQL_SP_IN | SQL_SP_ISNOTNULL |
		                   SQL_SP_ISNULL | SQL_SP_LIKE;
		duckdb::OdbcUtils::StoreWithLength<SQLUINTEGER>(mask, info_value_ptr, string_length_ptr);
		return SQL_SUCCESS;
	}
	case SQL_SQL92_RELATIONAL_JOIN_OPERATORS: {
		SQLUINTEGER mask = SQL_SRJO_CROSS_JOIN | SQL_SRJO_FULL_OUTER_JOIN | SQL_SRJO_INNER_JOIN |
		                   SQL_SRJO_LEFT_OUTER_JOIN | SQL_SRJO_NATURAL_JOIN | SQL_SRJO_RIGHT_OUTER_JOIN;
		duckdb::OdbcUtils::StoreWithLength<SQLUINTEGER>(mask, info_value_ptr, string_length_ptr);
		return SQL_SUCCESS;
	}
	case SQL_SQL92_REVOKE: {
		duckdb::OdbcUtils::StoreWithLength<SQLUINTEGER>(0, info_value_ptr, string_length_ptr);
		return SQL_SUCCESS;
	}
	case SQL_SQL92_ROW_VALUE_CONSTRUCTOR: {
		SQLUINTEGER mask = SQL_SRVC_VALUE_EXPRESSION | SQL_SRVC_NULL | SQL_SRVC_DEFAULT | SQL_SRVC_ROW_SUBQUERY;
		duckdb::OdbcUtils::StoreWithLength<SQLUINTEGER>(mask, info_value_ptr, string_length_ptr);
		return SQL_SUCCESS;
	}
	case SQL_SQL92_STRING_FUNCTIONS: {
		SQLUINTEGER mask = SQL_SSF_CONVERT | SQL_SSF_LOWER | SQL_SSF_UPPER | SQL_SSF_SUBSTRING | SQL_SSF_TRIM_BOTH |
		                   SQL_SSF_TRIM_LEADING | SQL_SSF_TRIM_TRAILING;
		duckdb::OdbcUtils::StoreWithLength<SQLUINTEGER>(mask, info_value_ptr, string_length_ptr);
		return SQL_SUCCESS;
	}
	case SQL_SQL92_VALUE_EXPRESSIONS: {
		SQLUINTEGER mask = SQL_SVE_CASE | SQL_SVE_CAST | SQL_SVE_COALESCE | SQL_SVE_NULLIF;
		duckdb::OdbcUtils::StoreWithLength<SQLUINTEGER>(mask, info_value_ptr, string_length_ptr);
		return SQL_SUCCESS;
	}
	case SQL_STANDARD_CLI_CONFORMANCE: {
		// do we comply with SQL_SCC_XOPEN_CLI_VERSION1 | SQL_SCC_ISO92_CLI ??
		duckdb::OdbcUtils::StoreWithLength<SQLUINTEGER>(0, info_value_ptr, string_length_ptr);
		return SQL_SUCCESS;
	}
	case SQL_STATIC_CURSOR_ATTRIBUTES1: {
		SQLUINTEGER mask = SQL_CA1_NEXT | SQL_CA1_ABSOLUTE | SQL_CA1_RELATIVE;
		duckdb::OdbcUtils::StoreWithLength<SQLUINTEGER>(mask, info_value_ptr, string_length_ptr);
		return SQL_SUCCESS;
	}
	case SQL_STATIC_CURSOR_ATTRIBUTES2: {
		duckdb::OdbcUtils::StoreWithLength<SQLUINTEGER>(0, info_value_ptr, string_length_ptr);
		return SQL_SUCCESS;
	}
	case SQL_STRING_FUNCTIONS: {
		SQLUINTEGER mask = SQL_FN_STR_ASCII | SQL_FN_STR_BIT_LENGTH | SQL_FN_STR_CONCAT | SQL_FN_STR_LCASE |
		                   SQL_FN_STR_LEFT | SQL_FN_STR_LENGTH | SQL_FN_STR_LOCATE | SQL_FN_STR_LTRIM |
		                   SQL_FN_STR_REPEAT | SQL_FN_STR_REPLACE | SQL_FN_STR_RIGHT | SQL_FN_STR_RTRIM |
		                   SQL_FN_STR_SUBSTRING | SQL_FN_STR_UCASE;
		duckdb::OdbcUtils::StoreWithLength<SQLUINTEGER>(mask, info_value_ptr, string_length_ptr);
		return SQL_SUCCESS;
	}
	case SQL_SUBQUERIES: {
		SQLUINTEGER mask =
		    SQL_SQ_CORRELATED_SUBQUERIES | SQL_SQ_COMPARISON | SQL_SQ_EXISTS | SQL_SQ_IN | SQL_SQ_QUANTIFIED;
		duckdb::OdbcUtils::StoreWithLength<SQLUINTEGER>(mask, info_value_ptr, string_length_ptr);
		return SQL_SUCCESS;
	}
	case SQL_SYSTEM_FUNCTIONS: {
		duckdb::OdbcUtils::StoreWithLength<SQLUINTEGER>(0, info_value_ptr, string_length_ptr);
		return SQL_SUCCESS;
	}
	case SQL_TABLE_TERM: {
		duckdb::OdbcUtils::WriteString("table", reinterpret_cast<CHAR_TYPE *>(info_value_ptr), buffer_length,
		                               string_length_ptr);
		return SQL_SUCCESS;
	}
	case SQL_TIMEDATE_ADD_INTERVALS:
	case SQL_TIMEDATE_DIFF_INTERVALS: {
		SQLUINTEGER mask = SQL_FN_TSI_FRAC_SECOND | SQL_FN_TSI_SECOND | SQL_FN_TSI_MINUTE | SQL_FN_TSI_HOUR |
		                   SQL_FN_TSI_DAY | SQL_FN_TSI_WEEK | SQL_FN_TSI_MONTH | SQL_FN_TSI_QUARTER | SQL_FN_TSI_YEAR;
		duckdb::OdbcUtils::StoreWithLength<SQLUINTEGER>(mask, info_value_ptr, string_length_ptr);
		return SQL_SUCCESS;
	}
	case SQL_TIMEDATE_FUNCTIONS: {
		SQLUINTEGER mask = SQL_FN_TD_CURRENT_DATE | SQL_FN_TD_CURRENT_TIME | SQL_FN_TD_CURRENT_TIMESTAMP |
		                   SQL_FN_TD_DAYNAME | SQL_FN_TD_DAYOFMONTH | SQL_FN_TD_DAYOFWEEK | SQL_FN_TD_DAYOFYEAR |
		                   SQL_FN_TD_EXTRACT | SQL_FN_TD_HOUR | SQL_FN_TD_MINUTE | SQL_FN_TD_MONTH |
		                   SQL_FN_TD_MONTHNAME | SQL_FN_TD_NOW | SQL_FN_TD_QUARTER | SQL_FN_TD_SECOND | SQL_FN_TD_WEEK |
		                   SQL_FN_TD_YEAR;
		duckdb::OdbcUtils::StoreWithLength<SQLUINTEGER>(mask, info_value_ptr, string_length_ptr);
		return SQL_SUCCESS;
	}
	case SQL_TXN_CAPABLE: {
		duckdb::OdbcUtils::StoreWithLength<SQLUSMALLINT>(SQL_TC_ALL, info_value_ptr, string_length_ptr);
		return SQL_SUCCESS;
	}
	case SQL_TXN_ISOLATION_OPTION: {
		duckdb::OdbcUtils::StoreWithLength<SQLUINTEGER>(SQL_TXN_SERIALIZABLE, info_value_ptr, string_length_ptr);
		return SQL_SUCCESS;
	}
	case SQL_UNION: {
		SQLUINTEGER mask = SQL_U_UNION | SQL_U_UNION_ALL;
		duckdb::OdbcUtils::StoreWithLength<SQLUINTEGER>(mask, info_value_ptr, string_length_ptr);
		return SQL_SUCCESS;
	}
	case SQL_USER_NAME:
	case SQL_XOPEN_CLI_YEAR: {
		duckdb::OdbcUtils::WriteString("", reinterpret_cast<CHAR_TYPE *>(info_value_ptr), buffer_length,
		                               string_length_ptr);
		return SQL_SUCCESS;
	}
	case SQL_DTC_TRANSACTION_COST: {
		duckdb::OdbcUtils::StoreWithLength<SQLUINTEGER>(0, info_value_ptr, string_length_ptr);
		return SQL_SUCCESS;
	}
	case SQL_RETURN_ESCAPE_CLAUSE: {
		duckdb::OdbcUtils::StoreWithLength<SQLUINTEGER>(0, info_value_ptr, string_length_ptr);
		return SQL_SUCCESS;
	}
	default:
		duckdb::OdbcHandleDbc *dbc = nullptr;
		SQLRETURN ret = ConvertConnection(connection_handle, dbc);
		if (ret != SQL_SUCCESS) {
			return ret;
		}

		// return SQL_SUCCESS, but with a record message
		std::string msg = "Unrecognized attribute: " + std::to_string(info_type);
		return duckdb::SetDiagnosticRecord(dbc, SQL_SUCCESS, "SQLGetInfo", msg, SQLStateType::ST_HY092,
		                                   dbc->GetDataSourceName());
	}
}

/**
 * <a
 * href="https://learn.microsoft.com/en-us/sql/odbc/reference/syntax/sqlgetinfo-function?view=sql-server-ver16">Docs</a>
 */
SQLRETURN SQL_API SQLGetInfo(SQLHDBC connection_handle, SQLUSMALLINT info_type, SQLPOINTER info_value_ptr,
                             SQLSMALLINT buffer_length, SQLSMALLINT *string_length_ptr) {
	return GetInfoInternal<SQLCHAR>(connection_handle, info_type, info_value_ptr, buffer_length, string_length_ptr);
}

/**
 * Wide char version
 * <a
 * href="https://learn.microsoft.com/en-us/sql/odbc/reference/syntax/sqlgetinfo-function?view=sql-server-ver16">Docs</a>
 */
SQLRETURN SQL_API SQLGetInfoW(SQLHDBC connection_handle, SQLUSMALLINT info_type, SQLPOINTER info_value_ptr,
                              SQLSMALLINT buffer_length, SQLSMALLINT *string_length_ptr) {
	return GetInfoInternal<SQLWCHAR>(connection_handle, info_type, info_value_ptr, buffer_length, string_length_ptr);
}

//===--------------------------------------------------------------------===//
// SQLGetFunctions
//===--------------------------------------------------------------------===//

/**
 * <a
 * href="https://learn.microsoft.com/en-us/sql/odbc/reference/syntax/sqlgetfunctions-function?view=sql-server-ver16">Docs</a>
 */
SQLRETURN SQL_API SQLGetFunctions(SQLHDBC connection_handle, SQLUSMALLINT function_id, SQLUSMALLINT *supported_ptr) {
	if (function_id == SQL_API_ODBC3_ALL_FUNCTIONS) {
		return ApiInfo::GetFunctions30(connection_handle, function_id, supported_ptr);
	} else {
		return ApiInfo::GetFunctions(connection_handle, function_id, supported_ptr);
	}
}

//===--------------------------------------------------------------------===//
// SQLGetTypeInfo
//===--------------------------------------------------------------------===//

static SQLRETURN GetTypeInfoInternal(SQLHSTMT statement_handle, SQLSMALLINT data_type) {
	duckdb::OdbcHandleStmt *hstmt = nullptr;
	SQLRETURN ret = ConvertHSTMT(statement_handle, hstmt);
	if (ret != SQL_SUCCESS) {
		return ret;
	}

	std::string query("SELECT * FROM (VALUES ");

	if (data_type == SQL_ALL_TYPES) {
		auto vec_types = ApiInfo::GetVectorTypesAddr();
		ApiInfo::WriteInfoTypesToQueryString(vec_types, query);
	} else {
		vector<duckdb::OdbcTypeInfo> vec_types;
		ApiInfo::FindDataType(data_type, vec_types);
		ApiInfo::WriteInfoTypesToQueryString(vec_types, query);
	}

	// clang-format off
	query += R"(
	) AS odbc_types (
		"TYPE_NAME",
		"DATA_TYPE",
		"COLUMN_SIZE",
		"LITERAL_PREFIX",
		"LITERAL_SUFFIX",
		"CREATE_PARAMS",
		"NULLABLE",
		"CASE_SENSITIVE",
		"SEARCHABLE",
		"UNSIGNED_ATTRIBUTE",
		"FIXED_PREC_SCALE",
		"AUTO_UNIQUE_VALUE",
		"LOCAL_TYPE_NAME",
		"MINIMUM_SCALE",
		"MAXIMUM_SCALE",
		"SQL_DATA_TYPE",
		"SQL_DATETIME_SUB",
		"NUM_PREC_RADIX",
		"INTERVAL_PRECISION"
	))";
	// clang-format on

	return duckdb::ExecDirectStmt(hstmt, query);
}

/**
 * <a
 * href="https://learn.microsoft.com/en-us/sql/odbc/reference/syntax/sqlgettypeinfo-function?view=sql-server-ver16">Docs</a>
 */
SQLRETURN SQL_API SQLGetTypeInfo(SQLHSTMT statement_handle, SQLSMALLINT data_type) {
	return GetTypeInfoInternal(statement_handle, data_type);
}

/**
 * Wide char version
 * <a
 * href="https://learn.microsoft.com/en-us/sql/odbc/reference/syntax/sqlgettypeinfo-function?view=sql-server-ver16">Docs</a>
 */
SQLRETURN SQL_API SQLGetTypeInfoW(SQLHSTMT statement_handle, SQLSMALLINT data_type) {
	return GetTypeInfoInternal(statement_handle, data_type);
}

//===--------------------------------------------------------------------===//
// SQLDataSources
//===--------------------------------------------------------------------===//

static SQLRETURN DataSourcesInternal(SQLHENV environment_handle, SQLUSMALLINT direction, SQLCHAR *server_name,
                                     SQLSMALLINT buffer_length1, SQLSMALLINT *name_length1_ptr, SQLCHAR *description,
                                     SQLSMALLINT buffer_length2, SQLSMALLINT *name_length2_ptr) {
	duckdb::OdbcHandleEnv *env = nullptr;
	SQLRETURN ret = ConvertEnvironment(environment_handle, env);
	if (ret != SQL_SUCCESS) {
		return ret;
	}

	return duckdb::SetDiagnosticRecord(env, SQL_ERROR, "SQLDataSources", "Driver Manager only function",
	                                   SQLStateType::ST_HY000, "");
}

/**
 * <a
 * href="https://learn.microsoft.com/en-us/sql/odbc/reference/syntax/sqldatasources-function?view=sql-server-ver16">Docs</a>
 */
SQLRETURN SQL_API SQLDataSources(SQLHENV environment_handle, SQLUSMALLINT direction, SQLCHAR *server_name,
                                 SQLSMALLINT buffer_length1, SQLSMALLINT *name_length1_ptr, SQLCHAR *description,
                                 SQLSMALLINT buffer_length2, SQLSMALLINT *name_length2_ptr) {
	return DataSourcesInternal(environment_handle, direction, server_name, buffer_length1, name_length1_ptr,
	                           description, buffer_length2, name_length2_ptr);
}

/**
 * Wide char version
 * <a
 * href="https://learn.microsoft.com/en-us/sql/odbc/reference/syntax/sqldatasources-function?view=sql-server-ver16">Docs</a>
 */
SQLRETURN SQL_API SQLDataSourcesW(SQLHENV environment_handle, SQLUSMALLINT direction, SQLWCHAR *server_name,
                                  SQLSMALLINT buffer_length1, SQLSMALLINT *name_length1_ptr, SQLWCHAR *description,
                                  SQLSMALLINT buffer_length2, SQLSMALLINT *name_length2_ptr) {
	auto server_name_conv = duckdb::widechar::utf16_conv(server_name, buffer_length1);
	auto description_conv = duckdb::widechar::utf16_conv(description, buffer_length2);
	return DataSourcesInternal(environment_handle, direction, server_name_conv.utf8_str,
	                           server_name_conv.utf8_len_smallint(), name_length1_ptr, description_conv.utf8_str,
	                           description_conv.utf8_len_smallint(), name_length2_ptr);
}

//===--------------------------------------------------------------------===//
// SQLDrivers
//===--------------------------------------------------------------------===//

/**
 * <a
 * href="https://learn.microsoft.com/en-us/sql/odbc/reference/syntax/sqldrivers-function?view=sql-server-ver16">Docs</a>
 */
SQLRETURN SQL_API SQLDrivers(SQLHENV environment_handle, SQLUSMALLINT direction, SQLCHAR *driver_description,
                             SQLSMALLINT buffer_length1, SQLSMALLINT *description_length_ptr,
                             SQLCHAR *driver_attributes, SQLSMALLINT buffer_length2,
                             SQLSMALLINT *attributes_length_ptr) {
	duckdb::OdbcHandleEnv *env = nullptr;
	SQLRETURN ret = ConvertEnvironment(environment_handle, env);
	if (ret != SQL_SUCCESS) {
		return ret;
	}

	return duckdb::SetDiagnosticRecord(env, SQL_ERROR, "SQLDrivers", "Driver Manager only function",
	                                   SQLStateType::ST_HY000, "");
}
