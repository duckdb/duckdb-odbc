#include "duckdb_odbc.hpp"
#include "handle_functions.hpp"
#include "api_info.hpp"
#include "parameter_descriptor.hpp"

#include "duckdb/main/prepared_statement_data.hpp"

using duckdb::hugeint_t;
using duckdb::idx_t;
using duckdb::Load;
using duckdb::LogicalType;
using duckdb::Value;

//===--------------------------------------------------------------------===//
// SQLDescribeParam
//===--------------------------------------------------------------------===//

/**
 * <a
 * href="https://learn.microsoft.com/en-us/sql/odbc/reference/syntax/sqldescribeparam-function?view=sql-server-ver16">Docs</a>
 */
SQLRETURN SQL_API SQLDescribeParam(SQLHSTMT statement_handle, SQLUSMALLINT parameter_number, SQLSMALLINT *data_type_ptr,
                                   SQLULEN *parameter_size_ptr, SQLSMALLINT *decimal_digits_ptr,
                                   SQLSMALLINT *nullable_ptr) {
	duckdb::OdbcHandleStmt *hstmt = nullptr;
	SQLRETURN ret = ConvertHSTMTPrepared(statement_handle, hstmt);
	if (ret != SQL_SUCCESS) {
		return ret;
	}

	if (parameter_number <= 0 || parameter_number > hstmt->stmt->named_param_map.size()) {
		return SQL_ERROR;
	}
	// TODO make global maps with type mappings for duckdb <> odbc
	auto odbc_type = SQL_UNKNOWN_TYPE;
	auto odbc_size = 0;
	auto identifier = std::to_string(parameter_number);
	auto param_type_id = hstmt->stmt->data->GetType(identifier).id();
	switch (param_type_id) {
	case duckdb::LogicalTypeId::VARCHAR:
		odbc_type = SQL_VARCHAR;
		odbc_size = SQL_NO_TOTAL;
		break;
	case duckdb::LogicalTypeId::FLOAT:
		odbc_type = SQL_FLOAT;
		odbc_size = sizeof(float);
		break;
	case duckdb::LogicalTypeId::DOUBLE:
		odbc_type = SQL_DOUBLE;
		odbc_size = sizeof(double);
		break;
	case duckdb::LogicalTypeId::SMALLINT:
		odbc_type = SQL_SMALLINT;
		odbc_size = sizeof(int16_t);
		break;
	case duckdb::LogicalTypeId::INTEGER:
		odbc_type = SQL_INTEGER;
		odbc_size = sizeof(int32_t);
		break;
	default:
		// TODO probably more types should be supported here ay
		return SQL_ERROR;
	}
	if (data_type_ptr) {
		*data_type_ptr = odbc_type;
	}
	if (parameter_size_ptr) {
		*parameter_size_ptr = odbc_size;
	}
	// TODO decimal_digits_ptr
	if (nullable_ptr) {
		*nullable_ptr = SQL_NULLABLE_UNKNOWN;
	}
	return SQL_SUCCESS;
}

//===--------------------------------------------------------------------===//
// SQLNumParams
//===--------------------------------------------------------------------===//

/**
 * <a
 * href="https://learn.microsoft.com/en-us/sql/odbc/reference/syntax/sqlnumparams-function?view=sql-server-ver15">Docs</a>
 */
SQLRETURN SQL_API SQLNumParams(SQLHSTMT statement_handle, SQLSMALLINT *parameter_count_ptr) {
	duckdb::OdbcHandleStmt *hstmt = nullptr;
	SQLRETURN ret = ConvertHSTMTPrepared(statement_handle, hstmt);
	if (ret != SQL_SUCCESS) {
		return ret;
	}

	if (!parameter_count_ptr) {
		return SQL_ERROR;
	}
	*parameter_count_ptr = (SQLSMALLINT)hstmt->stmt->named_param_map.size();
	return SQL_SUCCESS;
}

//===--------------------------------------------------------------------===//
// SQLParamData
//===--------------------------------------------------------------------===//

/**
 * <a
 * href="https://learn.microsoft.com/en-us/sql/odbc/reference/syntax/sqlparamdata-function?view=sql-server-ver15">Docs</a>
 */
SQLRETURN SQL_API SQLParamData(SQLHSTMT statement_handle, SQLPOINTER *value_ptr_ptr) {
	duckdb::OdbcHandleStmt *hstmt = nullptr;
	SQLRETURN ret = ConvertHSTMTPrepared(statement_handle, hstmt);
	if (ret != SQL_SUCCESS) {
		return ret;
	}

	ret = hstmt->param_desc->GetNextParam(value_ptr_ptr);
	if (ret != SQL_NO_DATA) {
		return ret;
	}
	// should try to get value from bound columns
	return SQL_SUCCESS;
}

//===--------------------------------------------------------------------===//
// SQLPutData
//===--------------------------------------------------------------------===//

/**
 * <a
 * href="https://learn.microsoft.com/en-us/sql/odbc/reference/syntax/sqlputdata-function?view=sql-server-ver16">Docs</a>
 */
SQLRETURN SQL_API SQLPutData(SQLHSTMT statement_handle, SQLPOINTER data_ptr, SQLLEN str_len_or_ind_ptr) {
	duckdb::OdbcHandleStmt *hstmt = nullptr;
	SQLRETURN ret = ConvertHSTMTPrepared(statement_handle, hstmt);
	if (ret != SQL_SUCCESS) {
		return ret;
	}

	ret = hstmt->param_desc->PutData(data_ptr, str_len_or_ind_ptr);
	if (ret == SQL_SUCCESS) {
		return ret;
	}

	// should try to put value into bound columns
	return SQL_SUCCESS;
}
