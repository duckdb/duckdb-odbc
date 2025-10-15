#include "duckdb_odbc.hpp"
#include "statement_functions.hpp"
#include "handle_functions.hpp"
#include "api_info.hpp"

#include "duckdb/main/prepared_statement_data.hpp"

using duckdb::hugeint_t;
using duckdb::idx_t;
using duckdb::Load;
using duckdb::LogicalType;
using duckdb::Value;

//===--------------------------------------------------------------------===//
// SQLBindCol
//===--------------------------------------------------------------------===//

/**
 * <a
 * href="https://learn.microsoft.com/en-us/sql/odbc/reference/syntax/sqlbindcol-function?view=sql-server-ver16">Docs</a>
 */
SQLRETURN SQL_API SQLBindCol(SQLHSTMT statement_handle, SQLUSMALLINT column_number, SQLSMALLINT target_type,
                             SQLPOINTER target_value_ptr, SQLLEN buffer_length, SQLLEN *str_len_or_ind_ptr) {
	duckdb::OdbcHandleStmt *hstmt = nullptr;
	SQLRETURN ret = ConvertHSTMT(statement_handle, hstmt);
	if (ret != SQL_SUCCESS) {
		return ret;
	}

	D_ASSERT(column_number > 0);

	if (column_number > hstmt->bound_cols.size()) {
		hstmt->bound_cols.resize(column_number);
	}

	size_t col_nr_internal = column_number - 1;

	hstmt->bound_cols[col_nr_internal].type = target_type;
	hstmt->bound_cols[col_nr_internal].ptr = target_value_ptr;
	hstmt->bound_cols[col_nr_internal].len = buffer_length;
	hstmt->bound_cols[col_nr_internal].strlen_or_ind = str_len_or_ind_ptr;

	return SQL_SUCCESS;
}

//===--------------------------------------------------------------------===//
// SQLBindParameter
//===--------------------------------------------------------------------===//

/**
 * <a
 * href="https://learn.microsoft.com/en-us/sql/odbc/reference/syntax/sqlbindparameter-function?view=sql-server-ver16">Docs</a>
 */
SQLRETURN SQL_API SQLBindParameter(SQLHSTMT statement_handle, SQLUSMALLINT parameter_number,
                                   SQLSMALLINT input_output_type, SQLSMALLINT value_type, SQLSMALLINT parameter_type,
                                   SQLULEN column_size, SQLSMALLINT decimal_digits, SQLPOINTER parameter_value_ptr,
                                   SQLLEN buffer_length, SQLLEN *str_len_or_ind_ptr) {
	duckdb::OdbcHandleStmt *hstmt = nullptr;
	SQLRETURN ret = ConvertHSTMT(statement_handle, hstmt);
	if (ret != SQL_SUCCESS) {
		return ret;
	}

	return duckdb::BindParameterStmt(hstmt, parameter_number, input_output_type, value_type, parameter_type,
	                                 column_size, decimal_digits, parameter_value_ptr, buffer_length,
	                                 str_len_or_ind_ptr);
}
