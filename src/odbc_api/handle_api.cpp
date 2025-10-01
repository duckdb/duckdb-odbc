#include "driver.hpp"
#include "duckdb_odbc.hpp"
#include "handle_functions.hpp"
#include "odbc_diagnostic.hpp"
#include "odbc_fetch.hpp"
#include "odbc_utils.hpp"
#include "parameter_descriptor.hpp"
#include "statement_functions.hpp"

#include "duckdb/main/config.hpp"

using duckdb::OdbcDiagnostic;
using duckdb::OdbcUtils;
using duckdb::SQLStateType;

//===--------------------------------------------------------------------===//
// SQLAllocHandle
//===--------------------------------------------------------------------===//

/**
 * <a
 * href="https://learn.microsoft.com/en-us/sql/odbc/reference/syntax/sqlallochandle-function?view=sql-server-ver16">Docs</a>
 */
SQLRETURN SQL_API SQLAllocHandle(SQLSMALLINT handle_type, SQLHANDLE input_handle, SQLHANDLE *output_handle_ptr) {
	switch (handle_type) {
	case SQL_HANDLE_DBC: {
		D_ASSERT(input_handle);

		auto *env = static_cast<duckdb::OdbcHandleEnv *>(input_handle);
		D_ASSERT(env->type == duckdb::OdbcHandleType::ENV);
		*output_handle_ptr = new duckdb::OdbcHandleDbc(env);
		return SQL_SUCCESS;
	}
	case SQL_HANDLE_DESC: {
		D_ASSERT(input_handle);
		auto *dbc = static_cast<duckdb::OdbcHandleDbc *>(input_handle);
		D_ASSERT(dbc->type == duckdb::OdbcHandleType::DBC);
		*output_handle_ptr = new duckdb::OdbcHandleDesc(dbc);
		return SQL_SUCCESS;
	}
	case SQL_HANDLE_ENV:
		*output_handle_ptr = new duckdb::OdbcHandleEnv();
		return SQL_SUCCESS;
	case SQL_HANDLE_STMT: {
		D_ASSERT(input_handle);
		auto *dbc = static_cast<duckdb::OdbcHandleDbc *>(input_handle);
		D_ASSERT(dbc->type == duckdb::OdbcHandleType::DBC);
		*output_handle_ptr = new duckdb::OdbcHandleStmt(dbc);
		return SQL_SUCCESS;
	}
	default:
		return SQL_INVALID_HANDLE;
	}
}

//===--------------------------------------------------------------------===//
// SQLFreeHandle
//===--------------------------------------------------------------------===//

/**
 * <a
 * href="https://learn.microsoft.com/en-us/sql/odbc/reference/syntax/sqlfreehandle-function?view=sql-server-ver16">Docs</a>
 */
SQLRETURN SQL_API SQLFreeHandle(SQLSMALLINT handle_type, SQLHANDLE handle) {
	return duckdb::FreeHandle(handle_type, handle);
}

//===--------------------------------------------------------------------===//
// SQLFreeStmt
//===--------------------------------------------------------------------===//

/**
 * <a
 * href="https://learn.microsoft.com/en-us/sql/odbc/reference/syntax/sqlfreestmt-function?view=sql-server-ver16">Docs</a>
 */
SQLRETURN SQL_API SQLFreeStmt(SQLHSTMT statement_handle, SQLUSMALLINT option) {
	duckdb::OdbcHandleStmt *hstmt = nullptr;
	SQLRETURN ret = ConvertHSTMT(statement_handle, hstmt);
	if (ret != SQL_SUCCESS) {
		return ret;
	}

	if (option == SQL_DROP) {
		// mapping FreeStmt with DROP option to SQLFreeHandle
		return duckdb::FreeHandle(SQL_HANDLE_STMT, statement_handle);
	}
	if (option == SQL_UNBIND) {
		hstmt->bound_cols.clear();
		return SQL_SUCCESS;
	}
	if (option == SQL_RESET_PARAMS) {
		hstmt->param_desc->Clear();
		return SQL_SUCCESS;
	}
	if (option == SQL_CLOSE) {
		return duckdb::CloseStmt(hstmt);
	}
	return SQL_ERROR;
}
