#include "duckdb_odbc.hpp"
#include "statement_functions.hpp"
#include "handle_functions.hpp"

SQLRETURN SQL_API SQLCloseCursor(SQLHSTMT statement_handle) {
	duckdb::OdbcHandleStmt *hstmt = nullptr;
	SQLRETURN ret = ConvertHSTMT(statement_handle, hstmt);
	if (ret != SQL_SUCCESS) {
		return ret;
	}

	return duckdb::CloseStmt(hstmt);
}

SQLRETURN SQL_API SQLSetCursorName(SQLHSTMT statement_handle, SQLCHAR *cursor_name, SQLSMALLINT name_length) {
	duckdb::OdbcHandleStmt *hstmt = nullptr;
	SQLRETURN ret = ConvertHSTMT(statement_handle, hstmt);
	if (ret != SQL_SUCCESS) {
		return ret;
	}

	return duckdb::SetDiagnosticRecord(hstmt, SQL_ERROR, "SQLSetCursorName", "SQLSetCursorName is not supported.",
	                                   duckdb::SQLStateType::ST_IM001, hstmt->dbc->GetDataSourceName());
}

SQLRETURN SQL_API SQLSetCursorNameW(SQLHSTMT statement_handle, SQLWCHAR *cursor_name, SQLSMALLINT name_length) {
	duckdb::OdbcHandleStmt *hstmt = nullptr;
	SQLRETURN ret = ConvertHSTMT(statement_handle, hstmt);
	if (ret != SQL_SUCCESS) {
		return ret;
	}

	return duckdb::SetDiagnosticRecord(hstmt, SQL_ERROR, "SQLSetCursorNameW", "SQLSetCursorNameW is not supported.",
	                                   duckdb::SQLStateType::ST_IM001, hstmt->dbc->GetDataSourceName());
}

SQLRETURN SQL_API SQLGetCursorName(SQLHSTMT statement_handle, SQLCHAR *cursor_name, SQLSMALLINT name_length,
                                   SQLSMALLINT *length_ptr) {
	duckdb::OdbcHandleStmt *hstmt = nullptr;
	SQLRETURN ret = ConvertHSTMT(statement_handle, hstmt);
	if (ret != SQL_SUCCESS) {
		return ret;
	}
	return duckdb::SetDiagnosticRecord(hstmt, SQL_ERROR, "SQLGetCursorName", "SQLGetCursorName is not supported.",
	                                   duckdb::SQLStateType::ST_IM001, hstmt->dbc->GetDataSourceName());
}

SQLRETURN SQL_API SQLGetCursorNameW(SQLHSTMT statement_handle, SQLWCHAR *cursor_name, SQLSMALLINT name_length,
                                    SQLSMALLINT *length_ptr) {
	duckdb::OdbcHandleStmt *hstmt = nullptr;
	SQLRETURN ret = ConvertHSTMT(statement_handle, hstmt);
	if (ret != SQL_SUCCESS) {
		return ret;
	}
	return duckdb::SetDiagnosticRecord(hstmt, SQL_ERROR, "SQLGetCursorNameW", "SQLGetCursorNameW is not supported.",
	                                   duckdb::SQLStateType::ST_IM001, hstmt->dbc->GetDataSourceName());
}
