#include "duckdb_odbc.hpp"
#include "api_info.hpp"
#include "odbc_diagnostic.hpp"
#include "odbc_utils.hpp"
#include "statement_functions.hpp"
#include "handle_functions.hpp"
#include "widechar.hpp"

#include <regex>

using duckdb_odbc::LogicalTypeId;
using duckdb_odbc::OdbcUtils;
using duckdb_odbc::SQLStateType;

//===--------------------------------------------------------------------===//
// SQLPrepare
//===--------------------------------------------------------------------===//

static SQLRETURN PrepareInternal(SQLHSTMT statement_handle, SQLCHAR *statement_text, SQLINTEGER text_length) {
	duckdb_odbc::OdbcHandleStmt *hstmt = nullptr;
	SQLRETURN ret = ConvertHSTMT(statement_handle, hstmt);
	if (ret != SQL_SUCCESS) {
		return ret;
	}

	const auto query = OdbcUtils::ConvertSQLCHARToString(statement_text, text_length);
	return duckdb_odbc::PrepareStmt(hstmt, query);
}

/**
 * <a
 * href="https://learn.microsoft.com/en-us/sql/odbc/reference/syntax/sqlprepare-function?view=sql-server-ver16">Docs</a>
 */
SQLRETURN SQL_API SQLPrepare(SQLHSTMT statement_handle, SQLCHAR *statement_text, SQLINTEGER text_length) {
	return PrepareInternal(statement_handle, statement_text, text_length);
}

/**
 * Wide char version.
 * <a
 * href="https://learn.microsoft.com/en-us/sql/odbc/reference/syntax/sqlprepare-function?view=sql-server-ver16">Docs</a>
 */
SQLRETURN SQL_API SQLPrepareW(SQLHSTMT statement_handle, SQLWCHAR *statement_text, SQLINTEGER text_length) {
	auto statement_text_conv = duckdb_odbc::widechar::utf16_conv(statement_text, text_length);
	return PrepareInternal(statement_handle, statement_text_conv.utf8_str, statement_text_conv.utf8_len_int());
}

//===--------------------------------------------------------------------===//
// SQLExecute
//===--------------------------------------------------------------------===//

/**
 * <a
 * href="https://learn.microsoft.com/en-us/sql/odbc/reference/syntax/sqlexecute-function?view=sql-server-ver16">Docs</a>
 */
SQLRETURN SQL_API SQLExecute(SQLHSTMT statement_handle) {
	duckdb_odbc::OdbcHandleStmt *hstmt = nullptr;
	SQLRETURN ret = ConvertHSTMT(statement_handle, hstmt);
	if (ret != SQL_SUCCESS) {
		return ret;
	}

	return duckdb_odbc::BatchExecuteStmt(hstmt);
}

//===--------------------------------------------------------------------===//
// SQLExecDirect
//===--------------------------------------------------------------------===//

static SQLRETURN ExecDirectInternal(SQLHSTMT statement_handle, SQLCHAR *statement_text, SQLINTEGER text_length) {
	duckdb_odbc::OdbcHandleStmt *hstmt = nullptr;
	const SQLRETURN ret = ConvertHSTMT(statement_handle, hstmt);
	if (ret != SQL_SUCCESS) {
		return ret;
	}

	const auto query = OdbcUtils::ConvertSQLCHARToString(statement_text, text_length);
	return duckdb_odbc::ExecDirectStmt(hstmt, query);
}

/**
 * <a
 * href="https://learn.microsoft.com/en-us/sql/odbc/reference/syntax/sqlexecdirect-function?view=sql-server-ver15">Docs</a>
 */
SQLRETURN SQL_API SQLExecDirect(SQLHSTMT statement_handle, SQLCHAR *statement_text, SQLINTEGER text_length) {
	return ExecDirectInternal(statement_handle, statement_text, text_length);
}

/**
 * Wide char version.
 * <a
 * href="https://learn.microsoft.com/en-us/sql/odbc/reference/syntax/sqlexecdirect-function?view=sql-server-ver15">Docs</a>
 */
SQLRETURN SQL_API SQLExecDirectW(SQLHSTMT statement_handle, SQLWCHAR *statement_text, SQLINTEGER text_length) {
	auto statement_text_conv = duckdb_odbc::widechar::utf16_conv(statement_text, text_length);
	return ExecDirectInternal(statement_handle, statement_text_conv.utf8_str, statement_text_conv.utf8_len_int());
}

//===--------------------------------------------------------------------===//
// SQLCancel
//===--------------------------------------------------------------------===//

/**
 * <a
 * href="https://learn.microsoft.com/en-us/sql/odbc/reference/syntax/sqlcancel-function?view=sql-server-ver16">Docs</a>
 */
SQLRETURN SQL_API SQLCancel(SQLHSTMT statement_handle) {
	duckdb_odbc::OdbcHandleStmt *hstmt = nullptr;
	SQLRETURN ret = ConvertHSTMT(statement_handle, hstmt);
	if (ret != SQL_SUCCESS) {
		return ret;
	}

	hstmt->dbc->conn->Interrupt();
	return SQL_SUCCESS;
}
