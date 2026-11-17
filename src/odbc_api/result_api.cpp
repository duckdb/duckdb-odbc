#include "duckdb_odbc.hpp"
#include "statement_functions.hpp"
#include "handle_functions.hpp"
#include "odbc_fetch.hpp"
#include "parameter_descriptor.hpp"
#include "duckdb/main/prepared_statement_data.hpp"

//===--------------------------------------------------------------------===//
// SQLFetch
//===--------------------------------------------------------------------===//

static SQLRETURN ExecuteBeforeFetch(SQLHSTMT statement_handle) {
	duckdb_odbc::OdbcHandleStmt *hstmt = nullptr;
	SQLRETURN ret = ConvertHSTMTPrepared(statement_handle, hstmt);
	if (ret != SQL_SUCCESS) {
		return ret;
	}

	// case there is a result set, just fetch from it
	if (hstmt->res && !hstmt->res->HasError()) {
		return SQL_SUCCESS;
	}
	// check if it's needed to execute the stmt before fetch
	if (hstmt->param_desc->HasParamSetToProcess()) {
		auto rc = duckdb_odbc::SingleExecuteStmt(hstmt);
		if (rc == SQL_SUCCESS || rc == SQL_STILL_EXECUTING) {
			return SQL_SUCCESS;
		}
		return rc;
	}
	return SQL_SUCCESS;
}

/**
 * <a
 * href="https://learn.microsoft.com/en-us/sql/odbc/reference/syntax/sqlfetch-function?view=sql-server-ver16">Docs</a>
 */
SQLRETURN SQL_API SQLFetch(SQLHSTMT statement_handle) {
	duckdb_odbc::OdbcHandleStmt *hstmt = nullptr;
	SQLRETURN ret = ConvertHSTMTPrepared(statement_handle, hstmt);
	if (ret != SQL_SUCCESS) {
		return ret;
	}

	ret = hstmt->odbc_fetcher->DummyFetch();
	if (ret != SQL_NEED_DATA) {
		return ret;
	}

	ret = ExecuteBeforeFetch(statement_handle);
	if (ret != SQL_SUCCESS) {
		return ret;
	}
	return duckdb_odbc::FetchStmtResult(hstmt);
}

//===--------------------------------------------------------------------===//
// SQLFetchScroll
//===--------------------------------------------------------------------===//

/**
 * <a
 * href="https://learn.microsoft.com/en-us/sql/odbc/reference/syntax/sqlfetchscroll-function?view=sql-server-ver16">Docs</a>
 */
SQLRETURN SQL_API SQLFetchScroll(SQLHSTMT statement_handle, SQLSMALLINT fetch_orientation, SQLLEN fetch_offset) {
	switch (fetch_orientation) {
	case SQL_FETCH_FIRST:
	case SQL_FETCH_ABSOLUTE:
	case SQL_FETCH_PRIOR:
	case SQL_FETCH_NEXT: {
		duckdb_odbc::OdbcHandleStmt *hstmt = nullptr;
		SQLRETURN ret = ConvertHSTMTResult(statement_handle, hstmt);
		if (ret != SQL_SUCCESS) {
			return ret;
		}

		return duckdb_odbc::FetchStmtResult(hstmt, fetch_orientation, fetch_offset);
	}
	default:
		return SQL_ERROR;
	}
}

//===--------------------------------------------------------------------===//
// SQLGetData
//===--------------------------------------------------------------------===//

SQLRETURN SQL_API SQLGetData(SQLHSTMT statement_handle, SQLUSMALLINT col_or_param_num, SQLSMALLINT target_type,
                             SQLPOINTER target_value_ptr, SQLLEN buffer_length, SQLLEN *str_len_or_ind_ptr) {
	duckdb_odbc::OdbcHandleStmt *hstmt = nullptr;
	SQLRETURN ret = ConvertHSTMTResult(statement_handle, hstmt);
	if (ret != SQL_SUCCESS) {
		return ret;
	}

	return duckdb_odbc::GetDataStmtResult(hstmt, col_or_param_num, target_type, target_value_ptr, buffer_length,
	                                      str_len_or_ind_ptr);
}

//===--------------------------------------------------------------------===//
// SQLMoreResults
//===--------------------------------------------------------------------===//

/**
 * <a
 * href="https://learn.microsoft.com/en-us/sql/odbc/reference/syntax/sqlmoreresults-function?view=sql-server-ver16">Docs</a>
 */
SQLRETURN SQL_API SQLMoreResults(SQLHSTMT statement_handle) {
	duckdb_odbc::OdbcHandleStmt *hstmt = nullptr;
	SQLRETURN ret = ConvertHSTMT(statement_handle, hstmt);
	if (ret != SQL_SUCCESS) {
		return ret;
	}

	if (!hstmt->param_desc->HasParamSetToProcess()) {
		return SQL_NO_DATA;
	}
	return duckdb_odbc::SingleExecuteStmt(hstmt);
}

//===--------------------------------------------------------------------===//
// SQLRowCount
//===--------------------------------------------------------------------===//

SQLRETURN SQL_API SQLRowCount(SQLHSTMT statement_handle, SQLLEN *row_count_ptr) {
	duckdb_odbc::OdbcHandleStmt *hstmt = nullptr;
	SQLRETURN ret = ConvertHSTMTResult(statement_handle, hstmt);
	if (ret != SQL_SUCCESS) {
		return ret;
	}

	if (!row_count_ptr) {
		return SQL_ERROR;
	}
	// TODO row_count doesn't work well yet, fix later
	*row_count_ptr = hstmt->odbc_fetcher->GetRowCount();

	switch (hstmt->stmt->GetStatementType()) {
	case duckdb::StatementType::INSERT_STATEMENT:
	case duckdb::StatementType::UPDATE_STATEMENT:
	case duckdb::StatementType::DELETE_STATEMENT:
		break;
	default:
		*row_count_ptr = -1;
	}

	// *row_count_ptr = -1; // we don't actually know most of the time
	return SQL_SUCCESS;
}
