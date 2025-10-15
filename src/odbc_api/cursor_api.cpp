#include "duckdb_odbc.hpp"
#include "statement_functions.hpp"
#include "handle_functions.hpp"

//===--------------------------------------------------------------------===//
// SQLCloseCursor
//===--------------------------------------------------------------------===//

/**
 * <a
 * href="https://learn.microsoft.com/en-us/sql/odbc/reference/syntax/sqlclosecursor-function?view=sql-server-ver16">Docs</a>
 */
SQLRETURN SQL_API SQLCloseCursor(SQLHSTMT statement_handle) {
	duckdb::OdbcHandleStmt *hstmt = nullptr;
	SQLRETURN ret = ConvertHSTMT(statement_handle, hstmt);
	if (ret != SQL_SUCCESS) {
		return ret;
	}

	return duckdb::CloseStmt(hstmt);
}
