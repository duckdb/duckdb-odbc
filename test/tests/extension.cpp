#include "odbc_test_common.h"

using namespace odbc_test;

// If this test fails the version of the vendored DuckDB doesn't have an extension build
TEST_CASE("Test Extension", "[odbc]") {
	SQLHANDLE env;
	SQLHANDLE dbc;

	HSTMT hstmt = SQL_NULL_HSTMT;

	// Connect to the database using SQLConnect
	CONNECT_TO_DATABASE(env, dbc);

	// Allocate a statement handle
	EXECUTE_AND_CHECK("SQLAllocHandle (HSTMT)", SQLAllocHandle, SQL_HANDLE_STMT, dbc, &hstmt);

	// Install and load the httpfs extension
	EXECUTE_AND_CHECK("INSTALL httpfs", SQLExecDirect, hstmt, ConvertToSQLCHAR("INSTALL httpfs"), SQL_NTS);

	EXECUTE_AND_CHECK("LOAD httpfs", SQLExecDirect, hstmt, ConvertToSQLCHAR("LOAD httpfs"), SQL_NTS);

	// Check if the extension is loaded
	EXEC_SQL(hstmt, "SELECT LOADED FROM duckdb_extensions() WHERE extension_name = 'httpfs'");

	// Should return true
	EXECUTE_AND_CHECK("SQLFetch (SELECT LOADED FROM duckdb_extensions() WHERE extension_name = 'httpfs')", SQLFetch,
	                  hstmt);
	DATA_CHECK(hstmt, 1, "true");

	// Free the statement handle
	EXECUTE_AND_CHECK("SQLFreeStmt (HSTMT)", SQLFreeStmt, hstmt, SQL_CLOSE);
	EXECUTE_AND_CHECK("SQLFreeHandle (HSTMT)", SQLFreeHandle, SQL_HANDLE_STMT, hstmt);

	DISCONNECT_FROM_DATABASE(env, dbc);
}
