#include "odbc_test_common.h"
#include <iostream>

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

TEST_CASE("Test extension auto-load using Excel extension", "[odbc]") {
	SQLHANDLE env;
	SQLHANDLE dbc;

	HSTMT hstmt = SQL_NULL_HSTMT;

	// Connect to the database using SQLConnect
	CONNECT_TO_DATABASE(env, dbc);

	// Allocate a statement handle
	EXECUTE_AND_CHECK("SQLAllocHandle (HSTMT)", SQLAllocHandle, SQL_HANDLE_STMT, dbc, &hstmt);

	// Check that the Excel extension is not yet loaded
	std::string check_excel_loaded_sql = "SELECT LOADED FROM duckdb_extensions() WHERE extension_name = 'excel'";
	EXECUTE_AND_CHECK("SQLExecDirect (" + check_excel_loaded_sql + ")", SQLExecDirect, hstmt,
	                  ConvertToSQLCHAR(check_excel_loaded_sql), SQL_NTS);
	EXECUTE_AND_CHECK("SQLFetch (" + check_excel_loaded_sql + ")", SQLFetch, hstmt);
	DATA_CHECK(hstmt, 1, "false");

	// Check whether the Excel extension can be installed and loaded automatically
	std::string call_excel_text_sql = "SELECT excel_text(1_234_567.897, 'h:mm AM/PM')";
	EXECUTE_AND_CHECK("SQLExecDirect (" + call_excel_text_sql + ")", SQLExecDirect, hstmt,
	                  ConvertToSQLCHAR(call_excel_text_sql), SQL_NTS);
	EXECUTE_AND_CHECK("SQLFetch (" + call_excel_text_sql + ")", SQLFetch, hstmt);
	DATA_CHECK(hstmt, 1, "9:31 PM");

	// Check that the Excel extension is loaded now
	EXECUTE_AND_CHECK("SQLExecDirect (" + check_excel_loaded_sql + ")", SQLExecDirect, hstmt,
	                  ConvertToSQLCHAR(check_excel_loaded_sql), SQL_NTS);
	EXECUTE_AND_CHECK("SQLFetch (" + check_excel_loaded_sql + ")", SQLFetch, hstmt);
	DATA_CHECK(hstmt, 1, "true");

	// Free the statement handle
	EXECUTE_AND_CHECK("SQLFreeStmt (HSTMT)", SQLFreeStmt, hstmt, SQL_CLOSE);
	EXECUTE_AND_CHECK("SQLFreeHandle (HSTMT)", SQLFreeHandle, SQL_HANDLE_STMT, hstmt);

	DISCONNECT_FROM_DATABASE(env, dbc);
}

TEST_CASE("Log DuckDB Version", "[odbc]") {
	SQLHANDLE env;
	SQLHANDLE dbc;

	HSTMT hstmt = SQL_NULL_HSTMT;

	// Connect to the database using SQLConnect
	CONNECT_TO_DATABASE(env, dbc);

	// Allocate a statement handle
	EXECUTE_AND_CHECK("SQLAllocHandle (HSTMT)", SQLAllocHandle, SQL_HANDLE_STMT, dbc, &hstmt);

	// Get the version of DuckDB
	EXECUTE_AND_CHECK("PRAGMA version", SQLExecDirect, hstmt, ConvertToSQLCHAR("PRAGMA version"), SQL_NTS);

	// Fetch the results
	EXECUTE_AND_CHECK("SQLFetch", SQLFetch, hstmt);
	SQLCHAR content[256];
	SQLLEN content_len;

	// SQLGetData returns data for a single column in the result set.
	SQLRETURN ret = SQLGetData(hstmt, 2, SQL_C_CHAR, content, sizeof(content), &content_len);
	ODBC_CHECK(ret, "SQLGetData");
	std::cout << "Version: " << ConvertToString(content) << std::endl;

	// Free the statement handle
	EXECUTE_AND_CHECK("SQLFreeStmt (HSTMT)", SQLFreeStmt, hstmt, SQL_CLOSE);
	EXECUTE_AND_CHECK("SQLFreeHandle (HSTMT)", SQLFreeHandle, SQL_HANDLE_STMT, hstmt);

	DISCONNECT_FROM_DATABASE(env, dbc);
}
