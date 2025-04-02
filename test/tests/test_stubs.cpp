
#include "odbc_test_common.h"

using namespace odbc_test;

TEST_CASE("Test SQLSpecialColumns and SQLStatistics stubs", "[odbc]") {
	SQLHANDLE env = nullptr;
	SQLHANDLE dbc = nullptr;

	HSTMT hstmt = SQL_NULL_HSTMT;

	// Connect to the database using SQLConnect
	CONNECT_TO_DATABASE(env, dbc);

	// Allocate a statement handle
	EXECUTE_AND_CHECK("SQLAllocHandle (HSTMT)", hstmt, SQLAllocHandle, SQL_HANDLE_STMT, dbc, &hstmt);

	{ // Check SQLSpecialColumns
		EXECUTE_AND_CHECK("SQLSpecialColumns", hstmt, SQLSpecialColumns, hstmt, 0, nullptr, 0, nullptr, 0, nullptr, 0,
		                  0, 0);

		// Check columns
		SQLSMALLINT col_count;
		EXECUTE_AND_CHECK("SQLNumResultCols", hstmt, SQLNumResultCols, hstmt, &col_count);
		REQUIRE(col_count == 8);

		// Check that the result set is empty
		SQLRETURN ret = SQLFetch(hstmt);
		REQUIRE(ret == SQL_NO_DATA);
	}
	{ // Check SQLStatistics
		EXECUTE_AND_CHECK("SQLStatistics", hstmt, SQLStatistics, hstmt, nullptr, 0, nullptr, 0, nullptr, 0, 0, 0);
		// Check columns
		SQLSMALLINT col_count;
		EXECUTE_AND_CHECK("SQLNumResultCols", hstmt, SQLNumResultCols, hstmt, &col_count);
		REQUIRE(col_count == 13);

		// Check that the result set is empty
		SQLRETURN ret = SQLFetch(hstmt);
		REQUIRE(ret == SQL_NO_DATA);
	}

	// Free the statement handle
	EXECUTE_AND_CHECK("SQLFreeStmt (HSTMT)", hstmt, SQLFreeStmt, hstmt, SQL_CLOSE);
	EXECUTE_AND_CHECK("SQLFreeHandle (HSTMT)", hstmt, SQLFreeHandle, SQL_HANDLE_STMT, hstmt);

	// Disconnect from the database
	DISCONNECT_FROM_DATABASE(env, dbc);
}
