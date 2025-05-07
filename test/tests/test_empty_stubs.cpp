#include "odbc_test_common.h"

using namespace odbc_test;

void CheckEmpty(const SQLHSTMT hstmt, idx_t expected_col_count) {
	// Check columns
	SQLSMALLINT col_count;
	EXECUTE_AND_CHECK("SQLNumResultCols", hstmt, SQLNumResultCols, hstmt, &col_count);
	REQUIRE(col_count == expected_col_count);

	// Check that the result set is empty
	const SQLRETURN ret = SQLFetch(hstmt);
	REQUIRE(ret == SQL_NO_DATA);
}

// The following functions are stubs that should return empty result sets.
// * SQLPrimaryKeys
// * SQLForeignKeys
// * SQLProcedureColumns
// * SQLProcedures
// * SQLColumnPrivileges
// * SQLTablePrivileges
// * SQLSpecialColumns
// * SQLStatistics
TEST_CASE("Test Empty Stubs -- Should return empty result", "[odbc]") {
	SQLHANDLE env = nullptr;
	SQLHANDLE dbc = nullptr;

	HSTMT hstmt = SQL_NULL_HSTMT;

	// Connect to the database using SQLConnect
	CONNECT_TO_DATABASE(env, dbc);

	// Allocate a statement handle
	EXECUTE_AND_CHECK("SQLAllocHandle (HSTMT)", hstmt, SQLAllocHandle, SQL_HANDLE_STMT, dbc, &hstmt);

	{
		// SQLPrimaryKeys
		EXECUTE_AND_CHECK("SQLPrimaryKeys", hstmt, SQLPrimaryKeys, hstmt, nullptr, 0, nullptr, 0, nullptr, 0);
		CheckEmpty(hstmt, 6);
	}

	{
		// SQLForeignKeys
		EXECUTE_AND_CHECK("SQLForeignKeys", hstmt, SQLForeignKeys, hstmt, nullptr, 0, nullptr, 0, nullptr, 0, nullptr,
		                  0, nullptr, 0, nullptr, 0);
		CheckEmpty(hstmt, 14);
	}

	{
		// SQLProcedureColumns
		EXECUTE_AND_CHECK("SQLProcedureColumns", hstmt, SQLProcedureColumns, hstmt, nullptr, 0, nullptr, 0, nullptr, 0,
		                  nullptr, 0);
		CheckEmpty(hstmt, 13);
	}

	{
		// SQLProcedures
		EXECUTE_AND_CHECK("SQLProcedures", hstmt, SQLProcedures, hstmt, nullptr, 0, nullptr, 0, nullptr, 0);
		CheckEmpty(hstmt, 5);
	}

	{
		// SQLColumnPrivileges
		EXECUTE_AND_CHECK("SQLColumnPrivileges", hstmt, SQLColumnPrivileges, hstmt, nullptr, 0, nullptr, 0, nullptr, 0,
		                  nullptr, 0);
		CheckEmpty(hstmt, 8);
	}

	{
		// SQLTablePrivileges
		EXECUTE_AND_CHECK("SQLTablePrivileges", hstmt, SQLTablePrivileges, hstmt, nullptr, 0, nullptr, 0, nullptr, 0);
		CheckEmpty(hstmt, 8);
	}

	{
		// SQLSpecialColumns
		EXECUTE_AND_CHECK("SQLSpecialColumns", hstmt, SQLSpecialColumns, hstmt, 0, nullptr, 0, nullptr, 0, nullptr, 0,
		                  0, 0);
		CheckEmpty(hstmt, 8);
	}

	{
		// SQLStatistics
		EXECUTE_AND_CHECK("SQLStatistics", hstmt, SQLStatistics, hstmt, nullptr, 0, nullptr, 0, nullptr, 0, 0, 0);
		CheckEmpty(hstmt, 13);
	}

	// Free the statement handle
	EXECUTE_AND_CHECK("SQLFreeStmt (HSTMT)", hstmt, SQLFreeStmt, hstmt, SQL_CLOSE);
	EXECUTE_AND_CHECK("SQLFreeHandle (HSTMT)", hstmt, SQLFreeHandle, SQL_HANDLE_STMT, hstmt);

	// Disconnect from the database
	DISCONNECT_FROM_DATABASE(env, dbc);
}

void CheckForErrorAndDiagnosticRecord(const SQLRETURN ret, const SQLHANDLE handle, SQLSMALLINT handle_type) {
	REQUIRE(ret == SQL_ERROR);

	std::string state;
	std::string message;
	ACCESS_DIAGNOSTIC(state, message, handle, handle_type);
	REQUIRE(state == "HYC00");
	REQUIRE(duckdb::StringUtil::Contains(message, "not implemented"));
}

// The following functions are stubs that should return an error.
// * SQLNativeSql
// * SQLBrowseConnect
// * SQLBulkOperations
// * SQLSetPos
TEST_CASE("Test Empty Stubs -- Should return error", "[odbc]") {
	SQLHANDLE env;
	SQLHANDLE dbc;

	HSTMT hstmt = SQL_NULL_HSTMT;

	// Connect to the database using SQLConnect
	CONNECT_TO_DATABASE(env, dbc);

	{
		// SQLNativeSql
		const auto ret = SQLNativeSql(dbc, nullptr, 0, nullptr, 0, nullptr);
		CheckForErrorAndDiagnosticRecord(ret, dbc, SQL_HANDLE_DBC);
	}

	{
		// SQLBrowseConnect
		const auto ret = SQLBrowseConnect(dbc, nullptr, 0, nullptr, 0, nullptr);
		CheckForErrorAndDiagnosticRecord(ret, dbc, SQL_HANDLE_DBC);
	}

	// Allocate a statement handle
	EXECUTE_AND_CHECK("SQLAllocHandle (HSTMT)", hstmt, SQLAllocHandle, SQL_HANDLE_STMT, dbc, &hstmt);

#if (ODBCVER >= 0x0300)
	{
		// SQLBulkOperations
		const auto ret = SQLBulkOperations(hstmt, 0);
		CheckForErrorAndDiagnosticRecord(ret, hstmt, SQL_HANDLE_STMT);
	}
#endif

	{
		// SQLSetPos
		const auto ret = SQLSetPos(hstmt, 0, 0, 0);
		CheckForErrorAndDiagnosticRecord(ret, hstmt, SQL_HANDLE_STMT);
	}

	// Free the statement handle
	EXECUTE_AND_CHECK("SQLFreeStmt (HSTMT)", hstmt, SQLFreeStmt, hstmt, SQL_CLOSE);
	EXECUTE_AND_CHECK("SQLFreeHandle (HSTMT)", hstmt, SQLFreeHandle, SQL_HANDLE_STMT, hstmt);

	DISCONNECT_FROM_DATABASE(env, dbc);
}
