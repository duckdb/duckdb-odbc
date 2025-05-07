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
// * SQLPrimaryKeys and SQLPrimaryKeysW
// * SQLForeignKeys and SQLForeignKeysW
// * SQLProcedureColumns and SQLProcedureColumnsW
// * SQLProcedures and SQLProceduresW
// * SQLColumnPrivileges and SQLColumnPrivilegesW
// * SQLTablePrivileges and SQLTablePrivilegesW
// * SQLSpecialColumns and SQLSpecialColumnsW
// * SQLStatistics and SQLStatisticsW
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
		EXECUTE_AND_CHECK("SQLPrimaryKeys", hstmt, SQLPrimaryKeys, hstmt, ConvertToSQLCHAR("MY_CATALOG"), SQL_NTS,
		                  ConvertToSQLCHAR("MY_SCHEMA"), SQL_NTS, ConvertToSQLCHAR("MY_TABLE"), SQL_NTS);
		CheckEmpty(hstmt, 6);
	}

	{
		// SQLPrimaryKeysW
		EXECUTE_AND_CHECK("SQLPrimaryKeysW", hstmt, SQLPrimaryKeysW, hstmt, ConvertToSQLWCHARNTS("MY_CATALOG").data(),
		                  SQL_NTS, ConvertToSQLWCHARNTS("MY_SCHEMA").data(), SQL_NTS,
		                  ConvertToSQLWCHARNTS("MY_TABLE").data(), SQL_NTS);
		CheckEmpty(hstmt, 6);
	}

	{
		// SQLForeignKeys
		EXECUTE_AND_CHECK("SQLForeignKeys", hstmt, SQLForeignKeys, hstmt, ConvertToSQLCHAR("PK_CAT"), SQL_NTS,
		                  ConvertToSQLCHAR("PK_SCHEMA"), SQL_NTS, ConvertToSQLCHAR("PK_TABLE"), SQL_NTS,
		                  ConvertToSQLCHAR("FK_CAT"), SQL_NTS, ConvertToSQLCHAR("FK_SCHEMA"), SQL_NTS,
		                  ConvertToSQLCHAR("FK_TABLE"), SQL_NTS);
		CheckEmpty(hstmt, 14);
	}

	{
		// SQLForeignKeysW
		EXECUTE_AND_CHECK("SQLForeignKeysW", hstmt, SQLForeignKeysW, hstmt, ConvertToSQLWCHARNTS("PK_CAT").data(),
		                  SQL_NTS, ConvertToSQLWCHARNTS("PK_SCHEMA").data(), SQL_NTS,
		                  ConvertToSQLWCHARNTS("PK_TABLE").data(), SQL_NTS, ConvertToSQLWCHARNTS("FK_CAT").data(),
		                  SQL_NTS, ConvertToSQLWCHARNTS("FK_SCHEMA").data(), SQL_NTS,
		                  ConvertToSQLWCHARNTS("FK_TABLE").data(), SQL_NTS);
		CheckEmpty(hstmt, 14);
	}

	{
		// SQLProcedureColumns
		EXECUTE_AND_CHECK("SQLProcedureColumns", hstmt, SQLProcedureColumns, hstmt, ConvertToSQLCHAR("MY_CATALOG"),
		                  SQL_NTS, ConvertToSQLCHAR("MY_SCHEMA"), SQL_NTS, ConvertToSQLCHAR("MY_PROC"), SQL_NTS,
		                  ConvertToSQLCHAR("%"), SQL_NTS);
		CheckEmpty(hstmt, 19);
	}

	{
		// SQLProcedureColumnsW
		EXECUTE_AND_CHECK("SQLProcedureColumnsW", hstmt, SQLProcedureColumnsW, hstmt,
		                  ConvertToSQLWCHARNTS("MY_CATALOG").data(), SQL_NTS, ConvertToSQLWCHARNTS("MY_SCHEMA").data(),
		                  SQL_NTS, ConvertToSQLWCHARNTS("MY_PROC").data(), SQL_NTS, ConvertToSQLWCHARNTS("%").data(),
		                  SQL_NTS);
		CheckEmpty(hstmt, 19);
	}

	{
		// SQLProcedures
		EXECUTE_AND_CHECK("SQLProcedures", hstmt, SQLProcedures, hstmt, ConvertToSQLCHAR("MY_CATALOG"), SQL_NTS,
		                  ConvertToSQLCHAR("MY_SCHEMA"), SQL_NTS, ConvertToSQLCHAR("MY_PROC"), SQL_NTS);
		CheckEmpty(hstmt, 8);
	}

	{
		// SQLProceduresW
		EXECUTE_AND_CHECK("SQLProceduresW", hstmt, SQLProceduresW, hstmt, ConvertToSQLWCHARNTS("MY_CATALOG").data(),
		                  SQL_NTS, ConvertToSQLWCHARNTS("MY_SCHEMA").data(), SQL_NTS,
		                  ConvertToSQLWCHARNTS("MY_PROC").data(), SQL_NTS);
		CheckEmpty(hstmt, 8);
	}

	{
		// SQLColumnPrivileges
		EXECUTE_AND_CHECK("SQLColumnPrivileges", hstmt, SQLColumnPrivileges, hstmt, ConvertToSQLCHAR("MY_CATALOG"),
		                  SQL_NTS, ConvertToSQLCHAR("MY_SCHEMA"), SQL_NTS, ConvertToSQLCHAR("MY_TABLE"), SQL_NTS,
		                  ConvertToSQLCHAR("%"), SQL_NTS);
		CheckEmpty(hstmt, 8);
	}

	{
		// SQLColumnPrivilegesW
		EXECUTE_AND_CHECK("SQLColumnPrivilegesW", hstmt, SQLColumnPrivilegesW, hstmt,
		                  ConvertToSQLWCHARNTS("MY_CATALOG").data(), SQL_NTS, ConvertToSQLWCHARNTS("MY_SCHEMA").data(),
		                  SQL_NTS, ConvertToSQLWCHARNTS("MY_TABLE").data(), SQL_NTS, ConvertToSQLWCHARNTS("%").data(),
		                  SQL_NTS);
		CheckEmpty(hstmt, 8);
	}

	{
		// SQLTablePrivileges
		EXECUTE_AND_CHECK("SQLTablePrivileges", hstmt, SQLTablePrivileges, hstmt, ConvertToSQLCHAR("MY_CATALOG"),
		                  SQL_NTS, ConvertToSQLCHAR("MY_SCHEMA"), SQL_NTS, ConvertToSQLCHAR("MY_TABLE"), SQL_NTS);
		CheckEmpty(hstmt, 7);
	}

	{
		// SQLTablePrivilegesW
		EXECUTE_AND_CHECK("SQLTablePrivilegesW", hstmt, SQLTablePrivilegesW, hstmt,
		                  ConvertToSQLWCHARNTS("MY_CATALOG").data(), SQL_NTS, ConvertToSQLWCHARNTS("MY_SCHEMA").data(),
		                  SQL_NTS, ConvertToSQLWCHARNTS("MY_TABLE").data(), SQL_NTS);
		CheckEmpty(hstmt, 7);
	}

	{
		// SQLSpecialColumns
		EXECUTE_AND_CHECK("SQLSpecialColumns", hstmt, SQLSpecialColumns, hstmt, SQL_BEST_ROWID,
		                  ConvertToSQLCHAR("MY_CATALOG"), SQL_NTS, ConvertToSQLCHAR("MY_SCHEMA"), SQL_NTS,
		                  ConvertToSQLCHAR("MY_TABLE"), SQL_NTS, SQL_SCOPE_SESSION, SQL_NO_NULLS);
		CheckEmpty(hstmt, 8);
	}

	{
		// SQLSpecialColumnsW
		EXECUTE_AND_CHECK("SQLSpecialColumnsW", hstmt, SQLSpecialColumnsW, hstmt, SQL_BEST_ROWID,
		                  ConvertToSQLWCHARNTS("MY_CATALOG").data(), SQL_NTS, ConvertToSQLWCHARNTS("MY_SCHEMA").data(),
		                  SQL_NTS, ConvertToSQLWCHARNTS("MY_TABLE").data(), SQL_NTS, SQL_SCOPE_SESSION, SQL_NO_NULLS);
		CheckEmpty(hstmt, 8);
	}

	{
		// SQLStatistics
		EXECUTE_AND_CHECK("SQLStatistics", hstmt, SQLStatistics, hstmt, ConvertToSQLCHAR("MY_CATALOG"), SQL_NTS,
		                  ConvertToSQLCHAR("MY_SCHEMA"), SQL_NTS, ConvertToSQLCHAR("MY_TABLE"), SQL_NTS,
		                  SQL_INDEX_UNIQUE, SQL_ENSURE);
		CheckEmpty(hstmt, 13);
	}

	{
		// SQLStatisticsW
		EXECUTE_AND_CHECK("SQLStatisticsW", hstmt, SQLStatisticsW, hstmt, ConvertToSQLWCHARNTS("MY_CATALOG").data(),
		                  SQL_NTS, ConvertToSQLWCHARNTS("MY_SCHEMA").data(), SQL_NTS,
		                  ConvertToSQLWCHARNTS("MY_TABLE").data(), SQL_NTS, SQL_INDEX_UNIQUE, SQL_ENSURE);
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
// * SQLNativeSql and SQLNativeSqlW
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
		const std::string input_sql = "SELECT 1";
		SQLCHAR output_sql[256];
		SQLINTEGER output_len = 0;

		const auto ret =
		    SQLNativeSql(dbc, ConvertToSQLCHAR(input_sql.data()), static_cast<SQLINTEGER>(input_sql.size()), output_sql,
		                 sizeof(output_sql), &output_len);

		CheckForErrorAndDiagnosticRecord(ret, dbc, SQL_HANDLE_DBC);
	}

	{
		// SQLNativeSqlW
		const std::string input_sql = "SELECT 1";
		SQLWCHAR output_sql[256];
		SQLINTEGER output_len = 0;

		const auto ret =
		    SQLNativeSqlW(dbc, ConvertToSQLWCHARNTS(input_sql).data(), static_cast<SQLINTEGER>(input_sql.size()),
		                  output_sql, sizeof(output_sql) / sizeof(SQLWCHAR), &output_len);

		CheckForErrorAndDiagnosticRecord(ret, dbc, SQL_HANDLE_DBC);
	}

	{
		// SQLBrowseConnect
		const std::string connect_str = "DRIVER={Dummy};";
		SQLCHAR output_conn[256];
		SQLSMALLINT output_len = 0;

		const auto ret =
		    SQLBrowseConnect(dbc, ConvertToSQLCHAR(connect_str), static_cast<SQLSMALLINT>(connect_str.size()),
		                     output_conn, sizeof(output_conn), &output_len);

		CheckForErrorAndDiagnosticRecord(ret, dbc, SQL_HANDLE_DBC);
	}

	// Allocate a statement handle
	EXECUTE_AND_CHECK("SQLAllocHandle (HSTMT)", hstmt, SQLAllocHandle, SQL_HANDLE_STMT, dbc, &hstmt);

#if (ODBCVER >= 0x0300)
	{
		// SQLBulkOperations
		const auto ret = SQLBulkOperations(hstmt, SQL_ADD);
		CheckForErrorAndDiagnosticRecord(ret, hstmt, SQL_HANDLE_STMT);
	}
#endif

	{
		// SQLSetPos
		const auto ret = SQLSetPos(hstmt, 1, SQL_POSITION, SQL_LOCK_NO_CHANGE);
		CheckForErrorAndDiagnosticRecord(ret, hstmt, SQL_HANDLE_STMT);
	}

	// Free the statement handle
	EXECUTE_AND_CHECK("SQLFreeStmt (HSTMT)", hstmt, SQLFreeStmt, hstmt, SQL_CLOSE);
	EXECUTE_AND_CHECK("SQLFreeHandle (HSTMT)", hstmt, SQLFreeHandle, SQL_HANDLE_STMT, hstmt);

	DISCONNECT_FROM_DATABASE(env, dbc);
}
