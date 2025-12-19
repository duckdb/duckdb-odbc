#include "odbc_test_common.h"

using namespace odbc_test;

TEST_CASE("Test Select Statement", "[odbc]") {
	SQLHANDLE env;
	SQLHANDLE dbc;

	HSTMT hstmt = SQL_NULL_HSTMT;

	// Connect to the database using SQLConnect
	CONNECT_TO_DATABASE(env, dbc);

	// Allocate a statement handle
	EXECUTE_AND_CHECK("SQLAllocHandle (HSTMT)", hstmt, SQLAllocHandle, SQL_HANDLE_STMT, dbc, &hstmt);

	// Execute a simple query
	EXECUTE_AND_CHECK("SQLExecDirect (SELECT 1 UNION ALL SELECT 2)", hstmt, SQLExecDirect, hstmt,
	                  ConvertToSQLCHAR("SELECT 1 UNION ALL SELECT 2"), SQL_NTS);

	// Fetch the first row
	EXECUTE_AND_CHECK("SQLFetch (SELECT 1 UNION ALL SELECT 2)", hstmt, SQLFetch, hstmt);
	// Check the data
	DATA_CHECK(hstmt, 1, "1");

	// Fetch the second row
	EXECUTE_AND_CHECK("SQLFetch (SELECT 1 UNION ALL SELECT 2)", hstmt, SQLFetch, hstmt);
	// Check the data
	DATA_CHECK(hstmt, 1, "2");

	EXECUTE_AND_CHECK("SQLFreeStmt (SQL_CLOSE)", hstmt, SQLFreeStmt, hstmt, SQL_CLOSE);

	// Create a query with 1600 columns
	std::string query = "SELECT ";
	for (int i = 1; i < 1600; i++) {
		query += std::to_string(i);
		if (i < 1599) {
			query += ", ";
		}
	}

	EXECUTE_AND_CHECK("SQLExecDirect (SELECT 1600 columns)", hstmt, SQLExecDirect, hstmt,
	                  ConvertToSQLCHAR(query.c_str()), SQL_NTS);

	// Fetch the first row
	EXECUTE_AND_CHECK("SQLFetch (SELECT 1600 columns)", hstmt, SQLFetch, hstmt);

	// Check the data
	for (int i = 1; i < 1600; i++) {
		DATA_CHECK(hstmt, i, std::to_string(i));
	}

	// Free the statement handle
	EXECUTE_AND_CHECK("SQLFreeStmt (HSTMT)", hstmt, SQLFreeStmt, hstmt, SQL_CLOSE);
	EXECUTE_AND_CHECK("SQLFreeHandle (HSTMT)", hstmt, SQLFreeHandle, SQL_HANDLE_STMT, hstmt);

	DISCONNECT_FROM_DATABASE(env, dbc);
}

TEST_CASE("Test Select Statement Wide", "[odbc]") {
	SQLHANDLE env;
	SQLHANDLE dbc;

	HSTMT hstmt = SQL_NULL_HSTMT;

	// Connect to the database using SQLConnect
	CONNECT_TO_DATABASE(env, dbc);

	// Allocate a statement handle
	EXECUTE_AND_CHECK("SQLAllocHandle (HSTMT)", hstmt, SQLAllocHandle, SQL_HANDLE_STMT, dbc, &hstmt);

	// Execute a simple query
	EXECUTE_AND_CHECK("SQLExecDirectW (SELECT 1 UNION ALL SELECT 2)", hstmt, SQLExecDirectW, hstmt,
	                  ConvertToSQLWCHARNTS("SELECT 1 UNION ALL SELECT 2").data(), SQL_NTS);

	// Fetch the first row
	EXECUTE_AND_CHECK("SQLFetch (SELECT 1 UNION ALL SELECT 2)", hstmt, SQLFetch, hstmt);
	// Check the data
	DATA_CHECK(hstmt, 1, "1");

	// Fetch the second row
	EXECUTE_AND_CHECK("SQLFetch (SELECT 1 UNION ALL SELECT 2)", hstmt, SQLFetch, hstmt);
	// Check the data
	DATA_CHECK(hstmt, 1, "2");

	EXECUTE_AND_CHECK("SQLFreeStmt (SQL_CLOSE)", hstmt, SQLFreeStmt, hstmt, SQL_CLOSE);

	// Create a query with 1600 columns
	std::string query = "SELECT ";
	for (int i = 1; i < 1600; i++) {
		query += std::to_string(i);
		if (i < 1599) {
			query += ", ";
		}
	}

	EXECUTE_AND_CHECK("SQLExecDirectW (SELECT 1600 columns)", hstmt, SQLExecDirectW, hstmt,
	                  ConvertToSQLWCHARNTS(query).data(), SQL_NTS);

	// Fetch the first row
	EXECUTE_AND_CHECK("SQLFetch (SELECT 1600 columns)", hstmt, SQLFetch, hstmt);

	// Check the data
	for (int i = 1; i < 1600; i++) {
		DATA_CHECK(hstmt, i, std::to_string(i));
	}

	// Free the statement handle
	EXECUTE_AND_CHECK("SQLFreeStmt (HSTMT)", hstmt, SQLFreeStmt, hstmt, SQL_CLOSE);
	EXECUTE_AND_CHECK("SQLFreeHandle (HSTMT)", hstmt, SQLFreeHandle, SQL_HANDLE_STMT, hstmt);

	DISCONNECT_FROM_DATABASE(env, dbc);
}

TEST_CASE("Test Empty String", "[odbc]") {
	SQLHANDLE env;
	SQLHANDLE dbc;

	HSTMT hstmt = SQL_NULL_HSTMT;

	// Connect to the database using SQLConnect
	CONNECT_TO_DATABASE(env, dbc);

	// Allocate a statement handle
	EXECUTE_AND_CHECK("SQLAllocHandle (HSTMT)", hstmt, SQLAllocHandle, SQL_HANDLE_STMT, dbc, &hstmt);

	std::vector<std::string> empty_queries = {"",
	                                          " "
	                                          "--",
	                                          "/* I am a comment */"};
	for (std::string &query : empty_queries) {
		{
			EXECUTE_AND_CHECK("SQLExecDirect (empty string)", hstmt, SQLExecDirect, hstmt, ConvertToSQLCHAR(query),
			                  SQL_NTS);
			SQLRETURN ret = SQLFetch(hstmt);
			REQUIRE(ret == SQL_ERROR);
			std::string state, message;
			ACCESS_DIAGNOSTIC(state, message, hstmt, SQL_HANDLE_STMT);
			REQUIRE(state == "HY000");
			REQUIRE(message.find("No statement found") != std::string::npos);
		}

		{
			SQLRETURN ret = SQLPrepare(hstmt, ConvertToSQLCHAR(query), SQL_NTS);
			REQUIRE(ret == SQL_ERROR);
			std::string state, message;
			ACCESS_DIAGNOSTIC(state, message, hstmt, SQL_HANDLE_STMT);
			REQUIRE(state == "42000");
			REQUIRE(message.find("No statement to prepare") != std::string::npos);
		}
	}

	// Free the statement handle
	EXECUTE_AND_CHECK("SQLFreeStmt (HSTMT)", hstmt, SQLFreeStmt, hstmt, SQL_CLOSE);
	EXECUTE_AND_CHECK("SQLFreeHandle (HSTMT)", hstmt, SQLFreeHandle, SQL_HANDLE_STMT, hstmt);

	DISCONNECT_FROM_DATABASE(env, dbc);
}
