#include "odbc_test_common.h"

using namespace odbc_test;

TEST_CASE("Test EXPLAIN output", "[odbc]") {
	SQLHANDLE env;
	SQLHANDLE dbc;

	HSTMT hstmt = SQL_NULL_HSTMT;

	CONNECT_TO_DATABASE(env, dbc);
	EXECUTE_AND_CHECK("SQLAllocHandle (HSTMT)", hstmt, SQLAllocHandle, SQL_HANDLE_STMT, dbc, &hstmt);

	EXECUTE_AND_CHECK("SQLExecDirect", hstmt, SQLExecDirect, hstmt, ConvertToSQLCHAR("EXPLAIN SELECT 42"), SQL_NTS);

	EXECUTE_AND_CHECK("SQLFetch", hstmt, SQLFetch, hstmt);
	SQLSMALLINT cols = 0;
	EXECUTE_AND_CHECK("SQLNumResultCols", hstmt, SQLNumResultCols, hstmt, &cols);
	REQUIRE(cols == 2);
	DATA_CHECK(hstmt, 1, "physical_plan");

	EXECUTE_AND_CHECK("SQLFreeStmt (HSTMT)", hstmt, SQLFreeStmt, hstmt, SQL_CLOSE);
	EXECUTE_AND_CHECK("SQLFreeHandle (HSTMT)", hstmt, SQLFreeHandle, SQL_HANDLE_STMT, hstmt);
	DISCONNECT_FROM_DATABASE(env, dbc);
}

TEST_CASE("Test CALL output", "[odbc]") {
	SQLHANDLE env;
	SQLHANDLE dbc;

	HSTMT hstmt = SQL_NULL_HSTMT;

	CONNECT_TO_DATABASE(env, dbc);
	EXECUTE_AND_CHECK("SQLAllocHandle (HSTMT)", hstmt, SQLAllocHandle, SQL_HANDLE_STMT, dbc, &hstmt);

	EXECUTE_AND_CHECK("SQLExecDirect", hstmt, SQLExecDirect, hstmt,
	                  ConvertToSQLCHAR("CREATE MACRO hello() AS TABLE SELECT 'Hello' AS column1, 'World' AS column2"),
	                  SQL_NTS);
	EXECUTE_AND_CHECK("SQLExecDirect", hstmt, SQLExecDirect, hstmt, ConvertToSQLCHAR("CALL hello()"), SQL_NTS);

	EXECUTE_AND_CHECK("SQLFetch", hstmt, SQLFetch, hstmt);
	SQLSMALLINT cols = 0;
	EXECUTE_AND_CHECK("SQLNumResultCols", hstmt, SQLNumResultCols, hstmt, &cols);
	REQUIRE(cols == 2);
	DATA_CHECK(hstmt, 1, "Hello");
	DATA_CHECK(hstmt, 2, "World");

	EXECUTE_AND_CHECK("SQLFreeStmt (HSTMT)", hstmt, SQLFreeStmt, hstmt, SQL_CLOSE);
	EXECUTE_AND_CHECK("SQLFreeHandle (HSTMT)", hstmt, SQLFreeHandle, SQL_HANDLE_STMT, hstmt);
	DISCONNECT_FROM_DATABASE(env, dbc);
}

TEST_CASE("Test EXECUTE output", "[odbc]") {
	SQLHANDLE env;
	SQLHANDLE dbc;

	HSTMT hstmt = SQL_NULL_HSTMT;

	CONNECT_TO_DATABASE(env, dbc);
	EXECUTE_AND_CHECK("SQLAllocHandle (HSTMT)", hstmt, SQLAllocHandle, SQL_HANDLE_STMT, dbc, &hstmt);

	EXECUTE_AND_CHECK("SQLExecDirect", hstmt, SQLExecDirect, hstmt,
	                  ConvertToSQLCHAR("PREPARE p AS SELECT 'Hello' AS column1, 'World' AS column2"), SQL_NTS);
	EXECUTE_AND_CHECK("SQLExecDirect", hstmt, SQLExecDirect, hstmt, ConvertToSQLCHAR("EXECUTE p"), SQL_NTS);

	EXECUTE_AND_CHECK("SQLFetch", hstmt, SQLFetch, hstmt);
	SQLSMALLINT cols = 0;
	EXECUTE_AND_CHECK("SQLNumResultCols", hstmt, SQLNumResultCols, hstmt, &cols);
	REQUIRE(cols == 2);
	DATA_CHECK(hstmt, 1, "Hello");
	DATA_CHECK(hstmt, 2, "World");

	EXECUTE_AND_CHECK("SQLFreeStmt (HSTMT)", hstmt, SQLFreeStmt, hstmt, SQL_CLOSE);
	EXECUTE_AND_CHECK("SQLFreeHandle (HSTMT)", hstmt, SQLFreeHandle, SQL_HANDLE_STMT, hstmt);
	DISCONNECT_FROM_DATABASE(env, dbc);
}

TEST_CASE("Test the result columns after a failed SQLExecDirect", "[odbc]") {
	SQLHANDLE env;
	SQLHANDLE dbc;
	HSTMT hstmt = SQL_NULL_HSTMT;
	CONNECT_TO_DATABASE(env, dbc);
	EXECUTE_AND_CHECK("SQLAllocHandle (HSTMT)", hstmt, SQLAllocHandle, SQL_HANDLE_STMT, dbc, &hstmt);

	EXECUTE_AND_CHECK("SQLExecDirect", hstmt, SQLExecDirect, hstmt, ConvertToSQLCHAR("SELECT 1 AS a, 2 AS b, 3 AS c"),
	                  SQL_NTS);
	SQLSMALLINT cols = 0;
	EXECUTE_AND_CHECK("SQLNumResultCols", hstmt, SQLNumResultCols, hstmt, &cols);
	REQUIRE(cols == 3);
	EXECUTE_AND_CHECK("SQLFetch", hstmt, SQLFetch, hstmt);

	// a column past the last one
	SQLINTEGER value = 0;
	SQLLEN indicator = 0;
	REQUIRE(SQLGetData(hstmt, 5, SQL_C_SLONG, &value, sizeof(value), &indicator) == SQL_ERROR);
	std::string state;
	std::string message;
	ACCESS_DIAGNOSTIC(state, message, hstmt, SQL_HANDLE_STMT);
	REQUIRE(state == "07009");
	EXECUTE_AND_CHECK("SQLFreeStmt (SQL_CLOSE)", hstmt, SQLFreeStmt, hstmt, SQL_CLOSE);

	// fails when it is executed: it has no result columns, rather than those of the previous statement
	REQUIRE(SQLExecDirect(hstmt, ConvertToSQLCHAR("SELECT * FROM table_that_does_not_exist"), SQL_NTS) == SQL_ERROR);
	cols = -1;
	EXECUTE_AND_CHECK("SQLNumResultCols", hstmt, SQLNumResultCols, hstmt, &cols);
	REQUIRE(cols == 0);

	EXECUTE_AND_CHECK("SQLFreeHandle (HSTMT)", hstmt, SQLFreeHandle, SQL_HANDLE_STMT, hstmt);
	DISCONNECT_FROM_DATABASE(env, dbc);
}
