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
