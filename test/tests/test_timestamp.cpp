#include "odbc_test_common.h"

using namespace odbc_test;

TEST_CASE("Test SQLBindParameter with TIMESTAMP type", "[odbc]") {
	SQLHANDLE env;
	SQLHANDLE dbc;

	HSTMT hstmt = SQL_NULL_HSTMT;

	// Connect to the database using SQLConnect
	CONNECT_TO_DATABASE(env, dbc);

	// Allocate a statement handle
	EXECUTE_AND_CHECK("SQLAllocHandle (HSTMT)", hstmt, SQLAllocHandle, SQL_HANDLE_STMT, dbc, &hstmt);

	// Create table
	EXECUTE_AND_CHECK("SQLExecDirect", hstmt, SQLExecDirect, hstmt,
	                  ConvertToSQLCHAR("CREATE TABLE timestamp_bind_test (col1 TIMESTAMP)"), SQL_NTS);

	// Insert value
	EXECUTE_AND_CHECK("SQLPrepare", hstmt, SQLPrepare, hstmt,
	                  ConvertToSQLCHAR("INSERT INTO timestamp_bind_test VALUES (?)"), SQL_NTS);
	SQL_TIMESTAMP_STRUCT param = {2000, 1, 1, 12, 10, 30, 0};
	SQLLEN param_len = sizeof(param);
	EXECUTE_AND_CHECK("SQLBindParameter", hstmt, SQLBindParameter, hstmt, 1, SQL_PARAM_INPUT, SQL_C_TYPE_TIMESTAMP,
	                  SQL_TYPE_TIMESTAMP, 0, 0, &param, param_len, &param_len);
	EXECUTE_AND_CHECK("SQLExecute", hstmt, SQLExecute, hstmt);

	// Fetch and check
	EXECUTE_AND_CHECK("SQLExecDirect", hstmt, SQLExecDirect, hstmt,
	                  ConvertToSQLCHAR("SELECT * FROM timestamp_bind_test"), SQL_NTS);
	EXECUTE_AND_CHECK("SQLFetch", hstmt, SQLFetch, hstmt);
	SQL_TIMESTAMP_STRUCT fetched;
	EXECUTE_AND_CHECK("SQLGetData", hstmt, SQLGetData, hstmt, 1, SQL_C_TYPE_TIMESTAMP, &fetched, sizeof(fetched),
	                  nullptr);
	REQUIRE(fetched.year == param.year);
	REQUIRE(fetched.month == param.month);
	REQUIRE(fetched.day == param.day);
	REQUIRE(fetched.hour == param.hour);
	REQUIRE(fetched.minute == param.minute);
	REQUIRE(fetched.second == param.second);
	REQUIRE(fetched.fraction == param.fraction);

	// Free the statement handle
	EXECUTE_AND_CHECK("SQLFreeStmt (HSTMT)", hstmt, SQLFreeStmt, hstmt, SQL_CLOSE);
	EXECUTE_AND_CHECK("SQLFreeHandle (HSTMT)", hstmt, SQLFreeHandle, SQL_HANDLE_STMT, hstmt);

	DISCONNECT_FROM_DATABASE(env, dbc);
}

TEST_CASE("Test SQLBindParameter with DATE type", "[odbc]") {
	SQLHANDLE env;
	SQLHANDLE dbc;

	HSTMT hstmt = SQL_NULL_HSTMT;

	// Connect to the database using SQLConnect
	CONNECT_TO_DATABASE(env, dbc);

	// Allocate a statement handle
	EXECUTE_AND_CHECK("SQLAllocHandle (HSTMT)", hstmt, SQLAllocHandle, SQL_HANDLE_STMT, dbc, &hstmt);

	// Create table
	EXECUTE_AND_CHECK("SQLExecDirect", hstmt, SQLExecDirect, hstmt,
	                  ConvertToSQLCHAR("CREATE TABLE date_bind_test (col1 DATE)"), SQL_NTS);

	// Insert value
	EXECUTE_AND_CHECK("SQLPrepare", hstmt, SQLPrepare, hstmt, ConvertToSQLCHAR("INSERT INTO date_bind_test VALUES (?)"),
	                  SQL_NTS);
	SQL_DATE_STRUCT param = {2000, 1, 2};
	SQLLEN param_len = sizeof(param);
	EXECUTE_AND_CHECK("SQLBindParameter", hstmt, SQLBindParameter, hstmt, 1, SQL_PARAM_INPUT, SQL_C_TYPE_DATE,
	                  SQL_TYPE_DATE, 0, 0, &param, param_len, &param_len);
	EXECUTE_AND_CHECK("SQLExecute", hstmt, SQLExecute, hstmt);

	{ // Fetch and check as DATE
		EXECUTE_AND_CHECK("SQLExecDirect", hstmt, SQLExecDirect, hstmt,
		                  ConvertToSQLCHAR("SELECT * FROM date_bind_test"), SQL_NTS);
		EXECUTE_AND_CHECK("SQLFetch", hstmt, SQLFetch, hstmt);
		SQL_DATE_STRUCT fetched;
		EXECUTE_AND_CHECK("SQLGetData", hstmt, SQLGetData, hstmt, 1, SQL_C_TYPE_DATE, &fetched, sizeof(fetched),
		                  nullptr);
		REQUIRE(fetched.year == param.year);
		REQUIRE(fetched.month == param.month);
		REQUIRE(fetched.day == param.day);
	}

	{ // Fetch and check as TIMESTAMP
		EXECUTE_AND_CHECK("SQLExecDirect", hstmt, SQLExecDirect, hstmt,
		                  ConvertToSQLCHAR("SELECT * FROM date_bind_test"), SQL_NTS);
		EXECUTE_AND_CHECK("SQLFetch", hstmt, SQLFetch, hstmt);
		SQL_TIMESTAMP_STRUCT fetched;
		EXECUTE_AND_CHECK("SQLGetData", hstmt, SQLGetData, hstmt, 1, SQL_C_TYPE_TIMESTAMP, &fetched, sizeof(fetched),
		                  nullptr);
		REQUIRE(fetched.year == param.year);
		REQUIRE(fetched.month == param.month);
		REQUIRE(fetched.day == param.day);
		REQUIRE(fetched.hour == 0);
		REQUIRE(fetched.minute == 0);
		REQUIRE(fetched.second == 0);
		REQUIRE(fetched.fraction == 0);
	}

	// Free the statement handle
	EXECUTE_AND_CHECK("SQLFreeStmt (HSTMT)", hstmt, SQLFreeStmt, hstmt, SQL_CLOSE);
	EXECUTE_AND_CHECK("SQLFreeHandle (HSTMT)", hstmt, SQLFreeHandle, SQL_HANDLE_STMT, hstmt);

	DISCONNECT_FROM_DATABASE(env, dbc);
}

TEST_CASE("Test SQLBindParameter with TIME type", "[odbc]") {
	SQLHANDLE env;
	SQLHANDLE dbc;

	HSTMT hstmt = SQL_NULL_HSTMT;

	// Connect to the database using SQLConnect
	CONNECT_TO_DATABASE(env, dbc);

	// Allocate a statement handle
	EXECUTE_AND_CHECK("SQLAllocHandle (HSTMT)", hstmt, SQLAllocHandle, SQL_HANDLE_STMT, dbc, &hstmt);

	// Create table
	EXECUTE_AND_CHECK("SQLExecDirect", hstmt, SQLExecDirect, hstmt,
	                  ConvertToSQLCHAR("CREATE TABLE time_bind_test (col1 TIME)"), SQL_NTS);

	// Insert value
	EXECUTE_AND_CHECK("SQLPrepare", hstmt, SQLPrepare, hstmt, ConvertToSQLCHAR("INSERT INTO time_bind_test VALUES (?)"),
	                  SQL_NTS);
	SQL_TIME_STRUCT param = {12, 34, 56};
	SQLLEN param_len = sizeof(param);
	EXECUTE_AND_CHECK("SQLBindParameter", hstmt, SQLBindParameter, hstmt, 1, SQL_PARAM_INPUT, SQL_C_TYPE_TIME,
	                  SQL_TYPE_TIME, 0, 0, &param, param_len, &param_len);
	EXECUTE_AND_CHECK("SQLExecute", hstmt, SQLExecute, hstmt);

	{ // Fetch and check as TIME
		EXECUTE_AND_CHECK("SQLExecDirect", hstmt, SQLExecDirect, hstmt,
		                  ConvertToSQLCHAR("SELECT * FROM time_bind_test"), SQL_NTS);
		EXECUTE_AND_CHECK("SQLFetch", hstmt, SQLFetch, hstmt);
		SQL_TIME_STRUCT fetched;
		EXECUTE_AND_CHECK("SQLGetData", hstmt, SQLGetData, hstmt, 1, SQL_C_TYPE_TIME, &fetched, sizeof(fetched),
		                  nullptr);
		REQUIRE(fetched.hour == param.hour);
		REQUIRE(fetched.minute == param.minute);
		REQUIRE(fetched.second == param.second);
	}

	{ // Fetch and check as TIMESTAMP
		EXECUTE_AND_CHECK("SQLExecDirect", hstmt, SQLExecDirect, hstmt,
		                  ConvertToSQLCHAR("SELECT * FROM time_bind_test"), SQL_NTS);
		EXECUTE_AND_CHECK("SQLFetch", hstmt, SQLFetch, hstmt);
		SQL_TIMESTAMP_STRUCT fetched;
		EXECUTE_AND_CHECK("SQLGetData", hstmt, SQLGetData, hstmt, 1, SQL_C_TYPE_TIMESTAMP, &fetched, sizeof(fetched),
		                  nullptr);
		REQUIRE(fetched.year == 1970);
		REQUIRE(fetched.month == 1);
		REQUIRE(fetched.day == 1);
		REQUIRE(fetched.hour == param.hour);
		REQUIRE(fetched.minute == param.minute);
		REQUIRE(fetched.second == param.second);
		REQUIRE(fetched.fraction == 0);
	}

	// Free the statement handle
	EXECUTE_AND_CHECK("SQLFreeStmt (HSTMT)", hstmt, SQLFreeStmt, hstmt, SQL_CLOSE);
	EXECUTE_AND_CHECK("SQLFreeHandle (HSTMT)", hstmt, SQLFreeHandle, SQL_HANDLE_STMT, hstmt);

	DISCONNECT_FROM_DATABASE(env, dbc);
}
