#include "odbc_test_common.h"

using namespace odbc_test;

TEST_CASE("Test 'bound_all_parameters=false' case with SQLExecDirect", "[odbc]") {
	SQLHANDLE env;
	SQLHANDLE dbc;

	HSTMT hstmt = SQL_NULL_HSTMT;

	// Connect to the database using SQLConnect
	CONNECT_TO_DATABASE(env, dbc);

	// Allocate a statement handle
	EXECUTE_AND_CHECK("SQLAllocHandle (HSTMT)", hstmt, SQLAllocHandle, SQL_HANDLE_STMT, dbc, &hstmt);

	// Supply parameters
	int32_t param1 = 42;
	SQLLEN param1_len = sizeof(param1);
	EXECUTE_AND_CHECK("SQLBindParameter (param)", hstmt, SQLBindParameter, hstmt, 1, SQL_PARAM_INPUT, SQL_C_SLONG,
	                  SQL_INTEGER, 0, 0, &param1, param1_len, &param1_len);
	std::string param2 = "foo";
	SQLLEN param2_len = param2.length();
	EXECUTE_AND_CHECK("SQLBindParameter (param)", hstmt, SQLBindParameter, hstmt, 2, SQL_PARAM_INPUT, SQL_C_CHAR,
	                  SQL_VARCHAR, param2_len, 0, const_cast<char *>(param2.c_str()), param2_len, &param2_len);

	// Run the query
	SQLRETURN ret_exec = SQLExecDirect(hstmt, ConvertToSQLCHAR("SELECT ?, ?"), SQL_NTS);
	REQUIRE(ret_exec == SQL_SUCCESS_WITH_INFO);

	// Check that IRD was re-filled correctly
	SQLLEN ctype = -1;
	EXECUTE_AND_CHECK("SQLColAttribute", hstmt, SQLColAttribute, hstmt, 1, SQL_DESC_CONCISE_TYPE, nullptr, 0, nullptr,
	                  &ctype);
	REQUIRE(ctype == SQL_INTEGER);
	EXECUTE_AND_CHECK("SQLColAttribute", hstmt, SQLColAttribute, hstmt, 2, SQL_DESC_CONCISE_TYPE, nullptr, 0, nullptr,
	                  &ctype);
	REQUIRE(ctype == SQL_VARCHAR);

	// Fetch and check the data
	EXECUTE_AND_CHECK("SQLFetch", hstmt, SQLFetch, hstmt);
	int32_t fetched1 = -1;
	EXECUTE_AND_CHECK("SQLGetData", hstmt, SQLGetData, hstmt, 1, SQL_C_SLONG, &fetched1, sizeof(fetched1), nullptr);
	REQUIRE(fetched1 == param1);
	std::vector<char> fetched2(64);
	EXECUTE_AND_CHECK("SQLGetData", hstmt, SQLGetData, hstmt, 2, SQL_C_CHAR, fetched2.data(), sizeof(fetched2),
	                  nullptr);
	REQUIRE(std::string(fetched2.data()) == param2);

	// Free the statement handle
	EXECUTE_AND_CHECK("SQLFreeStmt (HSTMT)", hstmt, SQLFreeStmt, hstmt, SQL_CLOSE);
	EXECUTE_AND_CHECK("SQLFreeHandle (HSTMT)", hstmt, SQLFreeHandle, SQL_HANDLE_STMT, hstmt);

	DISCONNECT_FROM_DATABASE(env, dbc);
}

TEST_CASE("Test 'bound_all_parameters=false' case with SQLPrepare and SQLExecute", "[odbc]") {
	SQLHANDLE env;
	SQLHANDLE dbc;

	HSTMT hstmt = SQL_NULL_HSTMT;

	int32_t param1 = 42;
	SQLLEN param1_len = sizeof(param1);
	std::string param2 = "foo";
	SQLLEN param2_len = param2.length();

	// Connect to the database using SQLConnect
	CONNECT_TO_DATABASE(env, dbc);

	// Allocate a statement handle
	EXECUTE_AND_CHECK("SQLAllocHandle (HSTMT)", hstmt, SQLAllocHandle, SQL_HANDLE_STMT, dbc, &hstmt);

	// Prepare the query
	SQLRETURN ret_prepare = SQLPrepare(hstmt, ConvertToSQLCHAR("SELECT ?, ?"), SQL_NTS);
	REQUIRE(ret_prepare == SQL_SUCCESS_WITH_INFO);

	// Supply parameters
	EXECUTE_AND_CHECK("SQLBindParameter", hstmt, SQLBindParameter, hstmt, 1, SQL_PARAM_INPUT, SQL_C_SLONG, SQL_INTEGER,
	                  0, 0, &param1, param1_len, &param1_len);
	EXECUTE_AND_CHECK("SQLBindParameter", hstmt, SQLBindParameter, hstmt, 2, SQL_PARAM_INPUT, SQL_C_CHAR, SQL_VARCHAR,
	                  param2_len, 0, const_cast<char *>(param2.c_str()), param2_len, &param2_len);

	// Run the query
	EXECUTE_AND_CHECK("SQLExecute", hstmt, SQLExecute, hstmt);

	// Check that IRD was re-filled correctly
	{
		SQLLEN ctype = -1;
		EXECUTE_AND_CHECK("SQLColAttribute", hstmt, SQLColAttribute, hstmt, 1, SQL_DESC_CONCISE_TYPE, nullptr, 0,
		                  nullptr, &ctype);
		REQUIRE(ctype == SQL_INTEGER);
		EXECUTE_AND_CHECK("SQLColAttribute", hstmt, SQLColAttribute, hstmt, 2, SQL_DESC_CONCISE_TYPE, nullptr, 0,
		                  nullptr, &ctype);
		REQUIRE(ctype == SQL_VARCHAR);
	}

	// Fetch and check the data
	{
		EXECUTE_AND_CHECK("SQLFetch", hstmt, SQLFetch, hstmt);
		int32_t fetched1 = -1;
		EXECUTE_AND_CHECK("SQLGetData", hstmt, SQLGetData, hstmt, 1, SQL_C_SLONG, &fetched1, sizeof(fetched1), nullptr);
		REQUIRE(fetched1 == param1);
		std::vector<char> fetched2(64);
		EXECUTE_AND_CHECK("SQLGetData", hstmt, SQLGetData, hstmt, 2, SQL_C_CHAR, fetched2.data(), sizeof(fetched2),
		                  nullptr);
		REQUIRE(std::string(fetched2.data()) == param2);
	}

	// Reset parameters. TODO: investigate why this is required
	EXECUTE_AND_CHECK("SQLFreeStmt (PARAMS)", hstmt, SQLFreeStmt, hstmt, SQL_RESET_PARAMS);

	// Supply parameters in reverse order
	EXECUTE_AND_CHECK("SQLBindParameter", hstmt, SQLBindParameter, hstmt, 1, SQL_PARAM_INPUT, SQL_C_CHAR, SQL_VARCHAR,
	                  param2_len, 0, const_cast<char *>(param2.c_str()), param2_len, &param2_len);
	EXECUTE_AND_CHECK("SQLBindParameter", hstmt, SQLBindParameter, hstmt, 2, SQL_PARAM_INPUT, SQL_C_SLONG, SQL_INTEGER,
	                  0, 0, &param1, param1_len, &param1_len);

	// Run the query
	EXECUTE_AND_CHECK("SQLExecute", hstmt, SQLExecute, hstmt);

	// Check that IRD was re-filled correctly
	{
		SQLLEN ctype = -1;
		EXECUTE_AND_CHECK("SQLColAttribute", hstmt, SQLColAttribute, hstmt, 1, SQL_DESC_CONCISE_TYPE, nullptr, 0,
		                  nullptr, &ctype);
		REQUIRE(ctype == SQL_VARCHAR);
		EXECUTE_AND_CHECK("SQLColAttribute", hstmt, SQLColAttribute, hstmt, 2, SQL_DESC_CONCISE_TYPE, nullptr, 0,
		                  nullptr, &ctype);
		REQUIRE(ctype == SQL_INTEGER);
	}

	// Fetch and check the data
	{
		EXECUTE_AND_CHECK("SQLFetch", hstmt, SQLFetch, hstmt);
		std::vector<char> fetched1(64);
		EXECUTE_AND_CHECK("SQLGetData", hstmt, SQLGetData, hstmt, 1, SQL_C_CHAR, fetched1.data(), sizeof(fetched1),
		                  nullptr);
		REQUIRE(std::string(fetched1.data()) == param2);
		int32_t fetched2 = -1;
		EXECUTE_AND_CHECK("SQLGetData", hstmt, SQLGetData, hstmt, 2, SQL_C_SLONG, &fetched2, sizeof(fetched2), nullptr);
		REQUIRE(fetched2 == param1);
	}

	// Free the statement handle
	EXECUTE_AND_CHECK("SQLFreeStmt (HSTMT)", hstmt, SQLFreeStmt, hstmt, SQL_CLOSE);
	EXECUTE_AND_CHECK("SQLFreeHandle (HSTMT)", hstmt, SQLFreeHandle, SQL_HANDLE_STMT, hstmt);

	DISCONNECT_FROM_DATABASE(env, dbc);
}
