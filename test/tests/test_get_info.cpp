#include "odbc_test_common.h"

#include <string>
#include <vector>

using namespace odbc_test;

// SQL_DBMS_VER has to be the version the engine itself reports
TEST_CASE("Test SQLGetInfo SQL_DBMS_VER", "[odbc]") {
	SQLHANDLE env;
	SQLHANDLE dbc;
	HSTMT hstmt = SQL_NULL_HSTMT;

	CONNECT_TO_DATABASE(env, dbc);

	// The version according to the engine
	EXECUTE_AND_CHECK("SQLAllocHandle (HSTMT)", hstmt, SQLAllocHandle, SQL_HANDLE_STMT, dbc, &hstmt);
	EXECUTE_AND_CHECK("SQLExecDirect", hstmt, SQLExecDirect, hstmt,
	                  ConvertToSQLCHAR("SELECT library_version FROM pragma_version()"), SQL_NTS);
	EXECUTE_AND_CHECK("SQLFetch", hstmt, SQLFetch, hstmt);
	std::vector<char> expected_buf(64, '\0');
	SQLLEN expected_len = 0;
	EXECUTE_AND_CHECK("SQLGetData", hstmt, SQLGetData, hstmt, 1, SQL_C_CHAR, expected_buf.data(),
	                  static_cast<SQLLEN>(expected_buf.size()), &expected_len);
	std::string expected(expected_buf.data(), static_cast<size_t>(expected_len));
	REQUIRE(!expected.empty());
	EXECUTE_AND_CHECK("SQLFreeStmt (HSTMT)", hstmt, SQLFreeStmt, hstmt, SQL_CLOSE);
	EXECUTE_AND_CHECK("SQLFreeHandle (HSTMT)", hstmt, SQLFreeHandle, SQL_HANDLE_STMT, hstmt);

	SECTION("SQLGetInfo") {
		std::vector<char> buf(64, 'x');
		SQLSMALLINT len = -1;
		SQLRETURN ret = SQLGetInfo(dbc, SQL_DBMS_VER, buf.data(), static_cast<SQLSMALLINT>(buf.size()), &len);
		REQUIRE(ret == SQL_SUCCESS);
		REQUIRE(len == static_cast<SQLSMALLINT>(expected.size()));
		REQUIRE(std::string(buf.data()) == expected);
	}

	SECTION("SQLGetInfo length only") {
		SQLSMALLINT len = -1;
		SQLRETURN ret = SQLGetInfo(dbc, SQL_DBMS_VER, nullptr, 0, &len);
		REQUIRE(ret == SQL_SUCCESS);
		REQUIRE(len == static_cast<SQLSMALLINT>(expected.size()));
	}

	SECTION("SQLGetInfoW") {
		SQLSMALLINT len = -1;
		SQLRETURN ret = SQLGetInfoW(dbc, SQL_DBMS_VER, nullptr, 0, &len);
		REQUIRE(ret == SQL_SUCCESS);
		REQUIRE(len == static_cast<SQLSMALLINT>(expected.size() * sizeof(SQLWCHAR)));
	}

	DISCONNECT_FROM_DATABASE(env, dbc);
}
