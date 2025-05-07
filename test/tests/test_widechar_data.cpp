#include "odbc_test_common.h"

#include <cstring>

#include "widechar.hpp"

using namespace odbc_test;

static std::vector<SQLCHAR> hello_bg_utf8 = {0xd0, 0x97, 0xd0, 0xb4, 0xd1, 0x80, 0xd0, 0xb0, 0xd0,
                                             0xb2, 0xd0, 0xb5, 0xd0, 0xb9, 0xd1, 0x82, 0xd0, 0xb5};
static std::vector<SQLWCHAR> hello_bg_utf16 = {0x0417, 0x0434, 0x0440, 0x0430, 0x0432, 0x0435, 0x0439, 0x0442, 0x0435};

TEST_CASE("Test SQLBindParameter with WVARCHAR type", "[odbc]") {
	SQLHANDLE env;
	SQLHANDLE dbc;

	HSTMT hstmt = SQL_NULL_HSTMT;

	// Connect to the database using SQLConnect
	CONNECT_TO_DATABASE(env, dbc);

	// Allocate a statement handle
	EXECUTE_AND_CHECK("SQLAllocHandle (HSTMT)", hstmt, SQLAllocHandle, SQL_HANDLE_STMT, dbc, &hstmt);

	// Create table
	EXECUTE_AND_CHECK("SQLExecDirect", hstmt, SQLExecDirect, hstmt,
	                  ConvertToSQLCHAR("CREATE TABLE widechar_bind_test (col1 VARCHAR)"), SQL_NTS);

	// Insert value
	EXECUTE_AND_CHECK("SQLPrepare", hstmt, SQLPrepare, hstmt,
	                  ConvertToSQLCHAR("INSERT INTO widechar_bind_test VALUES (?)"), SQL_NTS);
	const SQLLEN hello_bg_utf16_len_bytes = hello_bg_utf16.size() * sizeof(SQLWCHAR);
	SQLLEN ind = hello_bg_utf16_len_bytes;
	EXECUTE_AND_CHECK("SQLBindParameter", hstmt, SQLBindParameter, hstmt, 1, SQL_PARAM_INPUT, SQL_C_WCHAR, SQL_WVARCHAR,
	                  hello_bg_utf16_len_bytes, 0, hello_bg_utf16.data(), hello_bg_utf16_len_bytes, &ind);
	EXECUTE_AND_CHECK("SQLExecute", hstmt, SQLExecute, hstmt);

	// Fetch and check UTF-16
	EXECUTE_AND_CHECK("SQLExecDirect", hstmt, SQLExecDirect, hstmt,
	                  ConvertToSQLCHAR("SELECT * FROM widechar_bind_test"), SQL_NTS);
	EXECUTE_AND_CHECK("SQLFetch", hstmt, SQLFetch, hstmt);
	std::vector<SQLWCHAR> buf_utf16;
	buf_utf16.resize(256);
	EXECUTE_AND_CHECK("SQLGetData", hstmt, SQLGetData, hstmt, 1, SQL_C_WCHAR, buf_utf16.data(),
	                  buf_utf16.size() * sizeof(SQLWCHAR), nullptr);
	REQUIRE(duckdb::widechar::utf16_length(buf_utf16.data()) == hello_bg_utf16.size());
	REQUIRE(std::equal(hello_bg_utf16.begin(), hello_bg_utf16.end(), buf_utf16.begin()));

	// Fetch and check UTF-8
	EXECUTE_AND_CHECK("SQLExecDirect", hstmt, SQLExecDirect, hstmt,
	                  ConvertToSQLCHAR("SELECT * FROM widechar_bind_test"), SQL_NTS);
	EXECUTE_AND_CHECK("SQLFetch", hstmt, SQLFetch, hstmt);
	std::vector<SQLCHAR> buf_utf8;
	buf_utf8.resize(256);
	EXECUTE_AND_CHECK("SQLGetData", hstmt, SQLGetData, hstmt, 1, SQL_C_CHAR, buf_utf8.data(), buf_utf8.size(), nullptr);
	REQUIRE(std::strlen(reinterpret_cast<char *>(buf_utf8.data())) == hello_bg_utf8.size());
	REQUIRE(std::equal(hello_bg_utf8.begin(), hello_bg_utf8.end(), buf_utf8.begin()));

	// Free the statement handle
	EXECUTE_AND_CHECK("SQLFreeStmt (HSTMT)", hstmt, SQLFreeStmt, hstmt, SQL_CLOSE);
	EXECUTE_AND_CHECK("SQLFreeHandle (HSTMT)", hstmt, SQLFreeHandle, SQL_HANDLE_STMT, hstmt);

	DISCONNECT_FROM_DATABASE(env, dbc);
}
