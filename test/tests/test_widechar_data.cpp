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

// ASCII -> UTF-16, for building SQL_C_WCHAR parameter buffers in tests.
static std::vector<SQLWCHAR> Widen(const std::string &s) {
	return std::vector<SQLWCHAR>(s.begin(), s.end());
}

static SQLBIGINT FetchCount(HSTMT hstmt) {
	EXECUTE_AND_CHECK("SQLFetch", hstmt, SQLFetch, hstmt);
	SQLBIGINT count = -1;
	EXECUTE_AND_CHECK("SQLGetData (count)", hstmt, SQLGetData, hstmt, 1, SQL_C_SBIGINT, &count, sizeof(count), nullptr);
	return count;
}

TEST_CASE("Test SQLBindParameter with mismatched C and SQL types", "[odbc]") {
	SQLHANDLE env;
	SQLHANDLE dbc;
	HSTMT hstmt = SQL_NULL_HSTMT;

	CONNECT_TO_DATABASE(env, dbc);
	EXECUTE_AND_CHECK("SQLAllocHandle (HSTMT)", hstmt, SQLAllocHandle, SQL_HANDLE_STMT, dbc, &hstmt);
	EXECUTE_AND_CHECK("SQLExecDirect (create)", hstmt, SQLExecDirect, hstmt,
	                  ConvertToSQLCHAR("CREATE OR REPLACE TABLE param_mix (region VARCHAR, amount DECIMAL(10,2), "
	                                   "d DATE, ts TIMESTAMP, tm TIME, flag BOOLEAN, big HUGEINT)"),
	                  SQL_NTS);
	EXECUTE_AND_CHECK("SQLExecDirect (insert)", hstmt, SQLExecDirect, hstmt,
	                  ConvertToSQLCHAR("INSERT INTO param_mix VALUES "
	                                   "('North', 42.00, DATE '2024-01-01', TIMESTAMP '2024-01-01 12:34:56', "
	                                   "TIME '12:34:56', true, 123456789012345678901234567890), "
	                                   "('South', 7.50, DATE '2024-02-02', TIMESTAMP '2024-02-02 01:02:03', "
	                                   "TIME '01:02:03', false, 1)"),
	                  SQL_NTS);

	SECTION("SQL_C_WCHAR buffer bound as SQL_VARCHAR") {
		auto buf = Widen("North");
		SQLLEN ind = static_cast<SQLLEN>(buf.size() * sizeof(SQLWCHAR));
		EXECUTE_AND_CHECK("SQLPrepare", hstmt, SQLPrepare, hstmt,
		                  ConvertToSQLCHAR("SELECT count(*) FROM param_mix WHERE region = ?"), SQL_NTS);
		EXECUTE_AND_CHECK("SQLBindParameter", hstmt, SQLBindParameter, hstmt, 1, SQL_PARAM_INPUT, SQL_C_WCHAR,
		                  SQL_VARCHAR, buf.size(), 0, buf.data(), ind, &ind);
		EXECUTE_AND_CHECK("SQLExecute", hstmt, SQLExecute, hstmt);
		REQUIRE(FetchCount(hstmt) == 1);
	}

	// A DECIMAL slicer arrives from Power BI as a text buffer bound against SQL_DECIMAL.
	SECTION("SQL_C_WCHAR buffer bound as SQL_DECIMAL") {
		auto buf = Widen("42.00");
		SQLLEN ind = static_cast<SQLLEN>(buf.size() * sizeof(SQLWCHAR));
		EXECUTE_AND_CHECK("SQLPrepare", hstmt, SQLPrepare, hstmt,
		                  ConvertToSQLCHAR("SELECT count(*) FROM param_mix WHERE amount = ?"), SQL_NTS);
		EXECUTE_AND_CHECK("SQLBindParameter", hstmt, SQLBindParameter, hstmt, 1, SQL_PARAM_INPUT, SQL_C_WCHAR,
		                  SQL_DECIMAL, 10, 2, buf.data(), ind, &ind);
		EXECUTE_AND_CHECK("SQLExecute", hstmt, SQLExecute, hstmt);
		REQUIRE(FetchCount(hstmt) == 1);
	}

	// Same path via a narrow SQL_C_CHAR buffer.
	SECTION("SQL_C_CHAR buffer bound as SQL_DECIMAL") {
		std::string s = "42.00";
		SQLLEN ind = static_cast<SQLLEN>(s.size());
		EXECUTE_AND_CHECK("SQLPrepare", hstmt, SQLPrepare, hstmt,
		                  ConvertToSQLCHAR("SELECT count(*) FROM param_mix WHERE amount = ?"), SQL_NTS);
		EXECUTE_AND_CHECK("SQLBindParameter", hstmt, SQLBindParameter, hstmt, 1, SQL_PARAM_INPUT, SQL_C_CHAR,
		                  SQL_DECIMAL, 10, 2, (SQLPOINTER)s.data(), ind, &ind);
		EXECUTE_AND_CHECK("SQLExecute", hstmt, SQLExecute, hstmt);
		REQUIRE(FetchCount(hstmt) == 1);
	}

	// A DATE filter arrives as text bound against SQL_TYPE_DATE.
	SECTION("SQL_C_WCHAR buffer bound as SQL_TYPE_DATE") {
		auto buf = Widen("2024-01-01");
		SQLLEN ind = static_cast<SQLLEN>(buf.size() * sizeof(SQLWCHAR));
		EXECUTE_AND_CHECK("SQLPrepare", hstmt, SQLPrepare, hstmt,
		                  ConvertToSQLCHAR("SELECT count(*) FROM param_mix WHERE d = ?"), SQL_NTS);
		EXECUTE_AND_CHECK("SQLBindParameter", hstmt, SQLBindParameter, hstmt, 1, SQL_PARAM_INPUT, SQL_C_WCHAR,
		                  SQL_TYPE_DATE, 10, 0, buf.data(), ind, &ind);
		EXECUTE_AND_CHECK("SQLExecute", hstmt, SQLExecute, hstmt);
		REQUIRE(FetchCount(hstmt) == 1);
	}

	SECTION("SQL_C_WCHAR buffer bound as SQL_TYPE_TIMESTAMP") {
		auto buf = Widen("2024-01-01 12:34:56");
		SQLLEN ind = static_cast<SQLLEN>(buf.size() * sizeof(SQLWCHAR));
		EXECUTE_AND_CHECK("SQLPrepare", hstmt, SQLPrepare, hstmt,
		                  ConvertToSQLCHAR("SELECT count(*) FROM param_mix WHERE ts = ?"), SQL_NTS);
		EXECUTE_AND_CHECK("SQLBindParameter", hstmt, SQLBindParameter, hstmt, 1, SQL_PARAM_INPUT, SQL_C_WCHAR,
		                  SQL_TYPE_TIMESTAMP, 23, 3, buf.data(), ind, &ind);
		EXECUTE_AND_CHECK("SQLExecute", hstmt, SQLExecute, hstmt);
		REQUIRE(FetchCount(hstmt) == 1);
	}

	SECTION("SQL_C_WCHAR buffer bound as SQL_TYPE_TIME") {
		auto buf = Widen("12:34:56");
		SQLLEN ind = static_cast<SQLLEN>(buf.size() * sizeof(SQLWCHAR));
		EXECUTE_AND_CHECK("SQLPrepare", hstmt, SQLPrepare, hstmt,
		                  ConvertToSQLCHAR("SELECT count(*) FROM param_mix WHERE tm = ?"), SQL_NTS);
		EXECUTE_AND_CHECK("SQLBindParameter", hstmt, SQLBindParameter, hstmt, 1, SQL_PARAM_INPUT, SQL_C_WCHAR,
		                  SQL_TYPE_TIME, 8, 0, buf.data(), ind, &ind);
		EXECUTE_AND_CHECK("SQLExecute", hstmt, SQLExecute, hstmt);
		REQUIRE(FetchCount(hstmt) == 1);
	}

	// Boolean: DuckDB casts the text 'true' to BOOLEAN for the comparison.
	SECTION("SQL_C_WCHAR buffer bound as SQL_BIT (boolean)") {
		auto buf = Widen("true");
		SQLLEN ind = static_cast<SQLLEN>(buf.size() * sizeof(SQLWCHAR));
		EXECUTE_AND_CHECK("SQLPrepare", hstmt, SQLPrepare, hstmt,
		                  ConvertToSQLCHAR("SELECT count(*) FROM param_mix WHERE flag = ?"), SQL_NTS);
		EXECUTE_AND_CHECK("SQLBindParameter", hstmt, SQLBindParameter, hstmt, 1, SQL_PARAM_INPUT, SQL_C_WCHAR,
		                  SQL_BIT, 5, 0, buf.data(), ind, &ind);
		EXECUTE_AND_CHECK("SQLExecute", hstmt, SQLExecute, hstmt);
		REQUIRE(FetchCount(hstmt) == 1);
	}

	// HUGEINT exceeds 64 bits; binding it as text is the only way it round-trips.
	SECTION("SQL_C_WCHAR buffer bound as SQL_NUMERIC (hugeint)") {
		auto buf = Widen("123456789012345678901234567890");
		SQLLEN ind = static_cast<SQLLEN>(buf.size() * sizeof(SQLWCHAR));
		EXECUTE_AND_CHECK("SQLPrepare", hstmt, SQLPrepare, hstmt,
		                  ConvertToSQLCHAR("SELECT count(*) FROM param_mix WHERE big = ?"), SQL_NTS);
		EXECUTE_AND_CHECK("SQLBindParameter", hstmt, SQLBindParameter, hstmt, 1, SQL_PARAM_INPUT, SQL_C_WCHAR,
		                  SQL_NUMERIC, 30, 0, buf.data(), ind, &ind);
		EXECUTE_AND_CHECK("SQLExecute", hstmt, SQLExecute, hstmt);
		REQUIRE(FetchCount(hstmt) == 1);
	}

	EXECUTE_AND_CHECK("SQLFreeStmt (HSTMT)", hstmt, SQLFreeStmt, hstmt, SQL_CLOSE);
	EXECUTE_AND_CHECK("SQLFreeHandle (HSTMT)", hstmt, SQLFreeHandle, SQL_HANDLE_STMT, hstmt);
	DISCONNECT_FROM_DATABASE(env, dbc);
}
