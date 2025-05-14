
#include "odbc_test_common.h"

using namespace odbc_test;

static void GetDataReady(SQLHANDLE &env, SQLHANDLE &dbc, HSTMT &hstmt, const std::string &value) {
	// Connect to the database using SQLConnect
	CONNECT_TO_DATABASE(env, dbc);

	// Allocate a statement handle
	EXECUTE_AND_CHECK("SQLAllocHandle (HSTMT)", hstmt, SQLAllocHandle, SQL_HANDLE_STMT, dbc, &hstmt);

	// Create table
	EXECUTE_AND_CHECK("SQLExecDirect", hstmt, SQLExecDirect, hstmt,
	                  ConvertToSQLCHAR("CREATE TABLE longdata_test (col1 VARCHAR)"), SQL_NTS);

	// Insert value
	EXECUTE_AND_CHECK("SQLPrepare", hstmt, SQLPrepare, hstmt, ConvertToSQLCHAR("INSERT INTO longdata_test VALUES (?)"),
	                  SQL_NTS);

	SQLLEN param_len = value.length();
	EXECUTE_AND_CHECK("SQLBindParameter", hstmt, SQLBindParameter, hstmt, 1, SQL_PARAM_INPUT, SQL_C_CHAR, SQL_VARCHAR,
	                  0, 0, static_cast<void *>(const_cast<char *>(value.c_str())), param_len, &param_len);

	EXECUTE_AND_CHECK("SQLExecute", hstmt, SQLExecute, hstmt);

	// Fetch and check
	EXECUTE_AND_CHECK("SQLExecDirect", hstmt, SQLExecDirect, hstmt, ConvertToSQLCHAR("SELECT * FROM longdata_test"),
	                  SQL_NTS);
	EXECUTE_AND_CHECK("SQLFetch", hstmt, SQLFetch, hstmt);
}

static void CleanUp(SQLHANDLE &env, SQLHANDLE &dbc, HSTMT &hstmt) {
	// Free the statement handle
	EXECUTE_AND_CHECK("SQLFreeStmt (HSTMT)", hstmt, SQLFreeStmt, hstmt, SQL_CLOSE);
	EXECUTE_AND_CHECK("SQLFreeHandle (HSTMT)", hstmt, SQLFreeHandle, SQL_HANDLE_STMT, hstmt);
	DISCONNECT_FROM_DATABASE(env, dbc);
}

TEST_CASE("Test long SQLGetData with SQL_CHAR", "[odbc]") {
	SQLHANDLE env = nullptr;
	SQLHANDLE dbc = nullptr;
	HSTMT hstmt = SQL_NULL_HSTMT;
	SQLLEN len_ret = 0;
	std::string value = "abcde";
	SQLSMALLINT fetch_type = SQL_C_CHAR;

	SECTION("Large buffer") {
		GetDataReady(env, dbc, hstmt, value);

		std::vector<char> buf(64, 'x');
		SQLRETURN ret = SQLGetData(hstmt, 1, fetch_type, buf.data(), buf.size(), &len_ret);

		REQUIRE(ret == SQL_SUCCESS);
		REQUIRE(len_ret == value.length());
		REQUIRE(buf.at(value.length()) == '\0');
		REQUIRE(std::string(buf.data()) == value);

		ret = SQLGetData(hstmt, 1, fetch_type, nullptr, 0, nullptr);
		REQUIRE(ret == SQL_NO_DATA);

		CleanUp(env, dbc, hstmt);
	}

	SECTION("Exact buffer") {
		GetDataReady(env, dbc, hstmt, value);

		std::vector<char> buf(value.length() + 1, 'x');
		SQLRETURN ret = SQLGetData(hstmt, 1, fetch_type, buf.data(), buf.size(), &len_ret);

		REQUIRE(ret == SQL_SUCCESS);
		REQUIRE(len_ret == value.length());
		REQUIRE(buf.at(value.length()) == '\0');
		REQUIRE(std::string(buf.data()) == value);

		ret = SQLGetData(hstmt, 1, fetch_type, nullptr, 0, nullptr);
		REQUIRE(ret == SQL_NO_DATA);

		CleanUp(env, dbc, hstmt);
	}

	SECTION("Short buffer") {
		GetDataReady(env, dbc, hstmt, value);

		std::vector<char> buf(3, 'x');
		SQLRETURN ret = SQLGetData(hstmt, 1, fetch_type, buf.data(), buf.size(), &len_ret);

		REQUIRE(ret == SQL_SUCCESS_WITH_INFO);
		REQUIRE(len_ret == value.length());
		REQUIRE(buf.at(0) == 'a');
		REQUIRE(buf.at(1) == 'b');
		REQUIRE(buf.at(2) == '\0');

		buf = std::vector<char>(3, 'x');
		ret = SQLGetData(hstmt, 1, fetch_type, buf.data(), buf.size(), &len_ret);
		REQUIRE(ret == SQL_SUCCESS_WITH_INFO);
		REQUIRE(len_ret == value.length() - 2);
		REQUIRE(buf.at(0) == 'c');
		REQUIRE(buf.at(1) == 'd');
		REQUIRE(buf.at(2) == '\0');

		buf = std::vector<char>(3, 'x');
		ret = SQLGetData(hstmt, 1, fetch_type, buf.data(), buf.size(), &len_ret);
		REQUIRE(ret == SQL_SUCCESS);
		REQUIRE(len_ret == 1);
		REQUIRE(buf.at(0) == 'e');
		REQUIRE(buf.at(1) == '\0');
		REQUIRE(buf.at(2) == 'x');

		ret = SQLGetData(hstmt, 1, fetch_type, nullptr, 0, nullptr);
		REQUIRE(ret == SQL_NO_DATA);

		CleanUp(env, dbc, hstmt);
	}

	SECTION("No null-term buffer") {
		GetDataReady(env, dbc, hstmt, value);

		std::vector<char> buf(value.length(), 'x');
		SQLRETURN ret = SQLGetData(hstmt, 1, fetch_type, buf.data(), buf.size(), &len_ret);

		REQUIRE(ret == SQL_SUCCESS_WITH_INFO);
		REQUIRE(len_ret == value.length());
		REQUIRE(buf.at(value.length() - 1) == '\0');
		REQUIRE(std::string(buf.data()) == value.substr(0, value.length() - 1));

		buf = std::vector<char>(2, 'x');
		ret = SQLGetData(hstmt, 1, fetch_type, buf.data(), buf.size(), &len_ret);
		REQUIRE(ret == SQL_SUCCESS);
		REQUIRE(len_ret == 1);
		REQUIRE(buf.at(0) == 'e');
		REQUIRE(buf.at(1) == '\0');

		ret = SQLGetData(hstmt, 1, fetch_type, nullptr, 0, nullptr);
		REQUIRE(ret == SQL_NO_DATA);

		CleanUp(env, dbc, hstmt);
	}
}

TEST_CASE("Test long SQLGetData with SQL_WCHAR", "[odbc]") {
	SQLHANDLE env = nullptr;
	SQLHANDLE dbc = nullptr;
	HSTMT hstmt = SQL_NULL_HSTMT;
	SQLLEN len_ret = 0;
	std::string value_narrow = "abcde";
	std::vector<char> value_wide = {'a', '\0', 'b', '\0', 'c', '\0', 'd', '\0', 'e', '\0'};
	SQLSMALLINT fetch_type = SQL_C_WCHAR;

	SECTION("Large buffer") {
		GetDataReady(env, dbc, hstmt, value_narrow);

		std::vector<char> buf(64, 'x');
		SQLRETURN ret = SQLGetData(hstmt, 1, fetch_type, buf.data(), buf.size(), &len_ret);

		REQUIRE(ret == SQL_SUCCESS);
		REQUIRE(len_ret == value_wide.size());
		REQUIRE(buf.at(value_wide.size()) == '\0');
		REQUIRE(buf.at(value_wide.size() + 1) == '\0');
		REQUIRE(std::equal(value_wide.begin(), value_wide.end(), buf.begin()));

		ret = SQLGetData(hstmt, 1, fetch_type, nullptr, 0, nullptr);
		REQUIRE(ret == SQL_NO_DATA);

		CleanUp(env, dbc, hstmt);
	}

	SECTION("Exact buffer") {
		GetDataReady(env, dbc, hstmt, value_narrow);

		std::vector<char> buf(value_wide.size() + 2, 'x');
		SQLRETURN ret = SQLGetData(hstmt, 1, fetch_type, buf.data(), buf.size(), &len_ret);

		REQUIRE(ret == SQL_SUCCESS);
		REQUIRE(len_ret == value_wide.size());
		REQUIRE(buf.at(value_wide.size()) == '\0');
		REQUIRE(buf.at(value_wide.size() + 1) == '\0');
		REQUIRE(std::equal(value_wide.begin(), value_wide.end(), buf.begin()));

		ret = SQLGetData(hstmt, 1, fetch_type, nullptr, 0, nullptr);
		REQUIRE(ret == SQL_NO_DATA);

		CleanUp(env, dbc, hstmt);
	}

	SECTION("Short buffer") {
		GetDataReady(env, dbc, hstmt, value_narrow);

		std::vector<char> buf(6, 'x');
		SQLRETURN ret = SQLGetData(hstmt, 1, fetch_type, buf.data(), buf.size(), &len_ret);

		REQUIRE(ret == SQL_SUCCESS_WITH_INFO);
		REQUIRE(len_ret == value_wide.size());
		REQUIRE(buf.at(0) == 'a');
		REQUIRE(buf.at(2) == 'b');
		REQUIRE(buf.at(4) == '\0');
		REQUIRE(buf.at(5) == '\0');

		buf = std::vector<char>(6, 'x');
		ret = SQLGetData(hstmt, 1, fetch_type, buf.data(), buf.size(), &len_ret);
		REQUIRE(ret == SQL_SUCCESS_WITH_INFO);
		REQUIRE(len_ret == value_wide.size() - 4);
		REQUIRE(buf.at(0) == 'c');
		REQUIRE(buf.at(2) == 'd');
		REQUIRE(buf.at(4) == '\0');
		REQUIRE(buf.at(5) == '\0');

		buf = std::vector<char>(6, 'x');
		ret = SQLGetData(hstmt, 1, fetch_type, buf.data(), buf.size(), &len_ret);
		REQUIRE(ret == SQL_SUCCESS);
		REQUIRE(len_ret == value_wide.size() - 8);
		REQUIRE(buf.at(0) == 'e');
		REQUIRE(buf.at(2) == '\0');
		REQUIRE(buf.at(3) == '\0');

		ret = SQLGetData(hstmt, 1, fetch_type, nullptr, 0, nullptr);
		REQUIRE(ret == SQL_NO_DATA);

		CleanUp(env, dbc, hstmt);
	}

	SECTION("No null-term buffer") {
		GetDataReady(env, dbc, hstmt, value_narrow);

		std::vector<char> buf(value_wide.size(), 'x');
		SQLRETURN ret = SQLGetData(hstmt, 1, fetch_type, buf.data(), buf.size(), &len_ret);
		REQUIRE(ret == SQL_SUCCESS_WITH_INFO);
		REQUIRE(len_ret == value_wide.size());
		REQUIRE(buf.at(value_wide.size() - 2) == '\0');
		REQUIRE(buf.at(value_wide.size() - 1) == '\0');
		REQUIRE(std::equal(value_wide.begin(), value_wide.end() - 2, buf.begin()));

		buf = std::vector<char>(4, 'x');
		ret = SQLGetData(hstmt, 1, fetch_type, buf.data(), buf.size(), &len_ret);
		REQUIRE(ret == SQL_SUCCESS);
		REQUIRE(len_ret == value_wide.size() - 8);
		REQUIRE(buf.at(0) == 'e');
		REQUIRE(buf.at(2) == '\0');
		REQUIRE(buf.at(3) == '\0');

		ret = SQLGetData(hstmt, 1, fetch_type, nullptr, 0, nullptr);
		REQUIRE(ret == SQL_NO_DATA);

		CleanUp(env, dbc, hstmt);
	}
}

TEST_CASE("Test long SQLGetData with SQL_BINARY", "[odbc]") {
	SQLHANDLE env = nullptr;
	SQLHANDLE dbc = nullptr;
	HSTMT hstmt = SQL_NULL_HSTMT;
	SQLLEN len_ret = 0;
	std::string value = "abcde";
	SQLSMALLINT fetch_type = SQL_C_BINARY;

	SECTION("Large buffer") {
		GetDataReady(env, dbc, hstmt, value);

		std::vector<char> buf(64, 'x');
		SQLRETURN ret = SQLGetData(hstmt, 1, fetch_type, buf.data(), buf.size(), &len_ret);

		REQUIRE(ret == SQL_SUCCESS);
		REQUIRE(len_ret == value.length());
		REQUIRE(buf.at(value.length()) == 'x');
		REQUIRE(std::string(buf.data(), value.length()) == value);

		ret = SQLGetData(hstmt, 1, fetch_type, nullptr, 0, nullptr);
		REQUIRE(ret == SQL_NO_DATA);

		CleanUp(env, dbc, hstmt);
	}

	SECTION("Exact buffer") {
		GetDataReady(env, dbc, hstmt, value);

		std::vector<char> buf(value.length(), 'x');
		SQLRETURN ret = SQLGetData(hstmt, 1, fetch_type, buf.data(), buf.size(), &len_ret);

		REQUIRE(ret == SQL_SUCCESS);
		REQUIRE(len_ret == value.length());
		REQUIRE(std::string(buf.data(), value.length()) == value);

		ret = SQLGetData(hstmt, 1, fetch_type, nullptr, 0, nullptr);
		REQUIRE(ret == SQL_NO_DATA);

		CleanUp(env, dbc, hstmt);
	}

	SECTION("Short buffer") {
		GetDataReady(env, dbc, hstmt, value);

		std::vector<char> buf(2, 'x');
		SQLRETURN ret = SQLGetData(hstmt, 1, fetch_type, buf.data(), buf.size(), &len_ret);

		REQUIRE(ret == SQL_SUCCESS_WITH_INFO);
		REQUIRE(len_ret == value.length());
		REQUIRE(buf.at(0) == 'a');
		REQUIRE(buf.at(1) == 'b');

		buf = std::vector<char>(2, 'x');
		ret = SQLGetData(hstmt, 1, fetch_type, buf.data(), buf.size(), &len_ret);
		REQUIRE(ret == SQL_SUCCESS_WITH_INFO);
		REQUIRE(len_ret == value.length() - 2);
		REQUIRE(buf.at(0) == 'c');
		REQUIRE(buf.at(1) == 'd');

		buf = std::vector<char>(2, 'x');
		ret = SQLGetData(hstmt, 1, fetch_type, buf.data(), buf.size(), &len_ret);
		REQUIRE(ret == SQL_SUCCESS);
		REQUIRE(len_ret == 1);
		REQUIRE(buf.at(0) == 'e');
		REQUIRE(buf.at(1) == 'x');

		ret = SQLGetData(hstmt, 1, fetch_type, nullptr, 0, nullptr);
		REQUIRE(ret == SQL_NO_DATA);

		CleanUp(env, dbc, hstmt);
	}
}
