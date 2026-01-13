#include "odbc_test_common.h"

#include "widechar.hpp"

using namespace odbc_test;

static const std::vector<SQLCHAR> hello_bg_utf8 = {0xd0, 0x97, 0xd0, 0xb4, 0xd1, 0x80, 0xd0, 0xb0, 0xd0,
                                                   0xb2, 0xd0, 0xb5, 0xd0, 0xb9, 0xd1, 0x82, 0xd0, 0xb5};
static const std::string hello_bg_utf8_str(reinterpret_cast<const char *>(hello_bg_utf8.data()), hello_bg_utf8.size());

static void CheckDiagCode(SQLHANDLE h, SQLSMALLINT htype) {
	std::string state;
	std::string message;
	ACCESS_DIAGNOSTIC(state, message, h, htype);
	REQUIRE(state == "01004");
}

TEST_CASE("Test truncation: NULL buffer behavior", "[odbc]") {
	SQLHANDLE env;
	SQLHANDLE dbc;
	HSTMT hstmt;

	CONNECT_TO_DATABASE(env, dbc);
	EXECUTE_AND_CHECK("SQLAllocHandle (HSTMT)", hstmt, SQLAllocHandle, SQL_HANDLE_STMT, dbc, &hstmt);
	EXECUTE_AND_CHECK("SQLExecuteDirectW", hstmt, SQLPrepareW, hstmt,
	                  ConvertToSQLWCHARNTS("SELECT 42::INT AS col1, 43::INT AS \"" + hello_bg_utf8_str + "\"").data(),
	                  SQL_NTS);

	SECTION("SQLColAttribute") {
		SQLSMALLINT len = -1;
		EXECUTE_AND_CHECK("SQLColAttribute", hstmt, SQLColAttribute, hstmt, 1, SQL_COLUMN_NAME, nullptr, 0, &len,
		                  nullptr);
		REQUIRE(len == 4); // "col1"
	}
	SECTION("SQLColAttributeW") {
		SQLSMALLINT len = -1;
		EXECUTE_AND_CHECK("SQLColAttributeW", hstmt, SQLColAttributeW, hstmt, 1, SQL_COLUMN_NAME, nullptr, 0, &len,
		                  nullptr);
		REQUIRE(len == 8); // "col1" bytes
		EXECUTE_AND_CHECK("SQLColAttributeW", hstmt, SQLColAttributeW, hstmt, 2, SQL_COLUMN_NAME, nullptr, 0, &len,
		                  nullptr);
		REQUIRE(len == 18); // "Здравейте" bytes
	}

	SECTION("SQLDescribeCol") {
		SQLSMALLINT len = -1;
		EXECUTE_AND_CHECK("SQLDescribeCol", hstmt, SQLDescribeCol, hstmt, 1, nullptr, 0, &len, nullptr, nullptr,
		                  nullptr, nullptr);
		REQUIRE(len == 4); // "col1"
	}
	SECTION("SQLDescribeColW") {
		SQLSMALLINT len = -1;
		EXECUTE_AND_CHECK("SQLDescribeColW", hstmt, SQLDescribeColW, hstmt, 1, nullptr, 0, &len, nullptr, nullptr,
		                  nullptr, nullptr);
		REQUIRE(len == 4); // "col1" chars
		EXECUTE_AND_CHECK("SQLDescribeColW", hstmt, SQLDescribeColW, hstmt, 2, nullptr, 0, &len, nullptr, nullptr,
		                  nullptr, nullptr);
		REQUIRE(len == 9); // "Здравейте" chars
	}

	SECTION("SQLGetConnectAttr") {
		SQLINTEGER len = -1;
		EXECUTE_AND_CHECK("SQLGetConnectAttr", hstmt, SQLGetConnectAttr, dbc, SQL_ATTR_CURRENT_CATALOG, nullptr, 0,
		                  &len);
		REQUIRE(len == 8); // ":memory:"
	}
	SECTION("SQLGetConnectAttrW") {
		SQLINTEGER len = -1;
		EXECUTE_AND_CHECK("SQLGetConnectAttrW", hstmt, SQLGetConnectAttrW, dbc, SQL_ATTR_CURRENT_CATALOG, nullptr, 0,
		                  &len);
		REQUIRE(len == 16); // ":memory:" bytes
	}

	SQLHDESC hird;
	EXECUTE_AND_CHECK("SQLGetStmtAttr", hstmt, SQLGetStmtAttr, hstmt, SQL_ATTR_IMP_ROW_DESC, &hird, 0, nullptr);
	SECTION("SQLGetDescField") {
		SQLINTEGER len = -1;
		SQLRETURN ret = SQLGetDescField(hird, 1, SQL_DESC_BASE_COLUMN_NAME, nullptr, 0, &len);
		REQUIRE(SQL_SUCCEEDED(ret));
		REQUIRE(len == 4); // "col1"
	}
	SECTION("SQLGetDescFieldW") {
		SQLINTEGER len = -1;
		SQLRETURN ret = SQLGetDescFieldW(hird, 1, SQL_DESC_BASE_COLUMN_NAME, nullptr, 0, &len);
		REQUIRE(SQL_SUCCEEDED(ret));
		REQUIRE(len == 8); // "col1" bytes
		ret = SQLGetDescFieldW(hird, 2, SQL_DESC_BASE_COLUMN_NAME, nullptr, 0, &len);
		REQUIRE(SQL_SUCCEEDED(ret));
		REQUIRE(len == 18); // "Здравейте" bytes
	}

	SECTION("SQLGetDiagField") {
		SQLRETURN ret_err = SQLGetStmtAttr(hstmt, SQL_ATTR_CURSOR_SENSITIVITY, nullptr, 0, nullptr);
		REQUIRE(ret_err == SQL_ERROR);
		SQLSMALLINT len = -1;
		SQLRETURN ret = SQLGetDiagField(SQL_HANDLE_STMT, hstmt, 1, SQL_DIAG_MESSAGE_TEXT, nullptr, 0, &len);
		REQUIRE(SQL_SUCCEEDED(ret));
		REQUIRE(len == 65);
	}
	SECTION("SQLGetDiagFieldW") {
		SQLRETURN ret_err = SQLGetStmtAttr(hstmt, SQL_ATTR_CURSOR_SENSITIVITY, nullptr, 0, nullptr);
		REQUIRE(ret_err == SQL_ERROR);
		SQLSMALLINT len = -1;
		SQLRETURN ret = SQLGetDiagFieldW(SQL_HANDLE_STMT, hstmt, 1, SQL_DIAG_MESSAGE_TEXT, nullptr, 0, &len);
		REQUIRE(SQL_SUCCEEDED(ret));
		REQUIRE(len == 130);
	}

	SECTION("SQLGetInfo") {
		SQLSMALLINT len = -1;
		SQLRETURN ret = SQLGetInfo(dbc, SQL_DBMS_NAME, nullptr, 0, &len);
		REQUIRE(ret == SQL_SUCCESS);
		REQUIRE(len == 6); // "DuckDB"
	}
	SECTION("SQLGetInfoW") {
		SQLSMALLINT len = -1;
		SQLRETURN ret = SQLGetInfoW(dbc, SQL_DBMS_NAME, nullptr, 0, &len);
		REQUIRE(ret == SQL_SUCCESS);
		REQUIRE(len == 12); // "DuckDB" bytes
	}

	EXECUTE_AND_CHECK("SQLFreeStmt (HSTMT)", hstmt, SQLFreeStmt, hstmt, SQL_CLOSE);
	EXECUTE_AND_CHECK("SQLFreeHandle (HSTMT)", hstmt, SQLFreeHandle, SQL_HANDLE_STMT, hstmt);
	DISCONNECT_FROM_DATABASE(env, dbc);
}

TEST_CASE("Test truncation: buffer too small behavior", "[odbc]") {
	SQLHANDLE env;
	SQLHANDLE dbc;
	HSTMT hstmt;

	CONNECT_TO_DATABASE(env, dbc);
	EXECUTE_AND_CHECK("SQLAllocHandle (HSTMT)", hstmt, SQLAllocHandle, SQL_HANDLE_STMT, dbc, &hstmt);
	EXECUTE_AND_CHECK("SQLExecuteDirectW", hstmt, SQLPrepareW, hstmt,
	                  ConvertToSQLWCHARNTS("SELECT 42::INT AS col1, 43::INT AS \"" + hello_bg_utf8_str + "\"").data(),
	                  SQL_NTS);

	SECTION("SQLColAttribute") {
		std::vector<SQLCHAR> buf;
		buf.resize(3);
		SQLSMALLINT len = -1;
		SQLRETURN ret = SQLColAttribute(hstmt, 1, SQL_COLUMN_NAME, reinterpret_cast<SQLPOINTER>(buf.data()),
		                                static_cast<SQLSMALLINT>(buf.size()), &len, nullptr);
		REQUIRE(ret == SQL_SUCCESS_WITH_INFO);
		CheckDiagCode(hstmt, SQL_HANDLE_STMT);
		REQUIRE(len == 4); // "col1"
		REQUIRE(buf[2] == '\0');
		REQUIRE(STR_EQUAL(reinterpret_cast<char *>(buf.data()), "co"));
	}
	SECTION("SQLColAttributeW") {
		std::vector<SQLWCHAR> buf;
		buf.resize(3);
		SQLSMALLINT len = -1;
		SQLRETURN ret = SQLColAttributeW(hstmt, 2, SQL_COLUMN_NAME, reinterpret_cast<SQLPOINTER>(buf.data()),
		                                 static_cast<SQLSMALLINT>(buf.size() * sizeof(SQLWCHAR)), &len, nullptr);
		REQUIRE(ret == SQL_SUCCESS_WITH_INFO);
		CheckDiagCode(hstmt, SQL_HANDLE_STMT);
		REQUIRE(len == 18); // "Здравейте" bytes
		REQUIRE(buf[2] == 0);
		auto utf8_buf = duckdb::widechar::utf16_to_utf8_lenient(buf.data(), buf.size() - 1);
		REQUIRE(utf8_buf.size() == 4);
		REQUIRE(utf8_buf.at(0) == 0xd0);
		REQUIRE(utf8_buf.at(1) == 0x97);
		REQUIRE(utf8_buf.at(2) == 0xd0);
		REQUIRE(utf8_buf.at(3) == 0xb4);
	}

	SECTION("SQLDescribeCol") {
		std::vector<SQLCHAR> buf;
		buf.resize(3);
		SQLSMALLINT len = -1;
		SQLRETURN ret = SQLDescribeCol(hstmt, 1, buf.data(), static_cast<SQLSMALLINT>(buf.size()), &len, nullptr,
		                               nullptr, nullptr, nullptr);
		REQUIRE(ret == SQL_SUCCESS_WITH_INFO);
		CheckDiagCode(hstmt, SQL_HANDLE_STMT);
		REQUIRE(len == 4); // "col1"
		REQUIRE(buf[2] == '\0');
		REQUIRE(STR_EQUAL(reinterpret_cast<char *>(buf.data()), "co"));
	}
	SECTION("SQLDescribeColW") {
		std::vector<SQLWCHAR> buf;
		buf.resize(3);
		SQLSMALLINT len = -1;
		SQLRETURN ret = SQLDescribeColW(hstmt, 2, buf.data(), static_cast<SQLSMALLINT>(buf.size()), &len, nullptr,
		                                nullptr, nullptr, nullptr);
		REQUIRE(ret == SQL_SUCCESS_WITH_INFO);
		CheckDiagCode(hstmt, SQL_HANDLE_STMT);
		REQUIRE(len == 9); // "Здравейте" chars
		REQUIRE(buf[2] == 0);
		auto utf8_buf = duckdb::widechar::utf16_to_utf8_lenient(buf.data(), buf.size() - 1);
		REQUIRE(utf8_buf.size() == 4);
		REQUIRE(utf8_buf.at(0) == 0xd0);
		REQUIRE(utf8_buf.at(1) == 0x97);
		REQUIRE(utf8_buf.at(2) == 0xd0);
		REQUIRE(utf8_buf.at(3) == 0xb4);
	}

	SECTION("SQLGetConnectAttr") {
		std::vector<SQLCHAR> buf;
		buf.resize(3);
		SQLINTEGER len = -1;
		SQLRETURN ret = SQLGetConnectAttr(dbc, SQL_ATTR_CURRENT_CATALOG, reinterpret_cast<SQLPOINTER>(buf.data()),
		                                  static_cast<SQLSMALLINT>(buf.size()), &len);
		REQUIRE(ret == SQL_SUCCESS_WITH_INFO);
		CheckDiagCode(dbc, SQL_HANDLE_DBC);
		REQUIRE(len == 8); // ":memory:"
		REQUIRE(buf[2] == '\0');
		REQUIRE(STR_EQUAL(reinterpret_cast<char *>(buf.data()), ":m"));
	}
	SECTION("SQLGetConnectAttrW") {
		std::vector<SQLWCHAR> buf;
		buf.resize(3);
		SQLINTEGER len = -1;
		SQLRETURN ret = SQLGetConnectAttrW(dbc, SQL_ATTR_CURRENT_CATALOG, reinterpret_cast<SQLPOINTER>(buf.data()),
		                                   static_cast<SQLSMALLINT>(buf.size() * sizeof(SQLWCHAR)), &len);
		REQUIRE(ret == SQL_SUCCESS_WITH_INFO);
		CheckDiagCode(dbc, SQL_HANDLE_DBC);
		REQUIRE(len == 16); // ":memory:" chars
		REQUIRE(buf[2] == 0);
		auto utf8_buf = duckdb::widechar::utf16_to_utf8_lenient(buf.data(), buf.size() - 1);
		REQUIRE(utf8_buf.size() == 2);
		REQUIRE(utf8_buf.at(0) == ':');
		REQUIRE(utf8_buf.at(1) == 'm');
	}

	SQLHDESC hird;
	EXECUTE_AND_CHECK("SQLGetStmtAttr", hstmt, SQLGetStmtAttr, hstmt, SQL_ATTR_IMP_ROW_DESC, &hird, 0, nullptr);
	SECTION("SQLGetDescField") {
		std::vector<SQLCHAR> buf;
		buf.resize(3);
		SQLINTEGER len = -1;
		SQLRETURN ret = SQLGetDescField(hird, 1, SQL_DESC_BASE_COLUMN_NAME, reinterpret_cast<SQLPOINTER>(buf.data()),
		                                static_cast<SQLINTEGER>(buf.size()), &len);
		REQUIRE(ret == SQL_SUCCESS_WITH_INFO);
		CheckDiagCode(hird, SQL_HANDLE_DESC);
		REQUIRE(len == 4); // "col1"
		REQUIRE(buf[2] == '\0');
		REQUIRE(STR_EQUAL(reinterpret_cast<char *>(buf.data()), "co"));
	}
	SECTION("SQLGetDescFieldW") {
		std::vector<SQLWCHAR> buf;
		buf.resize(3);
		SQLINTEGER len = -1;
		SQLRETURN ret = SQLGetDescFieldW(hird, 2, SQL_DESC_BASE_COLUMN_NAME, reinterpret_cast<SQLPOINTER>(buf.data()),
		                                 static_cast<SQLINTEGER>(buf.size() * sizeof(SQLWCHAR)), &len);
		REQUIRE(ret == SQL_SUCCESS_WITH_INFO);
		CheckDiagCode(hird, SQL_HANDLE_DESC);
		REQUIRE(len == 18); // "Здравейте" bytes
		REQUIRE(buf[2] == 0);
		auto utf8_buf = duckdb::widechar::utf16_to_utf8_lenient(buf.data(), buf.size() - 1);
		REQUIRE(utf8_buf.size() == 4);
		REQUIRE(utf8_buf.at(0) == 0xd0);
		REQUIRE(utf8_buf.at(1) == 0x97);
		REQUIRE(utf8_buf.at(2) == 0xd0);
		REQUIRE(utf8_buf.at(3) == 0xb4);
	}

	SECTION("SQLGetDiagField") {
		SQLRETURN ret_err = SQLGetStmtAttr(hstmt, SQL_ATTR_CURSOR_SENSITIVITY, nullptr, 0, nullptr);
		REQUIRE(ret_err == SQL_ERROR);
		std::vector<SQLCHAR> buf;
		buf.resize(3);
		SQLSMALLINT len = -1;
		SQLRETURN ret =
		    SQLGetDiagField(SQL_HANDLE_STMT, hstmt, 1, SQL_DIAG_MESSAGE_TEXT, reinterpret_cast<SQLPOINTER>(buf.data()),
		                    static_cast<SQLSMALLINT>(buf.size()), &len);
		REQUIRE(SQL_SUCCEEDED(ret));
		REQUIRE(len == 65); // "ODBC ..."
		REQUIRE(buf[2] == '\0');
		REQUIRE(STR_EQUAL(reinterpret_cast<char *>(buf.data()), "OD"));
	}
	SECTION("SQLGetDiagFieldW") {
		SQLRETURN ret_err = SQLGetStmtAttr(hstmt, SQL_ATTR_CURSOR_SENSITIVITY, nullptr, 0, nullptr);
		REQUIRE(ret_err == SQL_ERROR);
		std::vector<SQLWCHAR> buf;
		buf.resize(3);
		SQLSMALLINT len = -1;
		SQLRETURN ret =
		    SQLGetDiagFieldW(SQL_HANDLE_STMT, hstmt, 1, SQL_DIAG_MESSAGE_TEXT, reinterpret_cast<SQLPOINTER>(buf.data()),
		                     static_cast<SQLSMALLINT>(buf.size() * sizeof(SQLWCHAR)), &len);
		REQUIRE(SQL_SUCCEEDED(ret));
		REQUIRE(len == 130);
		REQUIRE(buf[2] == 0);
		auto utf8_buf = duckdb::widechar::utf16_to_utf8_lenient(buf.data(), buf.size() - 1);
		REQUIRE(utf8_buf.size() == 2);
		REQUIRE(utf8_buf.at(0) == 'O');
		REQUIRE(utf8_buf.at(1) == 'D');
	}

	SECTION("SQLGetInfo") {
		std::vector<SQLCHAR> buf;
		buf.resize(3);
		SQLSMALLINT len = -1;
		SQLRETURN ret = SQLGetInfo(dbc, SQL_DBMS_NAME, reinterpret_cast<SQLPOINTER>(buf.data()),
		                           static_cast<SQLSMALLINT>(buf.size()), &len);
		REQUIRE(ret == SQL_SUCCESS_WITH_INFO);
		CheckDiagCode(dbc, SQL_HANDLE_DBC);
		REQUIRE(len == 6); // "DuckDB"
		REQUIRE(buf[2] == '\0');
		REQUIRE(STR_EQUAL(reinterpret_cast<char *>(buf.data()), "Du"));
	}
	SECTION("SQLGetInfoW") {
		std::vector<SQLWCHAR> buf;
		buf.resize(3);
		SQLSMALLINT len = -1;
		SQLRETURN ret = SQLGetInfoW(dbc, SQL_DBMS_NAME, reinterpret_cast<SQLPOINTER>(buf.data()),
		                            static_cast<SQLSMALLINT>(buf.size() * sizeof(SQLWCHAR)), &len);
		REQUIRE(ret == SQL_SUCCESS_WITH_INFO);
		CheckDiagCode(dbc, SQL_HANDLE_DBC);
		REQUIRE(len == 12); // "DuckDB" bytes
		REQUIRE(buf[2] == 0);
		auto utf8_buf = duckdb::widechar::utf16_to_utf8_lenient(buf.data(), buf.size() - 1);
		REQUIRE(utf8_buf.size() == 2);
		REQUIRE(utf8_buf.at(0) == 'D');
		REQUIRE(utf8_buf.at(1) == 'u');
	}

	EXECUTE_AND_CHECK("SQLFreeStmt (HSTMT)", hstmt, SQLFreeStmt, hstmt, SQL_CLOSE);
	EXECUTE_AND_CHECK("SQLFreeHandle (HSTMT)", hstmt, SQLFreeHandle, SQL_HANDLE_STMT, hstmt);
	DISCONNECT_FROM_DATABASE(env, dbc);
}

TEST_CASE("Test truncation: buffer exact size behavior", "[odbc]") {
	SQLHANDLE env;
	SQLHANDLE dbc;
	HSTMT hstmt;

	CONNECT_TO_DATABASE(env, dbc);
	EXECUTE_AND_CHECK("SQLAllocHandle (HSTMT)", hstmt, SQLAllocHandle, SQL_HANDLE_STMT, dbc, &hstmt);
	EXECUTE_AND_CHECK("SQLExecuteDirectW", hstmt, SQLPrepareW, hstmt,
	                  ConvertToSQLWCHARNTS("SELECT 42::INT AS col1, 43::INT AS \"" + hello_bg_utf8_str + "\"").data(),
	                  SQL_NTS);

	SECTION("SQLColAttribute") {
		std::vector<SQLCHAR> buf;
		buf.resize(5);
		SQLSMALLINT len = -1;
		SQLRETURN ret = SQLColAttribute(hstmt, 1, SQL_COLUMN_NAME, reinterpret_cast<SQLPOINTER>(buf.data()),
		                                static_cast<SQLSMALLINT>(buf.size()), &len, nullptr);
		REQUIRE(ret == SQL_SUCCESS);
		REQUIRE(len == 4); // "col1"
		REQUIRE(buf[4] == '\0');
	}
	SECTION("SQLColAttributeW") {
		std::vector<SQLWCHAR> buf;
		buf.resize(10);
		SQLSMALLINT len = -1;
		SQLRETURN ret = SQLColAttributeW(hstmt, 2, SQL_COLUMN_NAME, reinterpret_cast<SQLPOINTER>(buf.data()),
		                                 static_cast<SQLSMALLINT>(buf.size() * sizeof(SQLWCHAR)), &len, nullptr);
		REQUIRE(ret == SQL_SUCCESS);
		REQUIRE(len == 18); // "Здравейте" bytes
		REQUIRE(buf[9] == 0);
	}

	SECTION("SQLDescribeCol") {
		std::vector<SQLCHAR> buf;
		buf.resize(5);
		SQLSMALLINT len = -1;
		SQLRETURN ret = SQLDescribeCol(hstmt, 1, buf.data(), static_cast<SQLSMALLINT>(buf.size()), &len, nullptr,
		                               nullptr, nullptr, nullptr);
		REQUIRE(ret == SQL_SUCCESS);
		REQUIRE(len == 4); // "col1"
		REQUIRE(buf[4] == '\0');
	}
	SECTION("SQLDescribeColW") {
		std::vector<SQLWCHAR> buf;
		buf.resize(10);
		SQLSMALLINT len = -1;
		SQLRETURN ret = SQLDescribeColW(hstmt, 2, buf.data(), static_cast<SQLSMALLINT>(buf.size()), &len, nullptr,
		                                nullptr, nullptr, nullptr);
		REQUIRE(ret == SQL_SUCCESS);
		REQUIRE(len == 9); // "Здравейте" chars
		REQUIRE(buf[9] == 0);
	}

	SECTION("SQLGetConnectAttr") {
		std::vector<SQLCHAR> buf;
		buf.resize(9);
		SQLINTEGER len = -1;
		SQLRETURN ret = SQLGetConnectAttr(dbc, SQL_ATTR_CURRENT_CATALOG, reinterpret_cast<SQLPOINTER>(buf.data()),
		                                  static_cast<SQLSMALLINT>(buf.size()), &len);
		REQUIRE(ret == SQL_SUCCESS);
		REQUIRE(len == 8); // ":memory:"
		REQUIRE(buf[8] == '\0');
	}
	SECTION("SQLGetConnectAttrW") {
		std::vector<SQLWCHAR> buf;
		buf.resize(9);
		SQLINTEGER len = -1;
		SQLRETURN ret = SQLGetConnectAttrW(dbc, SQL_ATTR_CURRENT_CATALOG, reinterpret_cast<SQLPOINTER>(buf.data()),
		                                   static_cast<SQLSMALLINT>(buf.size() * sizeof(SQLWCHAR)), &len);
		REQUIRE(ret == SQL_SUCCESS);
		REQUIRE(len == 16); // ":memory:" chars
		REQUIRE(buf[8] == 0);
	}

	SQLHDESC hird;
	EXECUTE_AND_CHECK("SQLGetStmtAttr", hstmt, SQLGetStmtAttr, hstmt, SQL_ATTR_IMP_ROW_DESC, &hird, 0, nullptr);
	SECTION("SQLGetDescField") {
		std::vector<SQLCHAR> buf;
		buf.resize(5);
		SQLINTEGER len = -1;
		SQLRETURN ret = SQLGetDescField(hird, 1, SQL_DESC_BASE_COLUMN_NAME, reinterpret_cast<SQLPOINTER>(buf.data()),
		                                static_cast<SQLINTEGER>(buf.size()), &len);
		REQUIRE(ret == SQL_SUCCESS);
		REQUIRE(len == 4); // "col1"
		REQUIRE(buf[4] == '\0');
	}
	SECTION("SQLGetDescFieldW") {
		std::vector<SQLWCHAR> buf;
		buf.resize(10);
		SQLINTEGER len = -1;
		SQLRETURN ret = SQLGetDescFieldW(hird, 2, SQL_DESC_BASE_COLUMN_NAME, reinterpret_cast<SQLPOINTER>(buf.data()),
		                                 static_cast<SQLINTEGER>(buf.size() * sizeof(SQLWCHAR)), &len);
		REQUIRE(ret == SQL_SUCCESS);
		REQUIRE(len == 18); // "Здравейте" bytes
		REQUIRE(buf[9] == 0);
	}

	SECTION("SQLGetDiagField") {
		SQLRETURN ret_err = SQLGetStmtAttr(hstmt, SQL_ATTR_CURSOR_SENSITIVITY, nullptr, 0, nullptr);
		REQUIRE(ret_err == SQL_ERROR);
		std::vector<SQLCHAR> buf;
		buf.resize(66);
		SQLSMALLINT len = -1;
		SQLRETURN ret =
		    SQLGetDiagField(SQL_HANDLE_STMT, hstmt, 1, SQL_DIAG_MESSAGE_TEXT, reinterpret_cast<SQLPOINTER>(buf.data()),
		                    static_cast<SQLSMALLINT>(buf.size()), &len);
		REQUIRE(ret == SQL_SUCCESS);
		REQUIRE(len == 65); // "ODBC ..."
		REQUIRE(buf[65] == '\0');
	}
	SECTION("SQLGetDiagFieldW") {
		SQLRETURN ret_err = SQLGetStmtAttr(hstmt, SQL_ATTR_CURSOR_SENSITIVITY, nullptr, 0, nullptr);
		REQUIRE(ret_err == SQL_ERROR);
		std::vector<SQLWCHAR> buf;
		buf.resize(66);
		SQLSMALLINT len = -1;
		SQLRETURN ret =
		    SQLGetDiagFieldW(SQL_HANDLE_STMT, hstmt, 1, SQL_DIAG_MESSAGE_TEXT, reinterpret_cast<SQLPOINTER>(buf.data()),
		                     static_cast<SQLSMALLINT>(buf.size() * sizeof(SQLWCHAR)), &len);
		REQUIRE(ret == SQL_SUCCESS);
		REQUIRE(len == 130);
		REQUIRE(buf[65] == 0);
	}

	SECTION("SQLGetInfo") {
		std::vector<SQLCHAR> buf;
		buf.resize(7);
		SQLSMALLINT len = -1;
		SQLRETURN ret = SQLGetInfo(dbc, SQL_DBMS_NAME, reinterpret_cast<SQLPOINTER>(buf.data()),
		                           static_cast<SQLSMALLINT>(buf.size()), &len);
		REQUIRE(ret == SQL_SUCCESS);
		REQUIRE(len == 6); // "DuckDB"
		REQUIRE(buf[6] == '\0');
	}
	SECTION("SQLGetInfoW") {
		std::vector<SQLWCHAR> buf;
		buf.resize(7);
		SQLSMALLINT len = -1;
		SQLRETURN ret = SQLGetInfoW(dbc, SQL_DBMS_NAME, reinterpret_cast<SQLPOINTER>(buf.data()),
		                            static_cast<SQLSMALLINT>(buf.size() * sizeof(SQLWCHAR)), &len);
		REQUIRE(ret == SQL_SUCCESS);
		REQUIRE(len == 12); // "DuckDB" bytes
		REQUIRE(buf[6] == 0);
	}

	EXECUTE_AND_CHECK("SQLFreeStmt (HSTMT)", hstmt, SQLFreeStmt, hstmt, SQL_CLOSE);
	EXECUTE_AND_CHECK("SQLFreeHandle (HSTMT)", hstmt, SQLFreeHandle, SQL_HANDLE_STMT, hstmt);
	DISCONNECT_FROM_DATABASE(env, dbc);
}
