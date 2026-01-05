#include "odbc_test_common.h"

using namespace std;
using namespace odbc_test;

// Test SQLGetInfo truncation behavior per ODBC spec
// https://learn.microsoft.com/en-us/sql/odbc/reference/syntax/sqlgetinfo-function
//
// Key requirements:
// 1. StringLengthPtr should return the total bytes available (excluding null terminator)
// 2. If InfoValuePtr is NULL, StringLengthPtr should still return the total length
// 3. If buffer is too small, return SQL_SUCCESS_WITH_INFO with truncated data
TEST_CASE("Test SQLGetInfo truncation and NULL buffer behavior - ASCII", "[odbc]") {
	SQLHANDLE env;
	SQLHANDLE dbc;

	// Connect to the database
	CONNECT_TO_DATABASE(env, dbc);

	// Test with SQL_DBMS_NAME which returns "DuckDB" (6 characters)

	// Test 1: Buffer too small (4 bytes) - should truncate to "Duc" and return full length
	{
		char buf[4];
		SQLSMALLINT out_len = -1;
		SQLRETURN rc = SQLGetInfo(dbc, SQL_DBMS_NAME, buf, sizeof(buf), &out_len);

		// Should return SQL_SUCCESS_WITH_INFO because of truncation
		REQUIRE(rc == SQL_SUCCESS_WITH_INFO);

		// Buffer should contain truncated string "Duc" (3 chars + null terminator)
		REQUIRE(STR_EQUAL(buf, "Duc"));

		// out_len should be 6 (full length), not 3 (truncated length)
		REQUIRE(out_len == 6);
	}

	// Test 2: NULL buffer - should still return the required length
	{
		SQLSMALLINT out_len = -1;
		SQLRETURN rc = SQLGetInfo(dbc, SQL_DBMS_NAME, NULL, 0, &out_len);

		// Should succeed
		REQUIRE(SQL_SUCCEEDED(rc));

		// out_len should be 6, not 0
		REQUIRE(out_len == 6);
	}

	// Test 3: Exact size buffer (7 bytes) - should succeed without truncation
	{
		char buf[7];
		SQLSMALLINT out_len = -1;
		SQLRETURN rc = SQLGetInfo(dbc, SQL_DBMS_NAME, buf, sizeof(buf), &out_len);

		// Should return SQL_SUCCESS (no truncation)
		REQUIRE(rc == SQL_SUCCESS);

		// Buffer should contain full string "DuckDB"
		REQUIRE(STR_EQUAL(buf, "DuckDB"));

		// out_len should be 6
		REQUIRE(out_len == 6);
	}

	// Disconnect
	DISCONNECT_FROM_DATABASE(env, dbc);
}

// Test SQLGetInfoW truncation behavior for wide characters
TEST_CASE("Test SQLGetInfoW truncation and NULL buffer behavior - Wide char", "[odbc]") {
	SQLHANDLE env;
	SQLHANDLE dbc;

	// Connect to the database
	CONNECT_TO_DATABASE(env, dbc);

	// Test with SQL_DBMS_NAME which returns "DuckDB" (6 characters = 12 bytes in UTF-16)

	// Test 1: Buffer too small for full string
	{
		SQLWCHAR buf[4]; // 8 bytes, can hold 3 chars + null terminator
		SQLSMALLINT out_len = -1;
		SQLRETURN rc = SQLGetInfoW(dbc, SQL_DBMS_NAME, buf, sizeof(buf), &out_len);

		// Should return SQL_SUCCESS_WITH_INFO because of truncation
		REQUIRE(rc == SQL_SUCCESS_WITH_INFO);

		// Buffer should contain truncated string "Duc"
		auto buf_utf8 = ConvertToString(buf);
		REQUIRE(buf_utf8 == std::string("Duc"));

		// out_len should be 12 (full length in bytes), not 6 (truncated length)
		REQUIRE(out_len == 12);
	}

	// Test 2: NULL buffer - should still return the required length
	{
		SQLSMALLINT out_len = -1;
		SQLRETURN rc = SQLGetInfoW(dbc, SQL_DBMS_NAME, NULL, 0, &out_len);

		// Should succeed
		REQUIRE(SQL_SUCCEEDED(rc));

		// out_len should be 12 (6 chars * 2 bytes), not 0
		REQUIRE(out_len == 12);
	}

	// Test 3: Buffer size = sizeof(SQLWCHAR) - only room for null terminator
	{
		SQLWCHAR buf[1];
		SQLSMALLINT out_len = -1;
		SQLRETURN rc = SQLGetInfoW(dbc, SQL_DBMS_NAME, buf, sizeof(buf), &out_len);

		// Should return SQL_SUCCESS_WITH_INFO because of truncation
		REQUIRE(rc == SQL_SUCCESS_WITH_INFO);

		// Buffer should contain just null terminator
		REQUIRE(buf[0] == 0);

		// out_len should be 12, not 0
		REQUIRE(out_len == 12);
	}

	// Test 4: Exact size buffer (14 bytes = 7 WCHARs)
	{
		SQLWCHAR buf[7];
		SQLSMALLINT out_len = -1;
		SQLRETURN rc = SQLGetInfoW(dbc, SQL_DBMS_NAME, buf, sizeof(buf), &out_len);

		// Should return SQL_SUCCESS (no truncation)
		REQUIRE(rc == SQL_SUCCESS);

		// Buffer should contain full string "DuckDB"
		auto buf_utf8 = ConvertToString(buf);
		REQUIRE(buf_utf8 == std::string("DuckDB"));

		// out_len should be 12
		REQUIRE(out_len == 12);
	}

	// Disconnect
	DISCONNECT_FROM_DATABASE(env, dbc);
}

// Test with various info types to ensure consistency
TEST_CASE("Test SQLGetInfo truncation with various info types", "[odbc]") {
	SQLHANDLE env;
	SQLHANDLE dbc;

	// Connect to the database
	CONNECT_TO_DATABASE(env, dbc);

	// Get the actual length first
	SQLSMALLINT actual_length = -1;
	SQLRETURN rc = SQLGetInfo(dbc, SQL_DRIVER_NAME, NULL, 0, &actual_length);
	REQUIRE(SQL_SUCCEEDED(rc));
	REQUIRE(actual_length > 0);

	// Test with buffer that's half the required size
	if (actual_length >= 6) {
		SQLSMALLINT half_size = actual_length / 2 + 1; // +1 for null terminator
		vector<char> buf(half_size, 0);
		SQLSMALLINT out_len = -1;

		rc = SQLGetInfo(dbc, SQL_DRIVER_NAME, buf.data(), half_size, &out_len);

		// Should return SQL_SUCCESS_WITH_INFO because of truncation
		REQUIRE(rc == SQL_SUCCESS_WITH_INFO);

		// out_len should be the full length, not the truncated length
		REQUIRE(out_len == actual_length);

		// Buffer should be null-terminated
		REQUIRE(buf[half_size - 1] == '\0');

		// Actual string in buffer should be shorter than full length
		REQUIRE(strlen(buf.data()) < actual_length);
	}

	// Disconnect
	DISCONNECT_FROM_DATABASE(env, dbc);
}
