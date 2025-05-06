#include "utils.h"
#include "widechar.hpp"

using namespace odbc_col_attribute_test;

TEST_CASE("Test General SQLColAttribute (descriptor information for a column)", "[odbc]") {
	SQLRETURN ret;
	SQLHANDLE env;
	SQLHANDLE dbc;

	HSTMT hstmt = SQL_NULL_HSTMT;

	// Connect to the database using SQLConnect
	CONNECT_TO_DATABASE(env, dbc);

	// Allocate a statement handle
	EXECUTE_AND_CHECK("SQLAllocHandle (HSTMT)", hstmt, SQLAllocHandle, SQL_HANDLE_STMT, dbc, &hstmt);

	// Get column attributes of a simple query
	EXECUTE_AND_CHECK("SQLExectDirect", hstmt, SQLExecDirect, hstmt,
	                  ConvertToSQLCHAR("SELECT "
	                                   "'1'::int AS intcol, "
	                                   "'foobar'::text AS textcol, "
	                                   "'varchar string'::varchar as varcharcol, "
	                                   "''::varchar as empty_varchar_col, "
	                                   "'varchar-5-col'::varchar(5) as varchar5col, "
	                                   "gen_random_uuid() as uuidcol, "
	                                   "'5 days'::interval day to second"),
	                  SQL_NTS);

	// Get the number of columns
	SQLSMALLINT num_cols;
	EXECUTE_AND_CHECK("SQLNumResultCols", hstmt, SQLNumResultCols, hstmt, &num_cols);
	REQUIRE(num_cols == 7);

	// Loop through the columns
	for (int i = 1; i <= num_cols; i++) {
		char buffer[64];

		// Get the column label
		EXECUTE_AND_CHECK("SQLColAttribute", hstmt, SQLColAttribute, hstmt, i, SQL_DESC_LABEL, buffer, sizeof(buffer),
		                  nullptr, nullptr);

		SQLSMALLINT col_label_size;
		EXECUTE_AND_CHECK("SQLColAttribute", hstmt, SQLColAttribute, hstmt, i, SQL_DESC_LABEL, nullptr, 0,
		                  &col_label_size, nullptr);

		// Get the column name and base column name
		char col_name[64];
		char base_col_name[64];
		EXECUTE_AND_CHECK("SQLColAttribute", hstmt, SQLColAttribute, hstmt, i, SQL_DESC_NAME, col_name,
		                  sizeof(col_name), nullptr, nullptr);
		SQLSMALLINT col_name_size;
		EXECUTE_AND_CHECK("SQLColAttribute", hstmt, SQLColAttribute, hstmt, i, SQL_DESC_NAME, nullptr, 0,
		                  &col_name_size, nullptr);

		EXECUTE_AND_CHECK("SQLColAttribute", hstmt, SQLColAttribute, hstmt, i, SQL_DESC_BASE_COLUMN_NAME, base_col_name,
		                  sizeof(base_col_name), nullptr, nullptr);
		SQLSMALLINT base_col_name_size;
		EXECUTE_AND_CHECK("SQLColAttribute", hstmt, SQLColAttribute, hstmt, i, SQL_DESC_BASE_COLUMN_NAME, nullptr, 0,
		                  &base_col_name_size, nullptr);

		std::vector<SQLWCHAR> col_name_utf16;
		col_name_utf16.resize(64);
		SQLSMALLINT col_name_utf16_len_bytes = 0;
		EXECUTE_AND_CHECK("SQLColAttributeW", hstmt, SQLColAttributeW, hstmt, i, SQL_DESC_NAME, col_name_utf16.data(),
		                  col_name_utf16.size() * sizeof(SQLWCHAR), &col_name_utf16_len_bytes, nullptr);
		auto utf16_len_calculated = duckdb::widechar::utf16_length(col_name_utf16.data());
		REQUIRE(col_name_utf16_len_bytes == utf16_len_calculated * sizeof(SQLWCHAR));
		std::string col_name_utf8 = ConvertToString(col_name_utf16.data());
		REQUIRE(STR_EQUAL(col_name_utf8.c_str(), col_name));

		switch (i) {
		case 1:
			REQUIRE(STR_EQUAL(buffer, "intcol"));
			REQUIRE(STR_EQUAL(col_name, "intcol"));
			REQUIRE(STR_EQUAL(base_col_name, "intcol"));
			REQUIRE(col_label_size == 6);
			REQUIRE(col_name_size == 6);
			REQUIRE(base_col_name_size == 6);
			break;
		case 2:
			REQUIRE(STR_EQUAL(buffer, "textcol"));
			REQUIRE(STR_EQUAL(col_name, "textcol"));
			REQUIRE(STR_EQUAL(base_col_name, "textcol"));
			REQUIRE(col_label_size == 7);
			REQUIRE(col_name_size == 7);
			REQUIRE(base_col_name_size == 7);
			break;
		case 3:
			REQUIRE(STR_EQUAL(buffer, "varcharcol"));
			REQUIRE(STR_EQUAL(col_name, "varcharcol"));
			REQUIRE(STR_EQUAL(base_col_name, "varcharcol"));
			REQUIRE(col_label_size == 10);
			REQUIRE(col_name_size == 10);
			REQUIRE(base_col_name_size == 10);
			break;
		case 4:
			REQUIRE(STR_EQUAL(buffer, "empty_varchar_col"));
			REQUIRE(STR_EQUAL(col_name, "empty_varchar_col"));
			REQUIRE(STR_EQUAL(base_col_name, "empty_varchar_col"));
			REQUIRE(col_label_size == 17);
			REQUIRE(col_name_size == 17);
			REQUIRE(base_col_name_size == 17);
			break;
		case 5:
			REQUIRE(STR_EQUAL(buffer, "varchar5col"));
			REQUIRE(STR_EQUAL(col_name, "varchar5col"));
			REQUIRE(STR_EQUAL(base_col_name, "varchar5col"));
			REQUIRE(col_label_size == 11);
			REQUIRE(col_name_size == 11);
			REQUIRE(base_col_name_size == 11);
			break;
		case 6:
			REQUIRE(STR_EQUAL(buffer, "uuidcol"));
			REQUIRE(STR_EQUAL(col_name, "uuidcol"));
			REQUIRE(STR_EQUAL(base_col_name, "uuidcol"));
			REQUIRE(col_label_size == 7);
			REQUIRE(col_name_size == 7);
			REQUIRE(base_col_name_size == 7);
			break;
		case 7:
			REQUIRE(STR_EQUAL(buffer, "CAST('5 days' AS INTERVAL)"));
			REQUIRE(STR_EQUAL(col_name, "CAST('5 days' AS INTERVAL)"));
			REQUIRE(STR_EQUAL(base_col_name, "CAST('5 days' AS INTERVAL)"));
			REQUIRE(col_label_size == 26);
			REQUIRE(col_name_size == 26);
			REQUIRE(base_col_name_size == 26);
			break;
		}

		// Get the column octet length
		CheckInteger(hstmt, 0, SQL_DESC_OCTET_LENGTH);

		// Get the column type name
		EXECUTE_AND_CHECK("SQLColAttribute", hstmt, SQLColAttribute, hstmt, i, SQL_DESC_TYPE_NAME, buffer,
		                  sizeof(buffer), nullptr, nullptr);
		switch (i) {
		case 1:
			REQUIRE(STR_EQUAL(buffer, "INT32"));
			break;
		case 2:
		case 3:
		case 4:
		case 5:
		case 6:
			REQUIRE(STR_EQUAL(buffer, "VARCHAR"));
			break;
		case 7:
			REQUIRE(STR_EQUAL(buffer, "INTERVAL"));
			break;
		}
	}

	// SQLColAttribute should fail if the column number is out of bounds
	ret = SQLColAttribute(hstmt, 8, SQL_DESC_TYPE_NAME, nullptr, 0, nullptr, nullptr);
	REQUIRE(ret == SQL_ERROR);

	// Create table and retrieve base table name using SQLColAttribute, should fail because the statement is not a
	// SELECT
	EXECUTE_AND_CHECK("SQLExecDirect", hstmt, SQLExecDirect, hstmt,
	                  ConvertToSQLCHAR("CREATE TABLE test (a INTEGER, b INTEGER)"), SQL_NTS);
	ExpectError(hstmt, SQL_DESC_BASE_TABLE_NAME);

	// Prepare a statement and call SQLColAttribute, succeeds but is undefined
	EXECUTE_AND_CHECK("SQLExecDirect", hstmt, SQLExecDirect, hstmt,
	                  ConvertToSQLCHAR("create table colattrfoo(col1 int, col2 varchar(20))"), SQL_NTS);

	EXECUTE_AND_CHECK("SQLPrepare", hstmt, SQLPrepare, hstmt, ConvertToSQLCHAR("select * From colattrfoo"), SQL_NTS);

	SQLLEN fixed_prec_scale;
	EXECUTE_AND_CHECK("SQLColAttribute", hstmt, SQLColAttribute, hstmt, 1, SQL_DESC_FIXED_PREC_SCALE, nullptr, 0,
	                  nullptr, &fixed_prec_scale);

	// Free the statement handle
	EXECUTE_AND_CHECK("SQLFreeStmt (HSTMT)", hstmt, SQLFreeStmt, hstmt, SQL_CLOSE);
	EXECUTE_AND_CHECK("SQLFreeHandle (HSTMT)", hstmt, SQLFreeHandle, SQL_HANDLE_STMT, hstmt);

	// Disconnect from the database
	DISCONNECT_FROM_DATABASE(env, dbc);
}

TEST_CASE("Test SQLColAttribute with unicode column name", "[odbc]") {
	SQLHANDLE env;
	SQLHANDLE dbc;

	HSTMT hstmt = SQL_NULL_HSTMT;

	// Connect to the database using SQLConnect
	CONNECT_TO_DATABASE(env, dbc);

	// Allocate a statement handle
	EXECUTE_AND_CHECK("SQLAllocHandle (HSTMT)", hstmt, SQLAllocHandle, SQL_HANDLE_STMT, dbc, &hstmt);

	std::vector<SQLCHAR> hello_bg_utf8 = {0xd0, 0x97, 0xd0, 0xb4, 0xd1, 0x80, 0xd0, 0xb0, 0xd0,
	                                      0xb2, 0xd0, 0xb5, 0xd0, 0xb9, 0xd1, 0x82, 0xd0, 0xb5};
	std::string hello_bg_utf8_str(reinterpret_cast<char *>(hello_bg_utf8.data()), hello_bg_utf8.size());
	std::vector<SQLWCHAR> hello_bg_utf16 = {0x0417, 0x0434, 0x0440, 0x0430, 0x0432, 0x0435, 0x0439, 0x0442, 0x0435};

	// Get column attributes of a simple query
	EXECUTE_AND_CHECK("SQLExectDirectW", hstmt, SQLExecDirectW, hstmt,
	                  ConvertToSQLWCHARNTS("SELECT 42 as \"" + hello_bg_utf8_str + "\"").data(), SQL_NTS);

	// Check column name UTF-16
	std::vector<SQLWCHAR> col_name_utf16;
	col_name_utf16.resize(64);
	SQLSMALLINT col_name_utf16_len_bytes = 0;
	EXECUTE_AND_CHECK("SQLColAttributeW", hstmt, SQLColAttributeW, hstmt, 1, SQL_DESC_NAME, col_name_utf16.data(),
	                  col_name_utf16.size() * sizeof(SQLWCHAR), &col_name_utf16_len_bytes, nullptr);
	auto utf16_len_calculated = duckdb::widechar::utf16_length(col_name_utf16.data());
	REQUIRE(col_name_utf16_len_bytes == utf16_len_calculated * sizeof(SQLWCHAR));
	std::string col_name_utf8_converted = ConvertToString(col_name_utf16.data());
	REQUIRE(STR_EQUAL(col_name_utf8_converted.c_str(), hello_bg_utf8_str.c_str()));

	// Check column name UTF-8
	std::vector<SQLCHAR> col_name_utf8;
	col_name_utf8.resize(64);
	SQLSMALLINT col_name_utf8_len_bytes = 0;
	EXECUTE_AND_CHECK("SQLColAttribute", hstmt, SQLColAttribute, hstmt, 1, SQL_DESC_NAME, col_name_utf8.data(),
	                  col_name_utf8.size(), &col_name_utf8_len_bytes, nullptr);
	auto utf8_len_calculated = std::strlen(reinterpret_cast<char *>(col_name_utf8.data()));
	REQUIRE(col_name_utf8_len_bytes == utf8_len_calculated);
	std::string col_name_utf8_str = std::string(reinterpret_cast<char *>(col_name_utf8.data()), utf8_len_calculated);
	REQUIRE(STR_EQUAL(col_name_utf8_str.c_str(), hello_bg_utf8_str.c_str()));

	// Free the statement handle
	EXECUTE_AND_CHECK("SQLFreeStmt (HSTMT)", hstmt, SQLFreeStmt, hstmt, SQL_CLOSE);
	EXECUTE_AND_CHECK("SQLFreeHandle (HSTMT)", hstmt, SQLFreeHandle, SQL_HANDLE_STMT, hstmt);

	// Disconnect from the database
	DISCONNECT_FROM_DATABASE(env, dbc);
}
