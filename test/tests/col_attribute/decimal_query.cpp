#include "utils.h"

using namespace odbc_col_attribute_test;

TEST_CASE("Test SQLColAttribute with decimal types - precision and scale", "[odbc]") {
	SQLRETURN ret;
	SQLHANDLE env;
	SQLHANDLE dbc;
	HSTMT hstmt = SQL_NULL_HSTMT;

	// Connect to the database using SQLConnect
	CONNECT_TO_DATABASE(env, dbc);

	// Allocate a statement handle
	EXECUTE_AND_CHECK("SQLAllocHandle (HSTMT)", hstmt, SQLAllocHandle, SQL_HANDLE_STMT, dbc, &hstmt);

	// Test various decimal types with different precision and scale
	EXECUTE_AND_CHECK("SQLExecDirect", hstmt, SQLExecDirect, hstmt,
	                  ConvertToSQLCHAR("SELECT "
	                                   "123.45::DECIMAL(5,2) AS dec_5_2, "
	                                   "9999.9999::DECIMAL(8,4) AS dec_8_4, "
	                                   "0.123456789::DECIMAL(10,9) AS dec_10_9, "
	                                   "12345678901234567890.123456789::DECIMAL(30,9) AS dec_30_9, "
	                                   "99999999999999999999999999999999999999::DECIMAL(38,0) AS dec_38_0, "
	                                   "0.0000000000000000000000000000000000001::DECIMAL(38,37) AS dec_38_37"),
	                  SQL_NTS);

	// Get the number of columns
	SQLSMALLINT num_cols;
	EXECUTE_AND_CHECK("SQLNumResultCols", hstmt, SQLNumResultCols, hstmt, &num_cols);
	REQUIRE(num_cols == 6);

	// Test each decimal column
	struct DecimalTest {
		int col;
		const char *name;
		SQLSMALLINT expected_type;
		SQLULEN expected_precision;
		SQLSMALLINT expected_scale;
	};

	DecimalTest tests[] = {{1, "dec_5_2", SQL_DECIMAL, 5, 2},   {2, "dec_8_4", SQL_DECIMAL, 8, 4},
	                       {3, "dec_10_9", SQL_DECIMAL, 10, 9}, {4, "dec_30_9", SQL_DECIMAL, 30, 9},
	                       {5, "dec_38_0", SQL_DECIMAL, 38, 0}, {6, "dec_38_37", SQL_DECIMAL, 38, 37}};

	for (const auto &test : tests) {
		// Get column name
		char col_name[256];
		EXECUTE_AND_CHECK("SQLColAttribute (NAME)", hstmt, SQLColAttribute, hstmt, test.col, SQL_DESC_NAME, col_name,
		                  sizeof(col_name), nullptr, nullptr);
		REQUIRE(STR_EQUAL(col_name, test.name));

		// Get column type
		SQLLEN col_type;
		EXECUTE_AND_CHECK("SQLColAttribute (TYPE)", hstmt, SQLColAttribute, hstmt, test.col, SQL_DESC_TYPE, nullptr, 0,
		                  nullptr, &col_type);
		REQUIRE(col_type == test.expected_type);

		// Get precision
		SQLLEN precision;
		EXECUTE_AND_CHECK("SQLColAttribute (PRECISION)", hstmt, SQLColAttribute, hstmt, test.col, SQL_DESC_PRECISION,
		                  nullptr, 0, nullptr, &precision);
		REQUIRE(precision == test.expected_precision);

		// Get scale
		SQLLEN scale;
		EXECUTE_AND_CHECK("SQLColAttribute (SCALE)", hstmt, SQLColAttribute, hstmt, test.col, SQL_DESC_SCALE, nullptr,
		                  0, nullptr, &scale);
		REQUIRE(scale == test.expected_scale);

		// Get display size
		SQLLEN display_size;
		EXECUTE_AND_CHECK("SQLColAttribute (DISPLAY_SIZE)", hstmt, SQLColAttribute, hstmt, test.col,
		                  SQL_DESC_DISPLAY_SIZE, nullptr, 0, nullptr, &display_size);
		// Display size should be precision + 2 (for sign and decimal point)
		REQUIRE(display_size == test.expected_precision + 2);
	}

	// Free resources
	EXECUTE_AND_CHECK("SQLFreeStmt (HSTMT)", hstmt, SQLFreeStmt, hstmt, SQL_CLOSE);
	EXECUTE_AND_CHECK("SQLFreeHandle (HSTMT)", hstmt, SQLFreeHandle, SQL_HANDLE_STMT, hstmt);
	DISCONNECT_FROM_DATABASE(env, dbc);
}

TEST_CASE("Test SQLDescribeCol with decimal types - precision and scale", "[odbc]") {
	SQLRETURN ret;
	SQLHANDLE env;
	SQLHANDLE dbc;
	HSTMT hstmt = SQL_NULL_HSTMT;

	// Connect to the database using SQLConnect
	CONNECT_TO_DATABASE(env, dbc);

	// Allocate a statement handle
	EXECUTE_AND_CHECK("SQLAllocHandle (HSTMT)", hstmt, SQLAllocHandle, SQL_HANDLE_STMT, dbc, &hstmt);

	// Test various decimal types
	EXECUTE_AND_CHECK("SQLExecDirect", hstmt, SQLExecDirect, hstmt,
	                  ConvertToSQLCHAR("SELECT "
	                                   "123.45::DECIMAL(5,2) AS dec_5_2, "
	                                   "9999.9999::DECIMAL(8,4) AS dec_8_4, "
	                                   "0.123456789::DECIMAL(10,9) AS dec_10_9, "
	                                   "12345678901234567890.123456789::DECIMAL(30,9) AS dec_30_9, "
	                                   "99999999999999999999999999999999999999::DECIMAL(38,0) AS dec_38_0, "
	                                   "0.0000000000000000000000000000000000001::DECIMAL(38,37) AS dec_38_37"),
	                  SQL_NTS);

	// Get the number of columns
	SQLSMALLINT num_cols;
	EXECUTE_AND_CHECK("SQLNumResultCols", hstmt, SQLNumResultCols, hstmt, &num_cols);
	REQUIRE(num_cols == 6);

	// Test each decimal column with SQLDescribeCol
	struct DecimalTest {
		int col;
		const char *name;
		SQLSMALLINT expected_type;
		SQLULEN expected_precision;
		SQLSMALLINT expected_scale;
	};

	DecimalTest tests[] = {{1, "dec_5_2", SQL_DECIMAL, 5, 2},   {2, "dec_8_4", SQL_DECIMAL, 8, 4},
	                       {3, "dec_10_9", SQL_DECIMAL, 10, 9}, {4, "dec_30_9", SQL_DECIMAL, 30, 9},
	                       {5, "dec_38_0", SQL_DECIMAL, 38, 0}, {6, "dec_38_37", SQL_DECIMAL, 38, 37}};

	for (const auto &test : tests) {
		SQLCHAR col_name[256];
		SQLSMALLINT name_length;
		SQLSMALLINT data_type;
		SQLULEN column_size;
		SQLSMALLINT decimal_digits;
		SQLSMALLINT nullable;

		// Call SQLDescribeCol
		EXECUTE_AND_CHECK("SQLDescribeCol", hstmt, SQLDescribeCol, hstmt, test.col, col_name, sizeof(col_name),
		                  &name_length, &data_type, &column_size, &decimal_digits, &nullable);

		// Verify column name
		REQUIRE(STR_EQUAL((char *)col_name, test.name));
		REQUIRE(name_length == strlen(test.name));

		// Verify data type
		REQUIRE(data_type == test.expected_type);

		// Verify precision (column_size)
		REQUIRE(column_size == test.expected_precision);

		// Verify scale (decimal_digits)
		REQUIRE(decimal_digits == test.expected_scale);

		// Verify nullable
		REQUIRE(nullable == SQL_NULLABLE_UNKNOWN);
	}

	// Test with NULL parameters (should still succeed)
	EXECUTE_AND_CHECK("SQLDescribeCol with NULLs", hstmt, SQLDescribeCol, hstmt, 1, nullptr, 0, nullptr, nullptr,
	                  nullptr, nullptr, nullptr);

	// Free resources
	EXECUTE_AND_CHECK("SQLFreeStmt (HSTMT)", hstmt, SQLFreeStmt, hstmt, SQL_CLOSE);
	EXECUTE_AND_CHECK("SQLFreeHandle (HSTMT)", hstmt, SQLFreeHandle, SQL_HANDLE_STMT, hstmt);
	DISCONNECT_FROM_DATABASE(env, dbc);
}

TEST_CASE("Test numeric type mapping - NUMERIC vs DECIMAL", "[odbc]") {
	SQLRETURN ret;
	SQLHANDLE env;
	SQLHANDLE dbc;
	HSTMT hstmt = SQL_NULL_HSTMT;

	// Connect to the database using SQLConnect
	CONNECT_TO_DATABASE(env, dbc);

	// Allocate a statement handle
	EXECUTE_AND_CHECK("SQLAllocHandle (HSTMT)", hstmt, SQLAllocHandle, SQL_HANDLE_STMT, dbc, &hstmt);

	// Test NUMERIC type (should behave same as DECIMAL)
	EXECUTE_AND_CHECK("SQLExecDirect", hstmt, SQLExecDirect, hstmt,
	                  ConvertToSQLCHAR("SELECT "
	                                   "123.45::NUMERIC(5,2) AS num_5_2, "
	                                   "123.45::DECIMAL(5,2) AS dec_5_2"),
	                  SQL_NTS);

	// Check NUMERIC column
	SQLLEN numeric_type;
	EXECUTE_AND_CHECK("SQLColAttribute (NUMERIC TYPE)", hstmt, SQLColAttribute, hstmt, 1, SQL_DESC_TYPE, nullptr, 0,
	                  nullptr, &numeric_type);

	// Check DECIMAL column
	SQLLEN decimal_type;
	EXECUTE_AND_CHECK("SQLColAttribute (DECIMAL TYPE)", hstmt, SQLColAttribute, hstmt, 2, SQL_DESC_TYPE, nullptr, 0,
	                  nullptr, &decimal_type);

	// Both should map to SQL_DECIMAL
	REQUIRE(numeric_type == SQL_DECIMAL);
	REQUIRE(decimal_type == SQL_DECIMAL);

	// Check precision and scale for NUMERIC
	SQLLEN numeric_precision, numeric_scale;
	EXECUTE_AND_CHECK("SQLColAttribute (NUMERIC PRECISION)", hstmt, SQLColAttribute, hstmt, 1, SQL_DESC_PRECISION,
	                  nullptr, 0, nullptr, &numeric_precision);
	EXECUTE_AND_CHECK("SQLColAttribute (NUMERIC SCALE)", hstmt, SQLColAttribute, hstmt, 1, SQL_DESC_SCALE, nullptr, 0,
	                  nullptr, &numeric_scale);

	// Check precision and scale for DECIMAL
	SQLLEN decimal_precision, decimal_scale;
	EXECUTE_AND_CHECK("SQLColAttribute (DECIMAL PRECISION)", hstmt, SQLColAttribute, hstmt, 2, SQL_DESC_PRECISION,
	                  nullptr, 0, nullptr, &decimal_precision);
	EXECUTE_AND_CHECK("SQLColAttribute (DECIMAL SCALE)", hstmt, SQLColAttribute, hstmt, 2, SQL_DESC_SCALE, nullptr, 0,
	                  nullptr, &decimal_scale);

	// Both should have same precision and scale
	REQUIRE(numeric_precision == 5);
	REQUIRE(numeric_scale == 2);
	REQUIRE(decimal_precision == 5);
	REQUIRE(decimal_scale == 2);

	// Free resources
	EXECUTE_AND_CHECK("SQLFreeStmt (HSTMT)", hstmt, SQLFreeStmt, hstmt, SQL_CLOSE);
	EXECUTE_AND_CHECK("SQLFreeHandle (HSTMT)", hstmt, SQLFreeHandle, SQL_HANDLE_STMT, hstmt);
	DISCONNECT_FROM_DATABASE(env, dbc);
}
