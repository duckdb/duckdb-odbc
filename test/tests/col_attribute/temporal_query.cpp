#include "utils.h"

using namespace odbc_col_attribute_test;

TEST_CASE("Test SQLColAttribute with TIME/TIMESTAMP precision", "[odbc]") {
	SQLRETURN ret;
	SQLHANDLE env;
	SQLHANDLE dbc;
	HSTMT hstmt = SQL_NULL_HSTMT;

	// Connect to the database using SQLConnect
	CONNECT_TO_DATABASE(env, dbc);

	// Allocate a statement handle
	EXECUTE_AND_CHECK("SQLAllocHandle (HSTMT)", hstmt, SQLAllocHandle, SQL_HANDLE_STMT, dbc, &hstmt);

	// Test various TIME/TIMESTAMP types with different precisions
	EXECUTE_AND_CHECK("SQLExecDirect", hstmt, SQLExecDirect, hstmt,
	                  ConvertToSQLCHAR("SELECT "
	                                   "'12:34:56'::TIME AS time_us, "
	                                   "'2023-01-01 12:34:56'::TIMESTAMP AS timestamp_us, "
	                                   "'2023-01-01 12:34:56.123'::timestamp_ms AS timestamp_ms, "
	                                   "'2023-01-01 12:34:56'::timestamp_s AS timestamp_s, "
	                                   "'2023-01-01'::DATE AS date_col"),
	                  SQL_NTS);

	// Get the number of columns
	SQLSMALLINT num_cols;
	EXECUTE_AND_CHECK("SQLNumResultCols", hstmt, SQLNumResultCols, hstmt, &num_cols);
	REQUIRE(num_cols == 5);

	// Test each temporal column
	struct TemporalTest {
		int col;
		const char *name;
		SQLSMALLINT expected_type;
		SQLLEN expected_precision;
		SQLLEN expected_scale;
		SQLLEN expected_display_size;
	};

	TemporalTest tests[] = {{1, "time_us", SQL_TYPE_TIME, 6, 6, 15},
	                        {2, "timestamp_us", SQL_TYPE_TIMESTAMP, 6, 6, 26},
	                        {3, "timestamp_ms", SQL_TYPE_TIMESTAMP, 3, 3, 23},
	                        {4, "timestamp_s", SQL_TYPE_TIMESTAMP, 0, 0, 19},
	                        {5, "date_col", SQL_TYPE_DATE, 0, 0, 10}};

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

		// Get precision (only check for TIME/TIMESTAMP types)
		if (test.expected_type == SQL_TYPE_TIME || test.expected_type == SQL_TYPE_TIMESTAMP) {
			SQLLEN precision;
			EXECUTE_AND_CHECK("SQLColAttribute (PRECISION)", hstmt, SQLColAttribute, hstmt, test.col,
			                  SQL_DESC_PRECISION, nullptr, 0, nullptr, &precision);
			REQUIRE(precision == test.expected_precision);

			// Get scale
			SQLLEN scale;
			EXECUTE_AND_CHECK("SQLColAttribute (SCALE)", hstmt, SQLColAttribute, hstmt, test.col, SQL_DESC_SCALE,
			                  nullptr, 0, nullptr, &scale);
			REQUIRE(scale == test.expected_scale);
		}

		// Get display size
		SQLLEN display_size;
		EXECUTE_AND_CHECK("SQLColAttribute (DISPLAY_SIZE)", hstmt, SQLColAttribute, hstmt, test.col,
		                  SQL_DESC_DISPLAY_SIZE, nullptr, 0, nullptr, &display_size);
		REQUIRE(display_size == test.expected_display_size);
	}

	// Free resources
	EXECUTE_AND_CHECK("SQLFreeStmt (HSTMT)", hstmt, SQLFreeStmt, hstmt, SQL_CLOSE);
	EXECUTE_AND_CHECK("SQLFreeHandle (HSTMT)", hstmt, SQLFreeHandle, SQL_HANDLE_STMT, hstmt);
	DISCONNECT_FROM_DATABASE(env, dbc);
}

TEST_CASE("Test SQLDescribeCol with TIME/TIMESTAMP precision", "[odbc]") {
	SQLRETURN ret;
	SQLHANDLE env;
	SQLHANDLE dbc;
	HSTMT hstmt = SQL_NULL_HSTMT;

	// Connect to the database using SQLConnect
	CONNECT_TO_DATABASE(env, dbc);

	// Allocate a statement handle
	EXECUTE_AND_CHECK("SQLAllocHandle (HSTMT)", hstmt, SQLAllocHandle, SQL_HANDLE_STMT, dbc, &hstmt);

	// Test various TIME/TIMESTAMP types
	EXECUTE_AND_CHECK("SQLExecDirect", hstmt, SQLExecDirect, hstmt,
	                  ConvertToSQLCHAR("SELECT "
	                                   "'12:34:56'::TIME AS time_us, "
	                                   "'2023-01-01 12:34:56'::TIMESTAMP AS timestamp_us, "
	                                   "'2023-01-01 12:34:56.123'::timestamp_ms AS timestamp_ms, "
	                                   "'2023-01-01 12:34:56'::timestamp_s AS timestamp_s"),
	                  SQL_NTS);

	// Get the number of columns
	SQLSMALLINT num_cols;
	EXECUTE_AND_CHECK("SQLNumResultCols", hstmt, SQLNumResultCols, hstmt, &num_cols);
	REQUIRE(num_cols == 4);

	// Test each temporal column with SQLDescribeCol
	struct TemporalTest {
		int col;
		const char *name;
		SQLSMALLINT expected_type;
		SQLULEN expected_column_size;        // precision for temporal types
		SQLSMALLINT expected_decimal_digits; // scale for temporal types
	};

	TemporalTest tests[] = {{1, "time_us", SQL_TYPE_TIME, 15, 6},
	                        {2, "timestamp_us", SQL_TYPE_TIMESTAMP, 26, 6},
	                        {3, "timestamp_ms", SQL_TYPE_TIMESTAMP, 23, 3},
	                        {4, "timestamp_s", SQL_TYPE_TIMESTAMP, 19, 0}};

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
		REQUIRE(STR_EQUAL(reinterpret_cast<char *>(col_name), test.name));
		REQUIRE(name_length == strlen(test.name));

		// Verify data type
		REQUIRE(data_type == test.expected_type);

		// Verify precision (column_size)
		REQUIRE(column_size == test.expected_column_size);

		// Verify scale (decimal_digits)
		REQUIRE(decimal_digits == test.expected_decimal_digits);

		// Verify nullable
		REQUIRE(nullable == SQL_NULLABLE_UNKNOWN);
	}

	// Free resources
	EXECUTE_AND_CHECK("SQLFreeStmt (HSTMT)", hstmt, SQLFreeStmt, hstmt, SQL_CLOSE);
	EXECUTE_AND_CHECK("SQLFreeHandle (HSTMT)", hstmt, SQLFreeHandle, SQL_HANDLE_STMT, hstmt);
	DISCONNECT_FROM_DATABASE(env, dbc);
}
