#include "utils.h"

using namespace odbc_col_attribute_test;

TEST_CASE("Test SQLColAttribute with unsigned integer types - display sizes", "[odbc]") {
	SQLRETURN ret;
	SQLHANDLE env;
	SQLHANDLE dbc;
	HSTMT hstmt = SQL_NULL_HSTMT;

	// Connect to the database using SQLConnect
	CONNECT_TO_DATABASE(env, dbc);

	// Allocate a statement handle
	EXECUTE_AND_CHECK("SQLAllocHandle (HSTMT)", hstmt, SQLAllocHandle, SQL_HANDLE_STMT, dbc, &hstmt);

	// Test various unsigned integer types
	EXECUTE_AND_CHECK("SQLExecDirect", hstmt, SQLExecDirect, hstmt,
	                  ConvertToSQLCHAR("SELECT "
	                                   "255::UTINYINT AS utinyint_col, "
	                                   "65535::USMALLINT AS usmallint_col, "
	                                   "4294967295::UINTEGER AS uinteger_col, "
	                                   "18446744073709551615::UBIGINT AS ubigint_col, "
	                                   "127::TINYINT AS tinyint_col, "
	                                   "32767::SMALLINT AS smallint_col, "
	                                   "2147483647::INTEGER AS integer_col, "
	                                   "9223372036854775807::BIGINT AS bigint_col"),
	                  SQL_NTS);

	// Get the number of columns
	SQLSMALLINT num_cols;
	EXECUTE_AND_CHECK("SQLNumResultCols", hstmt, SQLNumResultCols, hstmt, &num_cols);
	REQUIRE(num_cols == 8);

	// Test each integer column
	struct IntegerTest {
		int col;
		const char *name;
		SQLSMALLINT expected_type;
		SQLLEN expected_display_size;
		bool is_unsigned;
	};

	IntegerTest tests[] = {{1, "utinyint_col", SQL_TINYINT, 3, true},  {2, "usmallint_col", SQL_SMALLINT, 5, true},
	                       {3, "uinteger_col", SQL_INTEGER, 10, true}, {4, "ubigint_col", SQL_BIGINT, 20, true},
	                       {5, "tinyint_col", SQL_TINYINT, 4, false},  {6, "smallint_col", SQL_SMALLINT, 6, false},
	                       {7, "integer_col", SQL_INTEGER, 11, false}, {8, "bigint_col", SQL_BIGINT, 20, false}};

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

		// Get display size
		SQLLEN display_size;
		EXECUTE_AND_CHECK("SQLColAttribute (DISPLAY_SIZE)", hstmt, SQLColAttribute, hstmt, test.col,
		                  SQL_DESC_DISPLAY_SIZE, nullptr, 0, nullptr, &display_size);
		REQUIRE(display_size == test.expected_display_size);

		// Check unsigned attribute
		SQLLEN unsigned_attr;
		EXECUTE_AND_CHECK("SQLColAttribute (UNSIGNED)", hstmt, SQLColAttribute, hstmt, test.col, SQL_DESC_UNSIGNED,
		                  nullptr, 0, nullptr, &unsigned_attr);
		if (test.is_unsigned) {
			REQUIRE(unsigned_attr == SQL_TRUE);
		} else {
			REQUIRE(unsigned_attr == SQL_FALSE);
		}
	}

	// Free resources
	EXECUTE_AND_CHECK("SQLFreeStmt (HSTMT)", hstmt, SQLFreeStmt, hstmt, SQL_CLOSE);
	EXECUTE_AND_CHECK("SQLFreeHandle (HSTMT)", hstmt, SQLFreeHandle, SQL_HANDLE_STMT, hstmt);
	DISCONNECT_FROM_DATABASE(env, dbc);
}

TEST_CASE("Test SQLDescribeCol with unsigned integer types - display sizes", "[odbc]") {
	SQLRETURN ret;
	SQLHANDLE env;
	SQLHANDLE dbc;
	HSTMT hstmt = SQL_NULL_HSTMT;

	// Connect to the database using SQLConnect
	CONNECT_TO_DATABASE(env, dbc);

	// Allocate a statement handle
	EXECUTE_AND_CHECK("SQLAllocHandle (HSTMT)", hstmt, SQLAllocHandle, SQL_HANDLE_STMT, dbc, &hstmt);

	// Test various unsigned integer types
	EXECUTE_AND_CHECK("SQLExecDirect", hstmt, SQLExecDirect, hstmt,
	                  ConvertToSQLCHAR("SELECT "
	                                   "255::UTINYINT AS utinyint_col, "
	                                   "65535::USMALLINT AS usmallint_col, "
	                                   "4294967295::UINTEGER AS uinteger_col, "
	                                   "18446744073709551615::UBIGINT AS ubigint_col"),
	                  SQL_NTS);

	// Get the number of columns
	SQLSMALLINT num_cols;
	EXECUTE_AND_CHECK("SQLNumResultCols", hstmt, SQLNumResultCols, hstmt, &num_cols);
	REQUIRE(num_cols == 4);

	// Test each unsigned integer column with SQLDescribeCol
	struct UnsignedIntegerTest {
		int col;
		const char *name;
		SQLSMALLINT expected_type;
		SQLULEN expected_column_size; // precision for integer types
	};

	UnsignedIntegerTest tests[] = {{1, "utinyint_col", SQL_TINYINT, 3},
	                               {2, "usmallint_col", SQL_SMALLINT, 5},
	                               {3, "uinteger_col", SQL_INTEGER, 10},
	                               {4, "ubigint_col", SQL_BIGINT, 20}};

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

		// Verify column size (precision for integer types)
		REQUIRE(column_size == test.expected_column_size);

		// Verify decimal digits (should be 0 for integer types)
		REQUIRE(decimal_digits == 0);

		// Verify nullable
		REQUIRE(nullable == SQL_NULLABLE_UNKNOWN);
	}

	// Free resources
	EXECUTE_AND_CHECK("SQLFreeStmt (HSTMT)", hstmt, SQLFreeStmt, hstmt, SQL_CLOSE);
	EXECUTE_AND_CHECK("SQLFreeHandle (HSTMT)", hstmt, SQLFreeHandle, SQL_HANDLE_STMT, hstmt);
	DISCONNECT_FROM_DATABASE(env, dbc);
}
