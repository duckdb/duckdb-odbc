#include "utils.h"

using namespace odbc_col_attribute_test;

TEST_CASE("Test SQLColAttribute for a query that returns an interval", "[odbc]") {
	SQLHANDLE env;
	SQLHANDLE dbc;

	HSTMT hstmt = SQL_NULL_HSTMT;

	// Connect to the database using SQLConnect
	CONNECT_TO_DATABASE(env, dbc);

	// Allocate a statement handle
	EXECUTE_AND_CHECK("SQLAllocHandle (HSTMT)", SQLAllocHandle, SQL_HANDLE_STMT, dbc, &hstmt);

	// run a simple query  with ints to get a result set
	EXECUTE_AND_CHECK("SQLExecDirect", SQLExecDirect, hstmt, ConvertToSQLCHAR("SELECT INTERVAL 1 HOUR AS a, INTERVAL 2 HOUR AS b"), SQL_NTS);
	std::map<SQLLEN, ExpectedResult> expected_interval;
	expected_interval[SQL_DESC_CASE_SENSITIVE] = ExpectedResult(SQL_FALSE);
	expected_interval[SQL_DESC_CATALOG_NAME] = ExpectedResult("system");
	expected_interval[SQL_DESC_CONCISE_TYPE] = ExpectedResult(SQL_VARCHAR);
	expected_interval[SQL_DESC_COUNT] = ExpectedResult(2);
	expected_interval[SQL_DESC_DISPLAY_SIZE] = ExpectedResult(0);
	expected_interval[SQL_DESC_FIXED_PREC_SCALE] = ExpectedResult(SQL_FALSE);
	expected_interval[SQL_DESC_LENGTH] = ExpectedResult(14);
	expected_interval[SQL_DESC_LITERAL_PREFIX] = ExpectedResult("''''");
	expected_interval[SQL_DESC_LITERAL_SUFFIX] = ExpectedResult("''''");
	expected_interval[SQL_DESC_LOCAL_TYPE_NAME] = ExpectedResult("");
	expected_interval[SQL_DESC_NULLABLE] = ExpectedResult(SQL_NULLABLE);
	expected_interval[SQL_DESC_NUM_PREC_RADIX] = ExpectedResult(0);
	expected_interval[SQL_DESC_PRECISION] = ExpectedResult(14);
	expected_interval[SQL_COLUMN_SCALE] = ExpectedResult(0);
	expected_interval[SQL_DESC_SCALE] = ExpectedResult(0);
	expected_interval[SQL_DESC_SCHEMA_NAME] = ExpectedResult("");
	expected_interval[SQL_DESC_SEARCHABLE] = ExpectedResult(SQL_PRED_BASIC);
	expected_interval[SQL_DESC_TYPE] = ExpectedResult(SQL_VARCHAR);
	expected_interval[SQL_DESC_UNNAMED] = ExpectedResult(SQL_NAMED);
	expected_interval[SQL_DESC_UNSIGNED] = ExpectedResult(1);
	expected_interval[SQL_DESC_UPDATABLE] = ExpectedResult(SQL_ATTR_READONLY);
	TestAllFields(hstmt, expected_interval);

	// Free the statement handle
	EXECUTE_AND_CHECK("SQLFreeStmt (HSTMT)", SQLFreeStmt, hstmt, SQL_CLOSE);
	EXECUTE_AND_CHECK("SQLFreeHandle (HSTMT)", SQLFreeHandle, SQL_HANDLE_STMT, hstmt);

	// Disconnect from the database
	DISCONNECT_FROM_DATABASE(env, dbc);
}
