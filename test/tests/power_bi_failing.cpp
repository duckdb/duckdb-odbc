#include "odbc_test_common.h"

// TODO REMOVE ME
#include <iostream>

using namespace odbc_test;

TEST_CASE("Test PowerBI issue", "[odbc]") {
	SQLHANDLE env;
	SQLHANDLE dbc;

	HSTMT hstmt = SQL_NULL_HSTMT;

	// Connect to the database using SQLConnect
	CONNECT_TO_DATABASE(env, dbc);

	// Allocate a statement handle
	EXECUTE_AND_CHECK("SQLAllocHandle (HSTMT)", hstmt, hstmt, hstmt, SQLAllocHandle, SQL_HANDLE_STMT, dbc, &hstmt);

	// Drop the test table if it exists
	EXEC_SQL(hstmt, "DROP TABLE IF EXISTS test_table");

	// Create a test table
	EXEC_SQL(hstmt, "CREATE TABLE test_table (id_1 INTEGER, customer_small_int_field_1 INTEGER)");

	// Insert some data
	EXEC_SQL(hstmt, "INSERT INTO test_table VALUES"
	                "(1, 10),"
	                "(1, 20),"
	                "(1, NULL),"
	                "(2, 20),"
	                "(2, 30),"
	                "(3, 30),"
	                "(3, 40),"
	                "(3, 50),"
	                "(3, NULL)");

	// Execute PowerBI query
	auto ret = SQLExecDirect(hstmt,
	                         ConvertToSQLCHAR("SELECT \"id_1\", "
	                                          "CAST(COUNT(DISTINCT \"customer_small_int_field_1\") AS DOUBLE) + "
	                                          "CAST(MAX(\"C1\") AS DOUBLE) AS \"C1\" "
	                                          "FROM ("
	                                          "    SELECT \"customer_small_int_field_1\", "
	                                          "           \"id_1\", "
	                                          "           CASE "
	                                          "               WHEN \"customer_small_int_field_1\" IS NULL "
	                                          "				  THEN ? "
	                                          "               ELSE ? "
	                                          "           END AS \"C1\" "
	                                          "    FROM \"test_table\""
	                                          ") AS \"ITBL\" "
	                                          "GROUP BY \"id_1\" "
	                                          "ORDER BY \"C1\" DESC, \"id_1\" "
	                                          "LIMIT 1001"),
	                         SQL_NTS);

	if (ret == SQL_ERROR) {
		// Get the diagnostics
		std::string state;
		std::string message;
		ACCESS_DIAGNOSTIC(state, message, hstmt, SQL_HANDLE_STMT);
		std::cout << state << ": " << message << std::endl;
	}
}
