#include "odbc_test_common.h"

using namespace odbc_test;

// A column-wise parameter array of fixed-length C types has to store each set's own value
TEST_CASE("Test parameter array with fixed-length types", "[odbc]") {
	SQLHANDLE env;
	SQLHANDLE dbc;
	HSTMT hstmt = SQL_NULL_HSTMT;

	CONNECT_TO_DATABASE(env, dbc);
	EXECUTE_AND_CHECK("SQLAllocHandle (HSTMT)", hstmt, SQLAllocHandle, SQL_HANDLE_STMT, dbc, &hstmt);
	EXECUTE_AND_CHECK("SQLExecDirect (CREATE TABLE)", hstmt, SQLExecDirect, hstmt,
	                  ConvertToSQLCHAR("CREATE TABLE param_array_fixed (i INTEGER, b BIGINT, d DOUBLE, dt DATE, ts "
	                                   "TIMESTAMP, s VARCHAR)"),
	                  SQL_NTS);

	const SQLULEN set_count = 3;
	SQLINTEGER i_vals[set_count] = {10, 20, 30};
	SQLBIGINT b_vals[set_count] = {10000000000LL, 20000000000LL, 30000000000LL};
	SQLDOUBLE d_vals[set_count] = {1.5, 2.5, 3.5};
	SQL_DATE_STRUCT dt_vals[set_count] = {{2024, 1, 1}, {2024, 2, 29}, {2024, 12, 31}};
	SQL_TIMESTAMP_STRUCT ts_vals[set_count] = {
	    {2024, 1, 1, 1, 2, 3, 0}, {2024, 2, 29, 13, 45, 10, 0}, {2024, 12, 31, 23, 59, 59, 0}};
	SQLCHAR s_vals[set_count][8] = {"a", "bb", "ccc"};
	SQLLEN i_ind[set_count] = {0, SQL_NULL_DATA, 0};
	SQLLEN b_ind[set_count] = {0, 0, 0};
	SQLLEN d_ind[set_count] = {0, 0, 0};
	SQLLEN dt_ind[set_count] = {0, 0, 0};
	SQLLEN ts_ind[set_count] = {0, 0, 0};
	SQLLEN s_ind[set_count] = {SQL_NTS, SQL_NTS, SQL_NTS};
	SQLULEN processed = 0;

	EXECUTE_AND_CHECK("SQLSetStmtAttr (SQL_ATTR_PARAM_BIND_TYPE)", hstmt, SQLSetStmtAttr, hstmt,
	                  SQL_ATTR_PARAM_BIND_TYPE, reinterpret_cast<SQLPOINTER>(SQL_PARAM_BIND_BY_COLUMN), 0);
	EXECUTE_AND_CHECK("SQLSetStmtAttr (SQL_ATTR_PARAMSET_SIZE)", hstmt, SQLSetStmtAttr, hstmt, SQL_ATTR_PARAMSET_SIZE,
	                  reinterpret_cast<SQLPOINTER>(set_count), 0);
	EXECUTE_AND_CHECK("SQLSetStmtAttr (SQL_ATTR_PARAMS_PROCESSED_PTR)", hstmt, SQLSetStmtAttr, hstmt,
	                  SQL_ATTR_PARAMS_PROCESSED_PTR, &processed, 0);

	EXECUTE_AND_CHECK("SQLBindParameter (INTEGER)", hstmt, SQLBindParameter, hstmt, 1, SQL_PARAM_INPUT, SQL_C_SLONG,
	                  SQL_INTEGER, 0, 0, i_vals, 0, i_ind);
	EXECUTE_AND_CHECK("SQLBindParameter (BIGINT)", hstmt, SQLBindParameter, hstmt, 2, SQL_PARAM_INPUT, SQL_C_SBIGINT,
	                  SQL_BIGINT, 0, 0, b_vals, 0, b_ind);
	EXECUTE_AND_CHECK("SQLBindParameter (DOUBLE)", hstmt, SQLBindParameter, hstmt, 3, SQL_PARAM_INPUT, SQL_C_DOUBLE,
	                  SQL_DOUBLE, 0, 0, d_vals, 0, d_ind);
	EXECUTE_AND_CHECK("SQLBindParameter (DATE)", hstmt, SQLBindParameter, hstmt, 4, SQL_PARAM_INPUT, SQL_C_TYPE_DATE,
	                  SQL_TYPE_DATE, 0, 0, dt_vals, 0, dt_ind);
	EXECUTE_AND_CHECK("SQLBindParameter (TIMESTAMP)", hstmt, SQLBindParameter, hstmt, 5, SQL_PARAM_INPUT,
	                  SQL_C_TYPE_TIMESTAMP, SQL_TYPE_TIMESTAMP, 0, 0, ts_vals, 0, ts_ind);
	EXECUTE_AND_CHECK("SQLBindParameter (VARCHAR)", hstmt, SQLBindParameter, hstmt, 6, SQL_PARAM_INPUT, SQL_C_CHAR,
	                  SQL_VARCHAR, 8, 0, s_vals, 8, s_ind);

	EXECUTE_AND_CHECK("SQLExecDirect (INSERT)", hstmt, SQLExecDirect, hstmt,
	                  ConvertToSQLCHAR("INSERT INTO param_array_fixed VALUES (?, ?, ?, ?, ?, ?)"), SQL_NTS);
	REQUIRE(processed == set_count);

	EXECUTE_AND_CHECK("SQLFreeStmt (SQL_RESET_PARAMS)", hstmt, SQLFreeStmt, hstmt, SQL_RESET_PARAMS);
	EXECUTE_AND_CHECK("SQLSetStmtAttr (SQL_ATTR_PARAMSET_SIZE)", hstmt, SQLSetStmtAttr, hstmt, SQL_ATTR_PARAMSET_SIZE,
	                  reinterpret_cast<SQLPOINTER>(static_cast<SQLULEN>(1)), 0);
	EXECUTE_AND_CHECK("SQLExecDirect (SELECT)", hstmt, SQLExecDirect, hstmt,
	                  ConvertToSQLCHAR("SELECT i, b, d, dt, ts, s FROM param_array_fixed ORDER BY rowid"), SQL_NTS);

	EXECUTE_AND_CHECK("SQLFetch", hstmt, SQLFetch, hstmt);
	DATA_CHECK(hstmt, 1, "10");
	DATA_CHECK(hstmt, 2, "10000000000");
	DATA_CHECK(hstmt, 3, "1.5");
	DATA_CHECK(hstmt, 4, "2024-01-01");
	DATA_CHECK(hstmt, 5, "2024-01-01 01:02:03");
	DATA_CHECK(hstmt, 6, "a");

	EXECUTE_AND_CHECK("SQLFetch", hstmt, SQLFetch, hstmt);
	DATA_CHECK(hstmt, 1, ""); // the NULL set
	DATA_CHECK(hstmt, 2, "20000000000");
	DATA_CHECK(hstmt, 3, "2.5");
	DATA_CHECK(hstmt, 4, "2024-02-29");
	DATA_CHECK(hstmt, 5, "2024-02-29 13:45:10");
	DATA_CHECK(hstmt, 6, "bb");

	EXECUTE_AND_CHECK("SQLFetch", hstmt, SQLFetch, hstmt);
	DATA_CHECK(hstmt, 1, "30");
	DATA_CHECK(hstmt, 2, "30000000000");
	DATA_CHECK(hstmt, 3, "3.5");
	DATA_CHECK(hstmt, 4, "2024-12-31");
	DATA_CHECK(hstmt, 5, "2024-12-31 23:59:59");
	DATA_CHECK(hstmt, 6, "ccc");

	EXECUTE_AND_CHECK("SQLFreeStmt (HSTMT)", hstmt, SQLFreeStmt, hstmt, SQL_CLOSE);
	EXECUTE_AND_CHECK("SQLFreeHandle (HSTMT)", hstmt, SQLFreeHandle, SQL_HANDLE_STMT, hstmt);
	DISCONNECT_FROM_DATABASE(env, dbc);
}
