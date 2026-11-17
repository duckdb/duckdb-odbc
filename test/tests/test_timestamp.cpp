#include "odbc_test_common.h"

#include <cstdint>
#include <ctime>
#include <iomanip>
#include <mutex>
#include <sstream>
#include <vector>

using namespace odbc_test;

TEST_CASE("Test SQLBindParameter with TIMESTAMP type", "[odbc]") {
	SQLHANDLE env;
	SQLHANDLE dbc;

	HSTMT hstmt = SQL_NULL_HSTMT;

	// Connect to the database using SQLConnect
	CONNECT_TO_DATABASE(env, dbc);

	// Allocate a statement handle
	EXECUTE_AND_CHECK("SQLAllocHandle (HSTMT)", hstmt, SQLAllocHandle, SQL_HANDLE_STMT, dbc, &hstmt);

	// Create table
	EXECUTE_AND_CHECK("SQLExecDirect", hstmt, SQLExecDirect, hstmt,
	                  ConvertToSQLCHAR("CREATE TABLE timestamp_bind_test (col1 INT, col2 TIMESTAMP)"), SQL_NTS);

	// Insert literal value
	EXECUTE_AND_CHECK(
	    "SQLExecDirect", hstmt, SQLExecDirect, hstmt,
	    ConvertToSQLCHAR("INSERT INTO timestamp_bind_test VALUES(41, '2000-01-01 12:10:30.123456'::TIMESTAMP)"),
	    SQL_NTS);
	// Fetch and check
	EXECUTE_AND_CHECK("SQLExecDirect", hstmt, SQLExecDirect, hstmt,
	                  ConvertToSQLCHAR("SELECT col2 FROM timestamp_bind_test WHERE col1 = 41"), SQL_NTS);
	EXECUTE_AND_CHECK("SQLFetch", hstmt, SQLFetch, hstmt);
	{
		SQL_TIMESTAMP_STRUCT fetched;
		EXECUTE_AND_CHECK("SQLGetData", hstmt, SQLGetData, hstmt, 1, SQL_C_TYPE_TIMESTAMP, &fetched, sizeof(fetched),
		                  nullptr);
		REQUIRE(fetched.year == 2000);
		REQUIRE(fetched.month == 1);
		REQUIRE(fetched.day == 1);
		REQUIRE(fetched.hour == 12);
		REQUIRE(fetched.minute == 10);
		REQUIRE(fetched.second == 30);
		REQUIRE(fetched.fraction == 123456000);
	}

	// Insert parametrized value
	EXECUTE_AND_CHECK("SQLPrepare", hstmt, SQLPrepare, hstmt,
	                  ConvertToSQLCHAR("INSERT INTO timestamp_bind_test VALUES (42, ?)"), SQL_NTS);
	SQL_TIMESTAMP_STRUCT param = {2000, 2, 2, 12, 10, 30, 123456789};
	SQLLEN param_len = sizeof(param);
	EXECUTE_AND_CHECK("SQLBindParameter", hstmt, SQLBindParameter, hstmt, 1, SQL_PARAM_INPUT, SQL_C_TYPE_TIMESTAMP,
	                  SQL_TYPE_TIMESTAMP, 0, 0, &param, param_len, &param_len);
	EXECUTE_AND_CHECK("SQLExecute", hstmt, SQLExecute, hstmt);

	// Fetch and check
	EXECUTE_AND_CHECK("SQLExecDirect", hstmt, SQLExecDirect, hstmt,
	                  ConvertToSQLCHAR("SELECT col2 FROM timestamp_bind_test WHERE col1 = 42"), SQL_NTS);
	EXECUTE_AND_CHECK("SQLFetch", hstmt, SQLFetch, hstmt);
	{
		SQL_TIMESTAMP_STRUCT fetched;
		EXECUTE_AND_CHECK("SQLGetData", hstmt, SQLGetData, hstmt, 1, SQL_C_TYPE_TIMESTAMP, &fetched, sizeof(fetched),
		                  nullptr);
		REQUIRE(fetched.year == param.year);
		REQUIRE(fetched.month == param.month);
		REQUIRE(fetched.day == param.day);
		REQUIRE(fetched.hour == param.hour);
		REQUIRE(fetched.minute == param.minute);
		REQUIRE(fetched.second == param.second);
		REQUIRE(fetched.fraction == 123456000);
	}

	// Free the statement handle
	EXECUTE_AND_CHECK("SQLFreeStmt (HSTMT)", hstmt, SQLFreeStmt, hstmt, SQL_CLOSE);
	EXECUTE_AND_CHECK("SQLFreeHandle (HSTMT)", hstmt, SQLFreeHandle, SQL_HANDLE_STMT, hstmt);

	DISCONNECT_FROM_DATABASE(env, dbc);
}

TEST_CASE("Test SQLBindParameter with DATE type", "[odbc]") {
	SQLHANDLE env;
	SQLHANDLE dbc;

	HSTMT hstmt = SQL_NULL_HSTMT;

	// Connect to the database using SQLConnect
	CONNECT_TO_DATABASE(env, dbc);

	// Allocate a statement handle
	EXECUTE_AND_CHECK("SQLAllocHandle (HSTMT)", hstmt, SQLAllocHandle, SQL_HANDLE_STMT, dbc, &hstmt);

	// Create table
	EXECUTE_AND_CHECK("SQLExecDirect", hstmt, SQLExecDirect, hstmt,
	                  ConvertToSQLCHAR("CREATE TABLE date_bind_test (col1 DATE)"), SQL_NTS);

	// Insert value
	EXECUTE_AND_CHECK("SQLPrepare", hstmt, SQLPrepare, hstmt, ConvertToSQLCHAR("INSERT INTO date_bind_test VALUES (?)"),
	                  SQL_NTS);
	SQL_DATE_STRUCT param = {2000, 1, 2};
	SQLLEN param_len = sizeof(param);
	EXECUTE_AND_CHECK("SQLBindParameter", hstmt, SQLBindParameter, hstmt, 1, SQL_PARAM_INPUT, SQL_C_TYPE_DATE,
	                  SQL_TYPE_DATE, 0, 0, &param, param_len, &param_len);
	EXECUTE_AND_CHECK("SQLExecute", hstmt, SQLExecute, hstmt);

	{ // Fetch and check as DATE
		EXECUTE_AND_CHECK("SQLExecDirect", hstmt, SQLExecDirect, hstmt,
		                  ConvertToSQLCHAR("SELECT * FROM date_bind_test"), SQL_NTS);
		EXECUTE_AND_CHECK("SQLFetch", hstmt, SQLFetch, hstmt);
		SQL_DATE_STRUCT fetched;
		EXECUTE_AND_CHECK("SQLGetData", hstmt, SQLGetData, hstmt, 1, SQL_C_TYPE_DATE, &fetched, sizeof(fetched),
		                  nullptr);
		REQUIRE(fetched.year == param.year);
		REQUIRE(fetched.month == param.month);
		REQUIRE(fetched.day == param.day);
	}

	{ // Fetch and check as TIMESTAMP
		EXECUTE_AND_CHECK("SQLExecDirect", hstmt, SQLExecDirect, hstmt,
		                  ConvertToSQLCHAR("SELECT * FROM date_bind_test"), SQL_NTS);
		EXECUTE_AND_CHECK("SQLFetch", hstmt, SQLFetch, hstmt);
		SQL_TIMESTAMP_STRUCT fetched;
		EXECUTE_AND_CHECK("SQLGetData", hstmt, SQLGetData, hstmt, 1, SQL_C_TYPE_TIMESTAMP, &fetched, sizeof(fetched),
		                  nullptr);
		REQUIRE(fetched.year == param.year);
		REQUIRE(fetched.month == param.month);
		REQUIRE(fetched.day == param.day);
		REQUIRE(fetched.hour == 0);
		REQUIRE(fetched.minute == 0);
		REQUIRE(fetched.second == 0);
		REQUIRE(fetched.fraction == 0);
	}

	// Free the statement handle
	EXECUTE_AND_CHECK("SQLFreeStmt (HSTMT)", hstmt, SQLFreeStmt, hstmt, SQL_CLOSE);
	EXECUTE_AND_CHECK("SQLFreeHandle (HSTMT)", hstmt, SQLFreeHandle, SQL_HANDLE_STMT, hstmt);

	DISCONNECT_FROM_DATABASE(env, dbc);
}

TEST_CASE("Test SQLBindParameter with TIME type", "[odbc]") {
	SQLHANDLE env;
	SQLHANDLE dbc;

	HSTMT hstmt = SQL_NULL_HSTMT;

	// Connect to the database using SQLConnect
	CONNECT_TO_DATABASE(env, dbc);

	// Allocate a statement handle
	EXECUTE_AND_CHECK("SQLAllocHandle (HSTMT)", hstmt, SQLAllocHandle, SQL_HANDLE_STMT, dbc, &hstmt);

	// Create table
	EXECUTE_AND_CHECK("SQLExecDirect", hstmt, SQLExecDirect, hstmt,
	                  ConvertToSQLCHAR("CREATE TABLE time_bind_test (col1 TIME)"), SQL_NTS);

	// Insert value
	EXECUTE_AND_CHECK("SQLPrepare", hstmt, SQLPrepare, hstmt, ConvertToSQLCHAR("INSERT INTO time_bind_test VALUES (?)"),
	                  SQL_NTS);
	SQL_TIME_STRUCT param = {12, 34, 56};
	SQLLEN param_len = sizeof(param);
	EXECUTE_AND_CHECK("SQLBindParameter", hstmt, SQLBindParameter, hstmt, 1, SQL_PARAM_INPUT, SQL_C_TYPE_TIME,
	                  SQL_TYPE_TIME, 0, 0, &param, param_len, &param_len);
	EXECUTE_AND_CHECK("SQLExecute", hstmt, SQLExecute, hstmt);

	{ // Fetch and check as TIME
		EXECUTE_AND_CHECK("SQLExecDirect", hstmt, SQLExecDirect, hstmt,
		                  ConvertToSQLCHAR("SELECT * FROM time_bind_test"), SQL_NTS);
		EXECUTE_AND_CHECK("SQLFetch", hstmt, SQLFetch, hstmt);
		SQL_TIME_STRUCT fetched;
		EXECUTE_AND_CHECK("SQLGetData", hstmt, SQLGetData, hstmt, 1, SQL_C_TYPE_TIME, &fetched, sizeof(fetched),
		                  nullptr);
		REQUIRE(fetched.hour == param.hour);
		REQUIRE(fetched.minute == param.minute);
		REQUIRE(fetched.second == param.second);
	}

	{ // Fetch and check as TIMESTAMP
		EXECUTE_AND_CHECK("SQLExecDirect", hstmt, SQLExecDirect, hstmt,
		                  ConvertToSQLCHAR("SELECT * FROM time_bind_test"), SQL_NTS);
		EXECUTE_AND_CHECK("SQLFetch", hstmt, SQLFetch, hstmt);
		SQL_TIMESTAMP_STRUCT fetched;
		EXECUTE_AND_CHECK("SQLGetData", hstmt, SQLGetData, hstmt, 1, SQL_C_TYPE_TIMESTAMP, &fetched, sizeof(fetched),
		                  nullptr);
		REQUIRE(fetched.year == 1970);
		REQUIRE(fetched.month == 1);
		REQUIRE(fetched.day == 1);
		REQUIRE(fetched.hour == param.hour);
		REQUIRE(fetched.minute == param.minute);
		REQUIRE(fetched.second == param.second);
		REQUIRE(fetched.fraction == 0);
	}

	// Free the statement handle
	EXECUTE_AND_CHECK("SQLFreeStmt (HSTMT)", hstmt, SQLFreeStmt, hstmt, SQL_CLOSE);
	EXECUTE_AND_CHECK("SQLFreeHandle (HSTMT)", hstmt, SQLFreeHandle, SQL_HANDLE_STMT, hstmt);

	DISCONNECT_FROM_DATABASE(env, dbc);
}

static std::mutex &get_static_mutex() {
	static std::mutex mtx;
	return mtx;
}

static int64_t os_utc_offset_seconds(const std::string &timestamp_st) {
	std::mutex &mtx = get_static_mutex();
	std::lock_guard<std::mutex> guard(mtx);
	std::tm input_time = {};
	std::istringstream ss(timestamp_st);
	ss >> std::get_time(&input_time, "%Y-%m-%d %H:%M:%S");
	std::time_t input_instant = std::mktime(&input_time);
	std::tm local_time = *std::localtime(&input_instant);
	local_time.tm_isdst = 0;
	std::tm utc_time = *std::gmtime(&input_instant);
	std::time_t local_instant = std::mktime(&local_time);
	std::time_t utc_instant = std::mktime(&utc_time);
	return static_cast<int64_t>(difftime(local_instant, utc_instant));
}

static std::tm sql_timestamp_to_tm(const SQL_TIMESTAMP_STRUCT &ts) {
	std::tm dt = {};
	dt.tm_year = ts.year - 1900;
	dt.tm_mon = ts.month - 1;
	dt.tm_mday = ts.day;
	dt.tm_hour = ts.hour;
	dt.tm_min = ts.minute;
	dt.tm_sec = ts.second;
	return dt;
}

static int64_t sql_timestamp_diff_seconds(const SQL_TIMESTAMP_STRUCT &ts1, const SQL_TIMESTAMP_STRUCT &ts2) {
	std::mutex &mtx = get_static_mutex();
	std::lock_guard<std::mutex> guard(mtx);
	std::tm tm1 = sql_timestamp_to_tm(ts1);
	std::tm tm2 = sql_timestamp_to_tm(ts2);
	std::time_t instant1 = std::mktime(&tm1);
	std::time_t instant2 = std::mktime(&tm2);
	return static_cast<int64_t>(std::difftime(instant1, instant2));
}

TEST_CASE("Test fetching TIMESTAMP_TZ values as TIMESTAMP", "[odbc]") {
	SQLHANDLE env;
	SQLHANDLE dbc;

	HSTMT hstmt = SQL_NULL_HSTMT;

	// Connect to the database using SQLConnect
	CONNECT_TO_DATABASE(env, dbc);

	// Allocate a statement handle
	EXECUTE_AND_CHECK("SQLAllocHandle (HSTMT)", hstmt, SQLAllocHandle, SQL_HANDLE_STMT, dbc, &hstmt);

	std::string tm_local_st = "2004-07-19 10:23:54";

	// Process this date as TIMESTAMP_TZ and fetch is as SQL_C_TYPE_TIMESTAMP,
	// return value must be converted to local using OS-configured time-zone
	EXECUTE_AND_CHECK("SQLExecDirect", hstmt, SQLExecDirect, hstmt,
	                  ConvertToSQLCHAR("SELECT '" + tm_local_st + "+00'::TIMESTAMP WITH TIME ZONE"), SQL_NTS);
	EXECUTE_AND_CHECK("SQLFetch", hstmt, SQLFetch, hstmt);
	SQL_TIMESTAMP_STRUCT fetched_timestamp_tz;
	EXECUTE_AND_CHECK("SQLGetData", hstmt, SQLGetData, hstmt, 1, SQL_C_TYPE_TIMESTAMP, &fetched_timestamp_tz,
	                  sizeof(fetched_timestamp_tz), nullptr);

	// Process the same date as TIMESTAMP, no time-zone conversion performed
	EXECUTE_AND_CHECK("SQLExecDirect", hstmt, SQLExecDirect, hstmt,
	                  ConvertToSQLCHAR("SELECT '" + tm_local_st + "'::TIMESTAMP WITHOUT TIME ZONE"), SQL_NTS);
	EXECUTE_AND_CHECK("SQLFetch", hstmt, SQLFetch, hstmt);
	SQL_TIMESTAMP_STRUCT fetched_timestamp;
	EXECUTE_AND_CHECK("SQLGetData", hstmt, SQLGetData, hstmt, 1, SQL_C_TYPE_TIMESTAMP, &fetched_timestamp,
	                  sizeof(fetched_timestamp), nullptr);
	REQUIRE(fetched_timestamp.year == 2004);
	REQUIRE(fetched_timestamp.month == 7);
	REQUIRE(fetched_timestamp.day == 19);
	REQUIRE(fetched_timestamp.hour == 10);
	REQUIRE(fetched_timestamp.minute == 23);
	REQUIRE(fetched_timestamp.second == 54);
	REQUIRE(fetched_timestamp.fraction == 0);

	// Get OS time zone offset and compare the fetched results
	int64_t os_utc_offset = os_utc_offset_seconds(tm_local_st);
	int64_t fetched_diff = sql_timestamp_diff_seconds(fetched_timestamp_tz, fetched_timestamp);
	REQUIRE(fetched_diff == os_utc_offset);

	// Free the statement handle
	EXECUTE_AND_CHECK("SQLFreeStmt (HSTMT)", hstmt, SQLFreeStmt, hstmt, SQL_CLOSE);
	EXECUTE_AND_CHECK("SQLFreeHandle (HSTMT)", hstmt, SQLFreeHandle, SQL_HANDLE_STMT, hstmt);

	DISCONNECT_FROM_DATABASE(env, dbc);
}

//! Binds the parameters of a prepared statement to param, executes it and fetches its row.
template <class T>
static SQLRETURN ExecuteWithParameter(HSTMT hstmt, const char *query, SQLSMALLINT c_type, SQLSMALLINT sql_type,
                                      T &param) {
	static SQLLEN param_len;
	param_len = sizeof(param);
	EXECUTE_AND_CHECK("SQLFreeStmt (SQL_CLOSE)", hstmt, SQLFreeStmt, hstmt, SQL_CLOSE);
	EXECUTE_AND_CHECK("SQLPrepare", hstmt, SQLPrepare, hstmt, ConvertToSQLCHAR(query), SQL_NTS);
	SQLSMALLINT param_count = 0;
	EXECUTE_AND_CHECK("SQLNumParams", hstmt, SQLNumParams, hstmt, &param_count);
	for (SQLUSMALLINT i = 1; i <= param_count; i++) {
		EXECUTE_AND_CHECK("SQLBindParameter", hstmt, SQLBindParameter, hstmt, i, SQL_PARAM_INPUT, c_type, sql_type, 0,
		                  0, &param, param_len, &param_len);
	}
	auto ret = SQLExecute(hstmt);
	if (SQL_SUCCEEDED(ret)) {
		EXECUTE_AND_CHECK("SQLFetch", hstmt, SQLFetch, hstmt);
	}
	return ret;
}

static std::string Diagnostic(HSTMT hstmt) {
	std::string state, message;
	ACCESS_DIAGNOSTIC(state, message, hstmt, SQL_HANDLE_STMT);
	return state;
}

TEST_CASE("Test DATE, TIME and TIMESTAMP conversions at the edges of their ranges", "[odbc]") {
	SQLHANDLE env;
	SQLHANDLE dbc;
	HSTMT hstmt = SQL_NULL_HSTMT;
	CONNECT_TO_DATABASE(env, dbc);
	EXECUTE_AND_CHECK("SQLAllocHandle (HSTMT)", hstmt, SQLAllocHandle, SQL_HANDLE_STMT, dbc, &hstmt);

	// TIMESTAMP parameters, as DuckDB prints them and fetched back
	struct TimestampCase {
		SQL_TIMESTAMP_STRUCT param;
		const char *expected;
	};
	std::vector<TimestampCase> timestamps = {
	    {{1970, 1, 1, 0, 0, 0, 0}, "1970-01-01 00:00:00"},
	    {{1969, 12, 31, 23, 59, 59, 999999000}, "1969-12-31 23:59:59.999999"},
	    {{2024, 2, 29, 12, 30, 0, 0}, "2024-02-29 12:30:00"},
	    {{1900, 3, 1, 1, 2, 3, 4000}, "1900-03-01 01:02:03.000004"},
	    {{1, 1, 1, 0, 0, 0, 0}, "0001-01-01 00:00:00"},
	    {{9999, 12, 31, 23, 59, 59, 999999000}, "9999-12-31 23:59:59.999999"},
	};
	for (auto &test : timestamps) {
		INFO(test.expected);
		auto ret = ExecuteWithParameter(hstmt, "SELECT ?::TIMESTAMP::VARCHAR, ?::TIMESTAMP", SQL_C_TYPE_TIMESTAMP,
		                                SQL_TYPE_TIMESTAMP, test.param);
		ODBC_CHECK(ret, "SQLExecute", hstmt);
		DATA_CHECK(hstmt, 1, test.expected);
		SQL_TIMESTAMP_STRUCT fetched;
		EXECUTE_AND_CHECK("SQLGetData", hstmt, SQLGetData, hstmt, 2, SQL_C_TYPE_TIMESTAMP, &fetched, sizeof(fetched),
		                  nullptr);
		REQUIRE(fetched.year == test.param.year);
		REQUIRE(fetched.month == test.param.month);
		REQUIRE(fetched.day == test.param.day);
		REQUIRE(fetched.hour == test.param.hour);
		REQUIRE(fetched.minute == test.param.minute);
		REQUIRE(fetched.second == test.param.second);
		REQUIRE(fetched.fraction == test.param.fraction);
	}

	// DATE parameters
	SQL_DATE_STRUCT leap_day = {2000, 2, 29};
	ODBC_CHECK(ExecuteWithParameter(hstmt, "SELECT ?::DATE::VARCHAR", SQL_C_TYPE_DATE, SQL_TYPE_DATE, leap_day),
	           "SQLExecute", hstmt);
	DATA_CHECK(hstmt, 1, "2000-02-29");

	// dates and times that do not exist
	std::vector<SQL_TIMESTAMP_STRUCT> invalid_timestamps = {
	    {2023, 2, 29, 0, 0, 0, 0}, {1900, 2, 29, 0, 0, 0, 0}, {2023, 13, 1, 0, 0, 0, 0}, {2023, 0, 1, 0, 0, 0, 0},
	    {2023, 4, 31, 0, 0, 0, 0}, {2023, 1, 1, 24, 0, 0, 0}, {2023, 1, 1, 0, 60, 0, 0}, {2023, 1, 1, 0, 0, 60, 0},
	};
	for (auto &param : invalid_timestamps) {
		INFO(param.year << "-" << param.month << "-" << param.day << " " << param.hour << ":" << param.minute << ":"
		                << param.second);
		REQUIRE(ExecuteWithParameter(hstmt, "SELECT ?::TIMESTAMP", SQL_C_TYPE_TIMESTAMP, SQL_TYPE_TIMESTAMP, param) ==
		        SQL_ERROR);
	}
	SQL_DATE_STRUCT invalid_date = {2023, 2, 29};
	REQUIRE(ExecuteWithParameter(hstmt, "SELECT ?::DATE", SQL_C_TYPE_DATE, SQL_TYPE_DATE, invalid_date) == SQL_ERROR);
	SQL_TIME_STRUCT invalid_time = {25, 0, 0};
	REQUIRE(ExecuteWithParameter(hstmt, "SELECT ?::TIME", SQL_C_TYPE_TIME, SQL_TYPE_TIME, invalid_time) == SQL_ERROR);

	// values before 1970 and before the common era, fetched as DATE, TIME and TIMESTAMP
	EXECUTE_AND_CHECK("SQLFreeStmt (SQL_CLOSE)", hstmt, SQLFreeStmt, hstmt, SQL_CLOSE);
	EXECUTE_AND_CHECK("SQLExecDirect", hstmt, SQLExecDirect, hstmt,
	                  ConvertToSQLCHAR("SELECT TIMESTAMP '1969-12-31 23:59:59.999999', DATE '0044-03-15 (BC)'"),
	                  SQL_NTS);
	EXECUTE_AND_CHECK("SQLFetch", hstmt, SQLFetch, hstmt);
	{
		SQL_DATE_STRUCT date;
		EXECUTE_AND_CHECK("SQLGetData", hstmt, SQLGetData, hstmt, 1, SQL_C_TYPE_DATE, &date, sizeof(date), nullptr);
		REQUIRE(date.year == 1969);
		REQUIRE(date.month == 12);
		REQUIRE(date.day == 31);
		SQL_TIME_STRUCT time;
		EXECUTE_AND_CHECK("SQLGetData", hstmt, SQLGetData, hstmt, 1, SQL_C_TYPE_TIME, &time, sizeof(time), nullptr);
		REQUIRE(time.hour == 23);
		REQUIRE(time.minute == 59);
		REQUIRE(time.second == 59);
		EXECUTE_AND_CHECK("SQLGetData", hstmt, SQLGetData, hstmt, 2, SQL_C_TYPE_DATE, &date, sizeof(date), nullptr);
		REQUIRE(date.year == -43);
		REQUIRE(date.month == 3);
		REQUIRE(date.day == 15);
		SQL_TIMESTAMP_STRUCT timestamp;
		EXECUTE_AND_CHECK("SQLGetData", hstmt, SQLGetData, hstmt, 2, SQL_C_TYPE_TIMESTAMP, &timestamp,
		                  sizeof(timestamp), nullptr);
		REQUIRE(timestamp.year == -43);
		REQUIRE(timestamp.month == 3);
		REQUIRE(timestamp.day == 15);
		REQUIRE(timestamp.hour == 0);
	}

	// infinite values have no date and time parts
	EXECUTE_AND_CHECK("SQLFreeStmt (SQL_CLOSE)", hstmt, SQLFreeStmt, hstmt, SQL_CLOSE);
	EXECUTE_AND_CHECK("SQLExecDirect", hstmt, SQLExecDirect, hstmt,
	                  ConvertToSQLCHAR("SELECT 'infinity'::TIMESTAMP, '-infinity'::DATE"), SQL_NTS);
	EXECUTE_AND_CHECK("SQLFetch", hstmt, SQLFetch, hstmt);
	for (SQLUSMALLINT col = 1; col <= 2; col++) {
		SQL_TIMESTAMP_STRUCT timestamp;
		REQUIRE(SQLGetData(hstmt, col, SQL_C_TYPE_TIMESTAMP, &timestamp, sizeof(timestamp), nullptr) == SQL_ERROR);
		REQUIRE(Diagnostic(hstmt) == "22007");
	}

	EXECUTE_AND_CHECK("SQLFreeStmt (HSTMT)", hstmt, SQLFreeStmt, hstmt, SQL_CLOSE);
	EXECUTE_AND_CHECK("SQLFreeHandle (HSTMT)", hstmt, SQLFreeHandle, SQL_HANDLE_STMT, hstmt);
	DISCONNECT_FROM_DATABASE(env, dbc);
}
