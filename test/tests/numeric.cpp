#include "odbc_test_common.h"

#include <cmath>

using namespace odbc_test;

static unsigned char HexToInt(char c) {
	if (c >= '0' && c <= '9') {
		return static_cast<unsigned char>(c - '0');
	} else if (c >= 'a' && c <= 'f') {
		return static_cast<unsigned char>(c - 'a' + 10);
	} else if (c >= 'A' && c <= 'F') {
		return static_cast<unsigned char>(c - 'A' + 10);
	} else {
		FAIL("invalid hex-encoded numeric value");
		return 0;
	}
}

static void BuildNumericStruct(SQL_NUMERIC_STRUCT *numeric, unsigned char sign, const char *hexval,
                               unsigned char precision, unsigned char scale) {
	memset(numeric, 0, sizeof(SQL_NUMERIC_STRUCT));
	numeric->sign = sign;
	numeric->precision = precision;
	numeric->scale = scale;

	// Convert hexval to binary
	int len = 0;
	while (*hexval) {
		if (*hexval == ' ') {
			hexval++;
			continue;
		}
		if (len >= SQL_MAX_NUMERIC_LEN) {
			FAIL("hex-encoded numeric value too long");
		}
		numeric->val[len] = HexToInt(*hexval) << 4 | HexToInt(*(hexval + 1));
		hexval += 2;
		len++;
	}
}

static void TestNumericParams(HSTMT &hstmt, unsigned char sign, const char *hexval, unsigned char precision,
                              unsigned char scale, const std::string &expected) {
	SQL_NUMERIC_STRUCT numeric;
	BuildNumericStruct(&numeric, sign, hexval, precision, scale);

	SQLLEN numeric_len = sizeof(numeric);
	EXECUTE_AND_CHECK("SQLBindParameter (numeric)", hstmt, SQLBindParameter, hstmt, 1, SQL_PARAM_INPUT, SQL_C_NUMERIC,
	                  SQL_NUMERIC, precision, scale, &numeric, numeric_len, &numeric_len);

	EXECUTE_AND_CHECK("SQLExecute", hstmt, SQLExecute, hstmt);

	EXECUTE_AND_CHECK("SQLFetch", hstmt, SQLFetch, hstmt);
	DATA_CHECK(hstmt, 1, expected);
	EXECUTE_AND_CHECK("SQLFreeStmt (HSTMT)", hstmt, SQLFreeStmt, hstmt, SQL_CLOSE);
}

static void TestNumericResult(HSTMT &hstmt, const char *num_str, const std::string &expected_result,
                              unsigned char expected_precision = 18, unsigned char expected_scale = 3,
                              unsigned int precision = 18, unsigned int scale = 3, unsigned int expected_sign = 1) {
	SQL_NUMERIC_STRUCT numeric;

	std::string query = "SELECT '" + std::string(num_str) + "'::numeric(" + std::to_string(precision) + "," +
	                    std::to_string(scale) + ")";
	EXECUTE_AND_CHECK("SQLExecDirect", hstmt, SQLExecDirect, hstmt, ConvertToSQLCHAR(query), SQL_NTS);

	EXECUTE_AND_CHECK("SQLFetch", hstmt, SQLFetch, hstmt);
	EXECUTE_AND_CHECK("SQLGetData", hstmt, SQLGetData, hstmt, 1, SQL_C_NUMERIC, &numeric, sizeof(numeric), nullptr);
	REQUIRE(numeric.precision == expected_precision);
	REQUIRE(numeric.scale == expected_scale);
	REQUIRE(numeric.sign == expected_sign);
	REQUIRE(ConvertHexToString(numeric.val, expected_result.length()) == expected_result);
}

TEST_CASE("Test numeric limits and conversion", "[odbc]") {
	SQLHANDLE env;
	SQLHANDLE dbc;

	HSTMT hstmt = SQL_NULL_HSTMT;

	// Connect to the database using SQLConnect
	CONNECT_TO_DATABASE(env, dbc);

	// Allocate a statement handle
	EXECUTE_AND_CHECK("SQLAllocHandle (HSTMT)", hstmt, SQLAllocHandle, SQL_HANDLE_STMT, dbc, &hstmt);

	// Test 25.212 with default precision and scale
	EXECUTE_AND_CHECK("SQLPrepare (?::numeric)", hstmt, SQLPrepare, hstmt, ConvertToSQLCHAR("SELECT ?::numeric"),
	                  SQL_NTS);
	TestNumericParams(hstmt, 1, "7C62", 5, 3, "25.212");

	// Test 0 (negative and positive) with precision 1 and scale 0
	EXECUTE_AND_CHECK("SQLPrepare (?::numeric(1,0))", hstmt, SQLPrepare, hstmt,
	                  ConvertToSQLCHAR("SELECT ?::numeric(1,0)"), SQL_NTS);
	TestNumericParams(hstmt, 1, "00", 1, 0, "0");
	TestNumericParams(hstmt, 0, "00", 1, 0, "0");

	// Test 1 (negative and positive) with precision 1 and scale 0
	EXECUTE_AND_CHECK("SQLPrepare (?::numeric(1,0))", hstmt, SQLPrepare, hstmt,
	                  ConvertToSQLCHAR("SELECT ?::numeric(1,0)"), SQL_NTS);
	TestNumericParams(hstmt, 1, "01", 1, 0, "1");
	TestNumericParams(hstmt, 0, "01", 1, 0, "-1");

	// Test 7.70 (negative and positive) with precision 3 and scale 2
	EXECUTE_AND_CHECK("SQLPrepare (?::numeric(3,2))", hstmt, SQLPrepare, hstmt,
	                  ConvertToSQLCHAR("SELECT ?::numeric(3,2)"), SQL_NTS);
	TestNumericParams(hstmt, 1, "0203", 3, 2, "7.70");
	TestNumericParams(hstmt, 0, "0203", 3, 2, "-7.70");

	// Test 12345678901234567890123456789012345678 with precision 38 and scale 0
	EXECUTE_AND_CHECK("SQLPrepare (?::numeric(38,0))", hstmt, SQLPrepare, hstmt,
	                  ConvertToSQLCHAR("SELECT ?::numeric(38,0)"), SQL_NTS);
	TestNumericParams(hstmt, 1, "4EF338DE509049C4133302F0F6B04909", 38, 0, "12345678901234567890123456789012345678");

	// Test -1234567890123456789.12345 with precision 24 and scale 5 (tests hugeint_t path with negative)
	EXECUTE_AND_CHECK("SQLPrepare (?::numeric(24,5))", hstmt, SQLPrepare, hstmt,
	                  ConvertToSQLCHAR("SELECT ?::numeric(24,5)"), SQL_NTS);
	TestNumericParams(hstmt, 0, "59DB966CA0101F9B241A", 24, 5, "-1234567890123456789.12345");

	// Test setting numeric struct within the application
	TestNumericResult(hstmt, "25.212", "7C62000000000000", 18, 3);
	TestNumericResult(hstmt, "24197857161011715162171839636988778104", "7856341278563412", 38, 0, 38, 0);
	TestNumericResult(hstmt, "12345678901234567890123456789012345678", "4EF338DE509049C4", 38, 0, 38, 0);
	TestNumericResult(hstmt, "-0", "0000000000000000", 18, 3);
	TestNumericResult(hstmt, "0", "0000000000000000", 18, 3);
	TestNumericResult(hstmt, "-1", "0100000000000000", 1, 0, 1, 0, 0);
	TestNumericResult(hstmt, "1", "0100000000000000", 1, 0, 1, 0);
	TestNumericResult(hstmt, "7.70", "0203000000000000", 3, 2, 3, 2);
	TestNumericResult(hstmt, "-7.70", "0203000000000000", 3, 2, 3, 2, 0);
	TestNumericResult(hstmt, "999999999999", "187CC6A47E8D0300", 18, 3);
	TestNumericResult(hstmt, "-1234567890123456789.12345", "59DB966CA0101F9B241A000000000000", 24, 5, 24, 5, 0);

	// Free the statement handle
	EXECUTE_AND_CHECK("SQLFreeStmt (HSTMT)", hstmt, SQLFreeStmt, hstmt, SQL_CLOSE);
	EXECUTE_AND_CHECK("SQLFreeHandle (HSTMT)", hstmt, SQLFreeHandle, SQL_HANDLE_STMT, hstmt);

	DISCONNECT_FROM_DATABASE(env, dbc);
}

TEST_CASE("Test SQL_C_NUMERIC result with zero fractional part", "[odbc]") {
	SQLHANDLE env;
	SQLHANDLE dbc;

	HSTMT hstmt = SQL_NULL_HSTMT;

	// Connect to the database using SQLConnect
	CONNECT_TO_DATABASE(env, dbc);

	// Allocate a statement handle
	EXECUTE_AND_CHECK("SQLAllocHandle (HSTMT)", hstmt, SQLAllocHandle, SQL_HANDLE_STMT, dbc, &hstmt);

	// val must hold the unscaled value at the column scale, trailing zeros must not be stripped
	TestNumericResult(hstmt, "60.00", "7017000000000000", 18, 2, 18, 2);
	TestNumericResult(hstmt, "-60.00", "7017000000000000", 18, 2, 18, 2, 0);
	TestNumericResult(hstmt, "1.00", "6400000000000000", 18, 2, 18, 2);
	TestNumericResult(hstmt, "100.00", "1027000000000000", 18, 2, 18, 2);
	TestNumericResult(hstmt, "0.60", "3C00000000000000", 18, 2, 18, 2);
	TestNumericResult(hstmt, "60.10", "7A17000000000000", 18, 2, 18, 2);
	TestNumericResult(hstmt, "12.34", "D204000000000000", 18, 2, 18, 2);
	TestNumericResult(hstmt, "30.0", "2C01000000000000", 18, 1, 18, 1);
	TestNumericResult(hstmt, "30", "3075000000000000", 18, 3);
	// DECIMAL backed by int16, int32 and hugeint
	TestNumericResult(hstmt, "60.00", "7017000000000000", 4, 2, 4, 2);
	TestNumericResult(hstmt, "60.00", "7017000000000000", 9, 2, 9, 2);
	TestNumericResult(hstmt, "100000000000000000000000.00", "0000004A48011416954508", 38, 2, 38, 2);
	TestNumericResult(hstmt, "-100000000000000000000000.00", "0000004A48011416954508", 38, 2, 38, 2, 0);

	// Same column fetched as SQL_C_CHAR and SQL_C_NUMERIC must decode to the same value
	EXECUTE_AND_CHECK("SQLExecDirect", hstmt, SQLExecDirect, hstmt,
	                  ConvertToSQLCHAR("SELECT d FROM (VALUES (60.00::DECIMAL(18,2)), (0.60), (60.10), (1.00), "
	                                   "(100.00), (12.34), (-60.00)) t(d)"),
	                  SQL_NTS);
	int rows = 0;
	while (SQLFetch(hstmt) == SQL_SUCCESS) {
		SQLCHAR char_val[64];
		EXECUTE_AND_CHECK("SQLGetData (SQL_C_CHAR)", hstmt, SQLGetData, hstmt, 1, SQL_C_CHAR, char_val,
		                  sizeof(char_val), nullptr);
		SQL_NUMERIC_STRUCT numeric;
		SQLLEN numeric_len = 0;
		EXECUTE_AND_CHECK("SQLGetData (SQL_C_NUMERIC)", hstmt, SQLGetData, hstmt, 1, SQL_C_NUMERIC, &numeric,
		                  sizeof(numeric), &numeric_len);
		REQUIRE(numeric_len == sizeof(SQL_NUMERIC_STRUCT));
		REQUIRE(numeric.precision == 18);
		REQUIRE(numeric.scale == 2);

		int64_t unscaled = 0;
		for (int i = sizeof(int64_t) - 1; i >= 0; i--) {
			unscaled = (unscaled << 8) | numeric.val[i];
		}
		if (numeric.sign == 0) {
			unscaled = -unscaled;
		}
		std::string char_str = ConvertToString(char_val);
		REQUIRE(unscaled == std::llround(std::stod(char_str) * 100));
		rows++;
	}
	REQUIRE(rows == 7);

	// Free the statement handle
	EXECUTE_AND_CHECK("SQLFreeStmt (HSTMT)", hstmt, SQLFreeStmt, hstmt, SQL_CLOSE);
	EXECUTE_AND_CHECK("SQLFreeHandle (HSTMT)", hstmt, SQLFreeHandle, SQL_HANDLE_STMT, hstmt);

	DISCONNECT_FROM_DATABASE(env, dbc);
}
