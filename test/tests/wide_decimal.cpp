#include "odbc_test_common.h"

#include <vector>

using namespace odbc_test;

// DECIMAL types wider than 38 digits: the engine stores them as a BIGNUM instead of a fixed-size integer, and binds
// them by default (the sum of two DECIMAL(38,2) is a DECIMAL(39,2)). The widths below cross the boundaries that
// matter to the driver: just past the 128-bit tier, the 256-bit range, the last width that fits a byte, and one
// that does not.

namespace {

struct WideType {
	uint32_t width;
	uint32_t scale;
};

const WideType WIDE_TYPES[] = {{39, 2}, {76, 10}, {255, 0}, {300, 150}};

std::string TypeName(const WideType &type) {
	return "DECIMAL(" + std::to_string(type.width) + "," + std::to_string(type.scale) + ")";
}

// A value that uses every digit of the type, e.g. 1234567.89 for DECIMAL(9,2)
std::string FullWidthValue(const WideType &type, bool negative) {
	std::string result = negative ? "-" : "";
	for (uint32_t i = 0; i < type.width; i++) {
		if (i == type.width - type.scale) {
			result += i == 0 ? "0." : ".";
		}
		result += static_cast<char>('1' + i % 9);
	}
	return result;
}

std::string FetchChar(HSTMT &hstmt, SQLUSMALLINT col) {
	std::vector<SQLCHAR> content(1024);
	SQLLEN content_len = 0;
	EXECUTE_AND_CHECK("SQLGetData (SQL_C_CHAR)", hstmt, SQLGetData, hstmt, col, SQL_C_CHAR, content.data(),
	                  static_cast<SQLLEN>(content.size()), &content_len);
	if (content_len == SQL_NULL_DATA) {
		return "NULL";
	}
	REQUIRE(content_len == static_cast<SQLLEN>(strlen(reinterpret_cast<char *>(content.data()))));
	return ConvertToString(content.data());
}

std::string FetchWChar(HSTMT &hstmt, SQLUSMALLINT col) {
	std::vector<SQLWCHAR> content(1024);
	SQLLEN content_len = 0;
	EXECUTE_AND_CHECK("SQLGetData (SQL_C_WCHAR)", hstmt, SQLGetData, hstmt, col, SQL_C_WCHAR, content.data(),
	                  static_cast<SQLLEN>(content.size() * sizeof(SQLWCHAR)), &content_len);
	if (content_len == SQL_NULL_DATA) {
		return "NULL";
	}
	return ConvertToString(content.data());
}

// Every SQLSTATE in the diagnostic records of the statement
std::vector<std::string> DiagnosticStates(HSTMT &hstmt) {
	std::vector<std::string> states;
	for (SQLSMALLINT rec_num = 1;; rec_num++) {
		SQLCHAR sqlstate[6];
		SQLINTEGER native_error;
		SQLCHAR message[512];
		SQLSMALLINT message_len;
		auto ret = SQLGetDiagRec(SQL_HANDLE_STMT, hstmt, rec_num, sqlstate, &native_error, message, sizeof(message),
		                         &message_len);
		if (!SQL_SUCCEEDED(ret)) {
			break;
		}
		states.push_back(ConvertToString(sqlstate));
	}
	return states;
}

bool HasState(HSTMT &hstmt, const std::string &state) {
	for (auto &entry : DiagnosticStates(hstmt)) {
		if (entry == state) {
			return true;
		}
	}
	return false;
}

void ExecDirect(HSTMT &hstmt, const std::string &query) {
	EXECUTE_AND_CHECK("SQLExecDirect", hstmt, SQLExecDirect, hstmt, ConvertToSQLCHAR(query), SQL_NTS);
}

void CloseCursor(HSTMT &hstmt) {
	EXECUTE_AND_CHECK("SQLFreeStmt (SQL_CLOSE)", hstmt, SQLFreeStmt, hstmt, SQL_CLOSE);
}

// The unscaled value of a SQL_NUMERIC_STRUCT as decimal digits, with the sign
std::string NumericToString(const SQL_NUMERIC_STRUCT &numeric) {
	unsigned char val[SQL_MAX_NUMERIC_LEN];
	memcpy(val, numeric.val, SQL_MAX_NUMERIC_LEN);
	std::string digits;
	while (true) {
		// divide the little-endian magnitude by 10
		unsigned int remainder = 0;
		bool nonzero = false;
		for (int i = SQL_MAX_NUMERIC_LEN - 1; i >= 0; i--) {
			unsigned int cur = (remainder << 8) | val[i];
			val[i] = static_cast<unsigned char>(cur / 10);
			remainder = cur % 10;
			nonzero = nonzero || val[i] != 0;
		}
		digits.insert(digits.begin(), static_cast<char>('0' + remainder));
		if (!nonzero) {
			break;
		}
	}
	return (numeric.sign == 0 ? "-" : "") + digits;
}

} // namespace

TEST_CASE("Test wide DECIMAL column metadata", "[odbc][wide_decimal]") {
	SQLHANDLE env;
	SQLHANDLE dbc;
	HSTMT hstmt = SQL_NULL_HSTMT;
	CONNECT_TO_DATABASE(env, dbc);
	EXECUTE_AND_CHECK("SQLAllocHandle (HSTMT)", hstmt, SQLAllocHandle, SQL_HANDLE_STMT, dbc, &hstmt);

	std::string query = "SELECT ";
	for (auto &type : WIDE_TYPES) {
		query += "NULL::" + TypeName(type) + " AS d" + std::to_string(type.width) + ", ";
	}
	// decimal arithmetic widens past 38 digits by default
	query += "999999999999999999999999999999999999.99::DECIMAL(38,2) + 1::DECIMAL(38,2) AS s";
	ExecDirect(hstmt, query);

	SQLSMALLINT num_cols;
	EXECUTE_AND_CHECK("SQLNumResultCols", hstmt, SQLNumResultCols, hstmt, &num_cols);
	REQUIRE(num_cols == 5);

	std::vector<WideType> expected(std::begin(WIDE_TYPES), std::end(WIDE_TYPES));
	expected.push_back({39, 2});
	for (SQLUSMALLINT col = 1; col <= expected.size(); col++) {
		auto &type = expected[col - 1];

		SQLCHAR col_name[64];
		SQLSMALLINT col_name_len;
		SQLSMALLINT col_type;
		SQLULEN col_size;
		SQLSMALLINT col_decimal_digits;
		SQLSMALLINT col_nullable;
		EXECUTE_AND_CHECK("SQLDescribeCol", hstmt, SQLDescribeCol, hstmt, col, col_name, sizeof(col_name),
		                  &col_name_len, &col_type, &col_size, &col_decimal_digits, &col_nullable);
		REQUIRE(col_type == SQL_DECIMAL);
		REQUIRE(col_size == type.width);
		REQUIRE(col_decimal_digits == static_cast<SQLSMALLINT>(type.scale));

		SQLLEN attr;
		EXECUTE_AND_CHECK("SQLColAttribute (TYPE)", hstmt, SQLColAttribute, hstmt, col, SQL_DESC_TYPE, nullptr, 0,
		                  nullptr, &attr);
		REQUIRE(attr == SQL_DECIMAL);
		EXECUTE_AND_CHECK("SQLColAttribute (PRECISION)", hstmt, SQLColAttribute, hstmt, col, SQL_DESC_PRECISION,
		                  nullptr, 0, nullptr, &attr);
		REQUIRE(attr == static_cast<SQLLEN>(type.width));
		EXECUTE_AND_CHECK("SQLColAttribute (SCALE)", hstmt, SQLColAttribute, hstmt, col, SQL_DESC_SCALE, nullptr, 0,
		                  nullptr, &attr);
		REQUIRE(attr == static_cast<SQLLEN>(type.scale));
		// the digits, a sign and a decimal point
		EXECUTE_AND_CHECK("SQLColAttribute (DISPLAY_SIZE)", hstmt, SQLColAttribute, hstmt, col, SQL_DESC_DISPLAY_SIZE,
		                  nullptr, 0, nullptr, &attr);
		REQUIRE(attr == static_cast<SQLLEN>(type.width + 2));

		// the type name is the storage of the digits, which is not the 128-bit integer of the widest narrow tier
		SQLCHAR type_name[64];
		SQLSMALLINT type_name_len;
		EXECUTE_AND_CHECK("SQLColAttribute (TYPE_NAME)", hstmt, SQLColAttribute, hstmt, col, SQL_DESC_TYPE_NAME,
		                  type_name, sizeof(type_name), &type_name_len, nullptr);
		REQUIRE(ConvertToString(type_name) == "BIGNUM");
	}
	CloseCursor(hstmt);

	// the narrow tiers are unchanged
	ExecDirect(hstmt, "SELECT 1::DECIMAL(38,2)");
	SQLCHAR type_name[64];
	SQLSMALLINT type_name_len;
	EXECUTE_AND_CHECK("SQLColAttribute (TYPE_NAME)", hstmt, SQLColAttribute, hstmt, 1, SQL_DESC_TYPE_NAME, type_name,
	                  sizeof(type_name), &type_name_len, nullptr);
	REQUIRE(ConvertToString(type_name) == "INT128");
	CloseCursor(hstmt);

	// a table column, through SQLColumns
	ExecDirect(hstmt, "CREATE OR REPLACE TABLE wide_decimal_meta (d DECIMAL(300,150))");
	EXECUTE_AND_CHECK("SQLColumns", hstmt, SQLColumns, hstmt, nullptr, 0, nullptr, 0,
	                  ConvertToSQLCHAR("wide_decimal_meta"), SQL_NTS, nullptr, 0);
	EXECUTE_AND_CHECK("SQLFetch", hstmt, SQLFetch, hstmt);
	REQUIRE(FetchChar(hstmt, 4) == "d");
	REQUIRE(FetchChar(hstmt, 5) == std::to_string(SQL_NUMERIC));
	REQUIRE(FetchChar(hstmt, 7) == "300");
	REQUIRE(FetchChar(hstmt, 9) == "150");
	CloseCursor(hstmt);
	ExecDirect(hstmt, "DROP TABLE wide_decimal_meta");

	EXECUTE_AND_CHECK("SQLFreeHandle (HSTMT)", hstmt, SQLFreeHandle, SQL_HANDLE_STMT, hstmt);
	DISCONNECT_FROM_DATABASE(env, dbc);
}

TEST_CASE("Test fetching wide DECIMAL as characters", "[odbc][wide_decimal]") {
	SQLHANDLE env;
	SQLHANDLE dbc;
	HSTMT hstmt = SQL_NULL_HSTMT;
	CONNECT_TO_DATABASE(env, dbc);
	EXECUTE_AND_CHECK("SQLAllocHandle (HSTMT)", hstmt, SQLAllocHandle, SQL_HANDLE_STMT, dbc, &hstmt);

	for (auto &type : WIDE_TYPES) {
		for (auto negative : {false, true}) {
			auto value = FullWidthValue(type, negative);
			ExecDirect(hstmt, "SELECT '" + value + "'::" + TypeName(type) + ", NULL::" + TypeName(type));
			EXECUTE_AND_CHECK("SQLFetch", hstmt, SQLFetch, hstmt);
			REQUIRE(FetchChar(hstmt, 1) == value);
			REQUIRE(FetchChar(hstmt, 2) == "NULL");
			CloseCursor(hstmt);

			ExecDirect(hstmt, "SELECT '" + value + "'::" + TypeName(type) + ", NULL::" + TypeName(type));
			EXECUTE_AND_CHECK("SQLFetch", hstmt, SQLFetch, hstmt);
			REQUIRE(FetchWChar(hstmt, 1) == value);
			REQUIRE(FetchWChar(hstmt, 2) == "NULL");
			CloseCursor(hstmt);
		}
	}

	// the result of arithmetic that no longer fits 38 digits
	ExecDirect(hstmt, "SELECT 999999999999999999999999999999999999.99::DECIMAL(38,2) + 0.01::DECIMAL(38,2)");
	EXECUTE_AND_CHECK("SQLFetch", hstmt, SQLFetch, hstmt);
	REQUIRE(FetchChar(hstmt, 1) == "1000000000000000000000000000000000000.00");
	CloseCursor(hstmt);

	// bound columns
	WideType type {300, 150};
	auto value = FullWidthValue(type, true);
	ExecDirect(hstmt, "SELECT '" + value + "'::" + TypeName(type));
	std::vector<SQLCHAR> content(1024);
	SQLLEN content_len = 0;
	EXECUTE_AND_CHECK("SQLBindCol", hstmt, SQLBindCol, hstmt, 1, SQL_C_CHAR, content.data(),
	                  static_cast<SQLLEN>(content.size()), &content_len);
	EXECUTE_AND_CHECK("SQLFetch", hstmt, SQLFetch, hstmt);
	REQUIRE(content_len == static_cast<SQLLEN>(value.size()));
	REQUIRE(ConvertToString(content.data()) == value);
	EXECUTE_AND_CHECK("SQLFreeStmt (SQL_UNBIND)", hstmt, SQLFreeStmt, hstmt, SQL_UNBIND);
	CloseCursor(hstmt);

	EXECUTE_AND_CHECK("SQLFreeHandle (HSTMT)", hstmt, SQLFreeHandle, SQL_HANDLE_STMT, hstmt);
	DISCONNECT_FROM_DATABASE(env, dbc);
}

// Fetches a value of the type as SQL_C_NUMERIC; the value fits the 38 digits of SQL_NUMERIC_STRUCT
static void CheckNumericFits(HSTMT &hstmt, const std::string &value, const WideType &type,
                             const std::string &expected_unscaled, int expected_scale) {
	ExecDirect(hstmt, "SELECT '" + value + "'::" + TypeName(type));
	EXECUTE_AND_CHECK("SQLFetch", hstmt, SQLFetch, hstmt);
	SQL_NUMERIC_STRUCT numeric;
	memset(&numeric, 0xAB, sizeof(numeric));
	SQLLEN numeric_len = 0;
	EXECUTE_AND_CHECK("SQLGetData (SQL_C_NUMERIC)", hstmt, SQLGetData, hstmt, 1, SQL_C_NUMERIC, &numeric,
	                  sizeof(numeric), &numeric_len);
	REQUIRE(numeric_len == sizeof(SQL_NUMERIC_STRUCT));
	REQUIRE(NumericToString(numeric) == expected_unscaled);
	REQUIRE(static_cast<int>(numeric.scale) == expected_scale);
	REQUIRE(static_cast<int>(numeric.precision) == 38);
	CloseCursor(hstmt);
}

// Fetches a value of the type as SQL_C_NUMERIC; the value does not fit SQL_NUMERIC_STRUCT
static void CheckNumericOutOfRange(HSTMT &hstmt, const std::string &value, const WideType &type) {
	ExecDirect(hstmt, "SELECT '" + value + "'::" + TypeName(type));
	EXECUTE_AND_CHECK("SQLFetch", hstmt, SQLFetch, hstmt);
	SQL_NUMERIC_STRUCT numeric;
	memset(&numeric, 0, sizeof(numeric));
	SQLLEN numeric_len = 0;
	auto ret = SQLGetData(hstmt, 1, SQL_C_NUMERIC, &numeric, sizeof(numeric), &numeric_len);
	REQUIRE(ret == SQL_ERROR);
	std::string state;
	std::string message;
	ACCESS_DIAGNOSTIC(state, message, hstmt, SQL_HANDLE_STMT);
	REQUIRE(state == "22003");
	// the value is still there for a type that holds it (with the zeros that pad the fraction to the scale)
	REQUIRE(FetchChar(hstmt, 1).rfind(value, 0) == 0);
	CloseCursor(hstmt);
}

TEST_CASE("Test fetching wide DECIMAL as SQL_C_NUMERIC", "[odbc][wide_decimal]") {
	SQLHANDLE env;
	SQLHANDLE dbc;
	HSTMT hstmt = SQL_NULL_HSTMT;
	CONNECT_TO_DATABASE(env, dbc);
	EXECUTE_AND_CHECK("SQLAllocHandle (HSTMT)", hstmt, SQLAllocHandle, SQL_HANDLE_STMT, dbc, &hstmt);

	const std::string max38 = "99999999999999999999999999999999999999";

	// values that fit 38 digits at the scale of the column are returned at that scale
	CheckNumericFits(hstmt, "25.21", {39, 2}, "2521", 2);
	CheckNumericFits(hstmt, "-25.21", {39, 2}, "-2521", 2);
	CheckNumericFits(hstmt, "0", {39, 2}, "0", 2);
	CheckNumericFits(hstmt, "999999999999999999999999999999999999.99", {39, 2}, max38, 2);
	CheckNumericFits(hstmt, "-999999999999999999999999999999999999.99", {39, 2}, "-" + max38, 2);
	CheckNumericFits(hstmt, "1234567890.0123456789", {76, 10}, "12345678900123456789", 10);
	CheckNumericFits(hstmt, max38, {255, 0}, max38, 0);
	CheckNumericFits(hstmt, "-" + max38, {255, 0}, "-" + max38, 0);
	CheckNumericFits(hstmt, "12345678901234567890123456789012345678", {255, 0},
	                 "12345678901234567890123456789012345678", 0);
	// 39 digits at the scale of the column, 38 without the last zero of the fraction
	CheckNumericFits(hstmt, "1000000000000000000000000000000000000.00", {39, 2}, "1" + std::string(37, '0'), 1);
	CheckNumericFits(hstmt, "-1000000000000000000000000000000000000.00", {39, 2}, "-1" + std::string(37, '0'), 1);
	// a scale past 38 digits: trailing zeros of the fraction are dropped until the value fits, which keeps the value
	CheckNumericFits(hstmt, "1.5", {300, 150}, "15" + std::string(36, '0'), 37);
	CheckNumericFits(hstmt, "-12345.000001", {300, 150}, "-12345000001" + std::string(27, '0'), 33);
	CheckNumericFits(hstmt, "0", {300, 150}, "0", 38);
	CheckNumericFits(hstmt, "12345678901234567890123456789012345678", {300, 150},
	                 "12345678901234567890123456789012345678", 0);
	CheckNumericFits(hstmt, "0.00000000000000000000000000000000000001", {300, 150}, "1", 38);

	// values that need more than 38 digits are refused, never truncated
	CheckNumericOutOfRange(hstmt, "1234567890123456789012345678901234567.89", {39, 2});
	CheckNumericOutOfRange(hstmt, "1000000000000000000000000000000000000.01", {39, 2});
	CheckNumericOutOfRange(hstmt, "-1000000000000000000000000000000000000.01", {39, 2});
	CheckNumericOutOfRange(hstmt, "100000000000000000000000000000000000000", {255, 0});
	// 2^128 and 2^128 + 1, which wrap to 0 and 1 in 128 bits
	CheckNumericOutOfRange(hstmt, "340282366920938463463374607431768211456", {255, 0});
	CheckNumericOutOfRange(hstmt, "340282366920938463463374607431768211457", {255, 0});
	CheckNumericOutOfRange(hstmt, "0.000000000000000000000000000000000000001", {300, 150});
	CheckNumericOutOfRange(hstmt, "123456789012345678901234567890123456789", {300, 150});
	CheckNumericOutOfRange(hstmt, "1.000000000000000000000000000000000000001", {300, 150});
	for (auto &type : WIDE_TYPES) {
		CheckNumericOutOfRange(hstmt, FullWidthValue(type, false), type);
		CheckNumericOutOfRange(hstmt, FullWidthValue(type, true), type);
	}

	// NULL
	ExecDirect(hstmt, "SELECT NULL::DECIMAL(300,150)");
	EXECUTE_AND_CHECK("SQLFetch", hstmt, SQLFetch, hstmt);
	SQL_NUMERIC_STRUCT numeric;
	SQLLEN numeric_len = 0;
	EXECUTE_AND_CHECK("SQLGetData (SQL_C_NUMERIC)", hstmt, SQLGetData, hstmt, 1, SQL_C_NUMERIC, &numeric,
	                  sizeof(numeric), &numeric_len);
	REQUIRE(numeric_len == SQL_NULL_DATA);
	CloseCursor(hstmt);

	// a conversion the application asks for is the engine's cast
	ExecDirect(hstmt, "SELECT '1.5'::DECIMAL(300,150)");
	EXECUTE_AND_CHECK("SQLFetch", hstmt, SQLFetch, hstmt);
	SQLDOUBLE double_val = 0;
	EXECUTE_AND_CHECK("SQLGetData (SQL_C_DOUBLE)", hstmt, SQLGetData, hstmt, 1, SQL_C_DOUBLE, &double_val,
	                  sizeof(double_val), nullptr);
	REQUIRE(double_val == 1.5);
	CloseCursor(hstmt);

	EXECUTE_AND_CHECK("SQLFreeHandle (HSTMT)", hstmt, SQLFreeHandle, SQL_HANDLE_STMT, hstmt);
	DISCONNECT_FROM_DATABASE(env, dbc);
}

TEST_CASE("Test binding parameters to wide DECIMAL", "[odbc][wide_decimal]") {
	SQLHANDLE env;
	SQLHANDLE dbc;
	HSTMT hstmt = SQL_NULL_HSTMT;
	CONNECT_TO_DATABASE(env, dbc);
	EXECUTE_AND_CHECK("SQLAllocHandle (HSTMT)", hstmt, SQLAllocHandle, SQL_HANDLE_STMT, dbc, &hstmt);

	// character parameters, declared as SQL_DECIMAL and as SQL_VARCHAR
	for (auto &type : WIDE_TYPES) {
		for (auto sql_type : {SQL_DECIMAL, SQL_NUMERIC, SQL_VARCHAR}) {
			for (auto negative : {false, true}) {
				auto value = FullWidthValue(type, negative);
				EXECUTE_AND_CHECK(
				    "SQLPrepare", hstmt, SQLPrepare, hstmt,
				    ConvertToSQLCHAR("SELECT ?::" + TypeName(type) + ", typeof(?::" + TypeName(type) + ")"), SQL_NTS);
				SQLLEN value_len = SQL_NTS;
				SQLLEN value_len2 = SQL_NTS;
				EXECUTE_AND_CHECK("SQLBindParameter", hstmt, SQLBindParameter, hstmt, 1, SQL_PARAM_INPUT, SQL_C_CHAR,
				                  static_cast<SQLSMALLINT>(sql_type), type.width, static_cast<SQLSMALLINT>(type.scale),
				                  ConvertToSQLCHAR(value), static_cast<SQLLEN>(value.size() + 1), &value_len);
				EXECUTE_AND_CHECK("SQLBindParameter", hstmt, SQLBindParameter, hstmt, 2, SQL_PARAM_INPUT, SQL_C_CHAR,
				                  static_cast<SQLSMALLINT>(sql_type), type.width, static_cast<SQLSMALLINT>(type.scale),
				                  ConvertToSQLCHAR(value), static_cast<SQLLEN>(value.size() + 1), &value_len2);
				EXECUTE_AND_CHECK("SQLExecute", hstmt, SQLExecute, hstmt);
				EXECUTE_AND_CHECK("SQLFetch", hstmt, SQLFetch, hstmt);
				REQUIRE(FetchChar(hstmt, 1) == value);
				REQUIRE(FetchChar(hstmt, 2) == TypeName(type));
				CloseCursor(hstmt);
				EXECUTE_AND_CHECK("SQLFreeStmt (SQL_RESET_PARAMS)", hstmt, SQLFreeStmt, hstmt, SQL_RESET_PARAMS);
			}
		}
	}

	// wide character parameters into a table
	ExecDirect(hstmt, "CREATE OR REPLACE TABLE wide_decimal_params (id INTEGER, d DECIMAL(300,150))");
	WideType type {300, 150};
	auto value = FullWidthValue(type, true);
	std::vector<SQLWCHAR> wide_value;
	for (auto c : value) {
		wide_value.push_back(static_cast<SQLWCHAR>(c));
	}
	wide_value.push_back(0);
	EXECUTE_AND_CHECK("SQLPrepare", hstmt, SQLPrepare, hstmt,
	                  ConvertToSQLCHAR("INSERT INTO wide_decimal_params VALUES (1, ?)"), SQL_NTS);
	SQLLEN wide_len = SQL_NTS;
	EXECUTE_AND_CHECK("SQLBindParameter", hstmt, SQLBindParameter, hstmt, 1, SQL_PARAM_INPUT, SQL_C_WCHAR, SQL_DECIMAL,
	                  type.width, static_cast<SQLSMALLINT>(type.scale), wide_value.data(),
	                  static_cast<SQLLEN>(wide_value.size() * sizeof(SQLWCHAR)), &wide_len);
	EXECUTE_AND_CHECK("SQLExecute", hstmt, SQLExecute, hstmt);
	EXECUTE_AND_CHECK("SQLFreeStmt (SQL_RESET_PARAMS)", hstmt, SQLFreeStmt, hstmt, SQL_RESET_PARAMS);
	ExecDirect(hstmt, "SELECT d FROM wide_decimal_params");
	EXECUTE_AND_CHECK("SQLFetch", hstmt, SQLFetch, hstmt);
	REQUIRE(FetchChar(hstmt, 1) == value);
	CloseCursor(hstmt);
	ExecDirect(hstmt, "DROP TABLE wide_decimal_params");

	// a SQL_NUMERIC_STRUCT parameter holds at most 38 digits, which a wide DECIMAL takes as they are
	SQL_NUMERIC_STRUCT numeric;
	memset(&numeric, 0, sizeof(numeric));
	numeric.sign = 0;
	numeric.precision = 38;
	numeric.scale = 2;
	// 12345678901234567890123456789012345678
	const unsigned char magnitude[] = {0x4E, 0xF3, 0x38, 0xDE, 0x50, 0x90, 0x49, 0xC4,
	                                   0x13, 0x33, 0x02, 0xF0, 0xF6, 0xB0, 0x49, 0x09};
	memcpy(numeric.val, magnitude, sizeof(magnitude));
	SQLLEN numeric_len = sizeof(numeric);
	EXECUTE_AND_CHECK("SQLPrepare", hstmt, SQLPrepare, hstmt,
	                  ConvertToSQLCHAR("SELECT ?::DECIMAL(76,10) * 1000000000000::DECIMAL(13,0)"), SQL_NTS);
	EXECUTE_AND_CHECK("SQLBindParameter", hstmt, SQLBindParameter, hstmt, 1, SQL_PARAM_INPUT, SQL_C_NUMERIC,
	                  SQL_NUMERIC, 38, 2, &numeric, sizeof(numeric), &numeric_len);
	EXECUTE_AND_CHECK("SQLExecute", hstmt, SQLExecute, hstmt);
	EXECUTE_AND_CHECK("SQLFetch", hstmt, SQLFetch, hstmt);
	REQUIRE(FetchChar(hstmt, 1) == "-123456789012345678901234567890123456780000000000.0000000000");
	CloseCursor(hstmt);
	EXECUTE_AND_CHECK("SQLFreeStmt (SQL_RESET_PARAMS)", hstmt, SQLFreeStmt, hstmt, SQL_RESET_PARAMS);

	// a SQL_NUMERIC_STRUCT parameter declared wider than the 38 digits the struct holds is refused with a diagnostic,
	// also when the width does not fit the 16 bits of the descriptor field
	for (SQLULEN precision : {39, 76, 255, 300, 65537}) {
		EXECUTE_AND_CHECK("SQLPrepare", hstmt, SQLPrepare, hstmt, ConvertToSQLCHAR("SELECT ?::DECIMAL(300,2)"),
		                  SQL_NTS);
		EXECUTE_AND_CHECK("SQLBindParameter", hstmt, SQLBindParameter, hstmt, 1, SQL_PARAM_INPUT, SQL_C_NUMERIC,
		                  SQL_NUMERIC, precision, 2, &numeric, sizeof(numeric), &numeric_len);
		auto ret = SQLExecute(hstmt);
		REQUIRE(ret == SQL_ERROR);
		REQUIRE(HasState(hstmt, "HY104"));
		EXECUTE_AND_CHECK("SQLFreeStmt (SQL_RESET_PARAMS)", hstmt, SQLFreeStmt, hstmt, SQL_RESET_PARAMS);
	}

	EXECUTE_AND_CHECK("SQLFreeHandle (HSTMT)", hstmt, SQLFreeHandle, SQL_HANDLE_STMT, hstmt);
	DISCONNECT_FROM_DATABASE(env, dbc);
}
