#include "odbc_test_common.h"

#include <vector>

using namespace odbc_test;

TEST_CASE("Test SQLGetDiagRec (returns diagnostic record)", "[odbc]") {
	SQLRETURN ret;
	SQLHANDLE env;
	SQLHANDLE dbc;

	HSTMT hstmt = SQL_NULL_HSTMT;

	// Connect to the database using SQLConnect
	CONNECT_TO_DATABASE(env, dbc);

	// Allocate a statement handle
	EXECUTE_AND_CHECK("SQLAllocHandle (HSTMT)", hstmt, SQLAllocHandle, SQL_HANDLE_STMT, dbc, &hstmt);

	/* TEST 1: Execute a query that will fail and check the diagnostics */
	// Execute a query that will fail
	ret = SQLExecDirect(hstmt, ConvertToSQLCHAR("this is not a valid query"), SQL_NTS);
	if (ret != SQL_ERROR) {
		FAIL("SQLExecDirect should have failed because the query is invalid");
	}

	// Get the diagnostics
	std::string first_state;
	std::string first_message;
	ACCESS_DIAGNOSTIC(first_state, first_message, hstmt, SQL_HANDLE_STMT);

	// Get the diagnostics again to make sure they are the same
	std::string second_state;
	std::string second_message;
	ACCESS_DIAGNOSTIC_WIDE(second_state, second_message, hstmt, SQL_HANDLE_STMT);

	// Compare the diagnostics to make sure they are the same and that SQLGetDiagRec does not change the state of the
	// statement
	REQUIRE(STR_EQUAL(first_state.c_str(), second_state.c_str()));
	REQUIRE(STR_EQUAL(first_message.c_str(), second_message.c_str()));

	/* TEST 2: Test a veeery long error message */
	// Create and fill a std::string with 1000 characters
	std::string long_string;
	for (int i = 0; i < 1000; i++) {
		long_string += "x";
	}
	long_string += "END";

	// Execute a query that will fail
	ret = SQLExecDirect(hstmt, ConvertToSQLCHAR(long_string.c_str()), SQL_NTS);
	if (ret != SQL_ERROR) {
		FAIL("SQLExecDirect should have failed because the query is invalid and too long");
	}

	// Get the diagnostics
	std::string long_state;
	std::string long_message;
	ACCESS_DIAGNOSTIC(long_state, long_message, hstmt, SQL_HANDLE_STMT);

	REQUIRE(long_message.length() > 0);
	REQUIRE(long_state == "42000");

	/* TEST 3: Test SQLEndTran without a transaction */
	ret = SQLEndTran(SQL_HANDLE_DBC, dbc, SQL_ROLLBACK);
	if (ret != SQL_ERROR) {
		FAIL("SQLEndTran should have failed because there is no transaction");
	}

	// Get the diagnostics
	std::string first_endtran_state;
	std::string first_endtran_message;
	ACCESS_DIAGNOSTIC(first_endtran_state, first_endtran_message, dbc, SQL_HANDLE_DBC);

	// Get the diagnostics again to make sure they are the same
	std::string second_endtran_state;
	std::string second_endtran_message;
	ACCESS_DIAGNOSTIC(second_endtran_state, second_endtran_message, dbc, SQL_HANDLE_DBC);

	// Compare the diagnostics to make sure they are the same and that SQLGetDiagRec does not change the state of the
	// statement
	REQUIRE(!first_endtran_state.compare(second_endtran_state));
	REQUIRE(!first_endtran_message.compare(second_endtran_message));

	// Check that TextLengthPtr is set to the actual length when the MessageText specified as NULL
	SQLSMALLINT text_length;
	ret = SQLGetDiagRec(SQL_HANDLE_DBC, dbc, 1, nullptr, nullptr, nullptr, 0, &text_length);
	REQUIRE(ret == SQL_SUCCESS_WITH_INFO);
	REQUIRE(first_endtran_message.length() == static_cast<size_t>(text_length));

	// Free the statement handle
	EXECUTE_AND_CHECK("SQLFreeStmt (HSTMT)", hstmt, SQLFreeStmt, hstmt, SQL_CLOSE);
	EXECUTE_AND_CHECK("SQLFreeHandle (HSTMT)", hstmt, SQLFreeHandle, SQL_HANDLE_STMT, hstmt);

	DISCONNECT_FROM_DATABASE(env, dbc);
}

TEST_CASE("Test SQLGetDiagRec with a truncated message", "[odbc]") {
	SQLRETURN ret;
	SQLHANDLE env;
	SQLHANDLE dbc;

	HSTMT hstmt = SQL_NULL_HSTMT;

	// Connect to the database using SQLConnect
	CONNECT_TO_DATABASE(env, dbc);

	// Allocate a statement handle
	EXECUTE_AND_CHECK("SQLAllocHandle (HSTMT)", hstmt, SQLAllocHandle, SQL_HANDLE_STMT, dbc, &hstmt);

	// The binder error lists all candidate functions, so the message is longer than SQL_MAX_MESSAGE_LENGTH
	ret = SQLExecDirect(hstmt, ConvertToSQLCHAR("SELECT DATE '1993-08-14' * 3"), SQL_NTS);
	if (ret != SQL_ERROR) {
		FAIL("SQLExecDirect should have failed because there is no matching function");
	}

	// Driver managers read the record with a fixed size buffer first, and read the same record again with a larger
	// buffer when the message is truncated. Reading a record must not create new records.
	SQLCHAR sqlstate[6];
	SQLINTEGER native_error;
	SQLSMALLINT text_length = 0;
	std::vector<SQLCHAR> small_buf(SQL_MAX_MESSAGE_LENGTH + 1);
	ret = SQLGetDiagRec(SQL_HANDLE_STMT, hstmt, 1, sqlstate, &native_error, small_buf.data(),
	                    static_cast<SQLSMALLINT>(small_buf.size()), &text_length);
	REQUIRE(ret == SQL_SUCCESS_WITH_INFO);
	REQUIRE(static_cast<size_t>(text_length) > SQL_MAX_MESSAGE_LENGTH);
	std::string truncated_message = ConvertToString(small_buf.data());
	REQUIRE(truncated_message.length() == SQL_MAX_MESSAGE_LENGTH);

	std::vector<SQLCHAR> full_buf(text_length + 1);
	SQLSMALLINT full_length = 0;
	ret = SQLGetDiagRec(SQL_HANDLE_STMT, hstmt, 1, sqlstate, &native_error, full_buf.data(),
	                    static_cast<SQLSMALLINT>(full_buf.size()), &full_length);
	REQUIRE(ret == SQL_SUCCESS);
	REQUIRE(full_length == text_length);
	std::string full_message = ConvertToString(full_buf.data());
	REQUIRE(full_message.length() == static_cast<size_t>(text_length));
	REQUIRE(full_message.compare(0, truncated_message.length(), truncated_message) == 0);
	REQUIRE(duckdb::StringUtil::Contains(full_message, "No function matches"));
	REQUIRE(ConvertToString(sqlstate) == "42000");

	ret = SQLGetDiagRec(SQL_HANDLE_STMT, hstmt, 2, sqlstate, &native_error, full_buf.data(),
	                    static_cast<SQLSMALLINT>(full_buf.size()), &full_length);
	REQUIRE(ret == SQL_NO_DATA);

	// The wide char version reports the length in characters
	std::vector<SQLWCHAR> sqlstate_w(6);
	std::vector<SQLWCHAR> small_buf_w(SQL_MAX_MESSAGE_LENGTH + 1);
	SQLSMALLINT text_length_w = 0;
	ret = SQLGetDiagRecW(SQL_HANDLE_STMT, hstmt, 1, sqlstate_w.data(), &native_error, small_buf_w.data(),
	                     static_cast<SQLSMALLINT>(small_buf_w.size()), &text_length_w);
	REQUIRE(ret == SQL_SUCCESS_WITH_INFO);
	REQUIRE(text_length_w == text_length);
	REQUIRE(ConvertToString(small_buf_w.data()) == truncated_message);

	std::vector<SQLWCHAR> full_buf_w(text_length_w + 1);
	ret = SQLGetDiagRecW(SQL_HANDLE_STMT, hstmt, 1, sqlstate_w.data(), &native_error, full_buf_w.data(),
	                     static_cast<SQLSMALLINT>(full_buf_w.size()), &text_length_w);
	REQUIRE(ret == SQL_SUCCESS);
	REQUIRE(ConvertToString(full_buf_w.data()) == full_message);

	ret = SQLGetDiagRecW(SQL_HANDLE_STMT, hstmt, 2, sqlstate_w.data(), &native_error, full_buf_w.data(),
	                     static_cast<SQLSMALLINT>(full_buf_w.size()), &text_length_w);
	REQUIRE(ret == SQL_NO_DATA);

	// SQLGetDiagField returns the whole message on every call as well
	SQLSMALLINT field_length = 0;
	ret = SQLGetDiagField(SQL_HANDLE_STMT, hstmt, 1, SQL_DIAG_MESSAGE_TEXT, small_buf.data(),
	                      static_cast<SQLSMALLINT>(small_buf.size()), &field_length);
	REQUIRE(ret == SQL_SUCCESS_WITH_INFO);
	REQUIRE(field_length == text_length);
	REQUIRE(ConvertToString(small_buf.data()) == truncated_message);

	ret = SQLGetDiagField(SQL_HANDLE_STMT, hstmt, 1, SQL_DIAG_MESSAGE_TEXT, full_buf.data(),
	                      static_cast<SQLSMALLINT>(full_buf.size()), &field_length);
	REQUIRE(ret == SQL_SUCCESS);
	REQUIRE(ConvertToString(full_buf.data()) == full_message);

	// Reading the record did not add any records
	SQLINTEGER diag_number = 0;
	EXECUTE_AND_CHECK("SQLGetDiagField (SQL_DIAG_NUMBER)", hstmt, SQLGetDiagField, SQL_HANDLE_STMT, hstmt, 0,
	                  SQL_DIAG_NUMBER, &diag_number, 0, nullptr);
	REQUIRE(diag_number == 1);

	// Free the statement handle
	EXECUTE_AND_CHECK("SQLFreeStmt (HSTMT)", hstmt, SQLFreeStmt, hstmt, SQL_CLOSE);
	EXECUTE_AND_CHECK("SQLFreeHandle (HSTMT)", hstmt, SQLFreeHandle, SQL_HANDLE_STMT, hstmt);

	DISCONNECT_FROM_DATABASE(env, dbc);
}
