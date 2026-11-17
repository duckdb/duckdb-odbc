#include "odbc_test_common.h"

#include <vector>

using namespace odbc_test;

// Block fetches where the result needs several rowsets. The application's buffers hold exactly one rowset (that is
// all the ODBC spec asks of it), followed by a guard area filled with a sentinel. Every rowset after the first used to
// be written at its position in the result chunk instead of at the start of the buffers -- past their end, into the
// guard area. A column bound without a length/indicator buffer (legal for a NOT NULL fixed-width column) used to get
// a garbage, non-null indicator address from the second row on.

static const SQLULEN ROWSET = 4;
static const SQLLEN TOTAL_ROWS = 10;
static const size_t GUARD = 3 * ROWSET;
static const SQLINTEGER VAL_SENTINEL = -12345;
static const SQLLEN IND_SENTINEL = -777;

// v: NULL on every third row, bound with an indicator; w: never NULL, bound without one
static const char *QUERY = "SELECT CASE WHEN i % 3 = 0 THEN NULL ELSE i END::INTEGER AS v, i::INTEGER AS w "
                           "FROM range(10) t(i) ORDER BY i";

static bool ExpectNull(SQLLEN row) {
	return row % 3 == 0;
}

TEST_CASE("Test column-wise block fetch over several rowsets", "[odbc]") {
	SQLHANDLE env;
	SQLHANDLE dbc;
	HSTMT hstmt = SQL_NULL_HSTMT;

	CONNECT_TO_DATABASE(env, dbc);
	EXECUTE_AND_CHECK("SQLAllocHandle (HSTMT)", hstmt, SQLAllocHandle, SQL_HANDLE_STMT, dbc, &hstmt);

	std::vector<SQLINTEGER> v(ROWSET + GUARD, VAL_SENTINEL);
	std::vector<SQLLEN> v_ind(ROWSET + GUARD, IND_SENTINEL);
	std::vector<SQLINTEGER> w(ROWSET + GUARD, VAL_SENTINEL);
	std::vector<SQLUSMALLINT> status(ROWSET + GUARD, 0xFFFF);
	SQLULEN rows_fetched = 0;

	EXECUTE_AND_CHECK("SQLSetStmtAttr (SQL_ATTR_ROW_ARRAY_SIZE)", hstmt, SQLSetStmtAttr, hstmt, SQL_ATTR_ROW_ARRAY_SIZE,
	                  ConvertToSQLPOINTER(ROWSET), 0);
	EXECUTE_AND_CHECK("SQLSetStmtAttr (SQL_ATTR_ROWS_FETCHED_PTR)", hstmt, SQLSetStmtAttr, hstmt,
	                  SQL_ATTR_ROWS_FETCHED_PTR, &rows_fetched, 0);
	EXECUTE_AND_CHECK("SQLSetStmtAttr (SQL_ATTR_ROW_STATUS_PTR)", hstmt, SQLSetStmtAttr, hstmt, SQL_ATTR_ROW_STATUS_PTR,
	                  status.data(), 0);

	EXECUTE_AND_CHECK("SQLBindCol (v)", hstmt, SQLBindCol, hstmt, 1, SQL_C_SLONG, v.data(), 0, v_ind.data());
	EXECUTE_AND_CHECK("SQLBindCol (w)", hstmt, SQLBindCol, hstmt, 2, SQL_C_SLONG, w.data(), 0, nullptr);

	EXECUTE_AND_CHECK("SQLExecDirect", hstmt, SQLExecDirect, hstmt, ConvertToSQLCHAR(QUERY), SQL_NTS);

	SQLLEN first = 0;
	while (first < TOTAL_ROWS) {
		EXECUTE_AND_CHECK("SQLFetch", hstmt, SQLFetch, hstmt);
		SQLLEN expected = std::min<SQLLEN>(ROWSET, TOTAL_ROWS - first);
		REQUIRE(rows_fetched == static_cast<SQLULEN>(expected));
		for (SQLLEN r = 0; r < expected; r++) {
			SQLLEN row = first + r;
			REQUIRE(status[r] == SQL_ROW_SUCCESS);
			REQUIRE(w[r] == row);
			if (ExpectNull(row)) {
				REQUIRE(v_ind[r] == SQL_NULL_DATA);
			} else {
				REQUIRE(v_ind[r] == static_cast<SQLLEN>(sizeof(SQLINTEGER)));
				REQUIRE(v[r] == row);
			}
		}
		// nothing may land past the rowset
		for (size_t g = ROWSET; g < ROWSET + GUARD; g++) {
			REQUIRE(v[g] == VAL_SENTINEL);
			REQUIRE(v_ind[g] == IND_SENTINEL);
			REQUIRE(w[g] == VAL_SENTINEL);
			REQUIRE(status[g] == 0xFFFF);
		}
		first += expected;
	}
	REQUIRE(SQLFetch(hstmt) == SQL_NO_DATA);

	EXECUTE_AND_CHECK("SQLFreeStmt (HSTMT)", hstmt, SQLFreeStmt, hstmt, SQL_CLOSE);
	EXECUTE_AND_CHECK("SQLFreeHandle (HSTMT)", hstmt, SQLFreeHandle, SQL_HANDLE_STMT, hstmt);
	DISCONNECT_FROM_DATABASE(env, dbc);
}

struct FetchRow {
	SQLINTEGER v;
	SQLLEN v_ind;
	SQLINTEGER w;
};

TEST_CASE("Test row-wise block fetch over several rowsets", "[odbc]") {
	SQLHANDLE env;
	SQLHANDLE dbc;
	HSTMT hstmt = SQL_NULL_HSTMT;

	CONNECT_TO_DATABASE(env, dbc);
	EXECUTE_AND_CHECK("SQLAllocHandle (HSTMT)", hstmt, SQLAllocHandle, SQL_HANDLE_STMT, dbc, &hstmt);

	FetchRow sentinel_row = {VAL_SENTINEL, IND_SENTINEL, VAL_SENTINEL};
	std::vector<FetchRow> rows(ROWSET + GUARD, sentinel_row);
	std::vector<SQLUSMALLINT> status(ROWSET + GUARD, 0xFFFF);
	SQLULEN rows_fetched = 0;

	EXECUTE_AND_CHECK("SQLSetStmtAttr (SQL_ATTR_ROW_BIND_TYPE)", hstmt, SQLSetStmtAttr, hstmt, SQL_ATTR_ROW_BIND_TYPE,
	                  ConvertToSQLPOINTER(sizeof(FetchRow)), 0);
	EXECUTE_AND_CHECK("SQLSetStmtAttr (SQL_ATTR_ROW_ARRAY_SIZE)", hstmt, SQLSetStmtAttr, hstmt, SQL_ATTR_ROW_ARRAY_SIZE,
	                  ConvertToSQLPOINTER(ROWSET), 0);
	EXECUTE_AND_CHECK("SQLSetStmtAttr (SQL_ATTR_ROWS_FETCHED_PTR)", hstmt, SQLSetStmtAttr, hstmt,
	                  SQL_ATTR_ROWS_FETCHED_PTR, &rows_fetched, 0);
	EXECUTE_AND_CHECK("SQLSetStmtAttr (SQL_ATTR_ROW_STATUS_PTR)", hstmt, SQLSetStmtAttr, hstmt, SQL_ATTR_ROW_STATUS_PTR,
	                  status.data(), 0);

	EXECUTE_AND_CHECK("SQLBindCol (v)", hstmt, SQLBindCol, hstmt, 1, SQL_C_SLONG, &rows[0].v, 0, &rows[0].v_ind);
	EXECUTE_AND_CHECK("SQLBindCol (w)", hstmt, SQLBindCol, hstmt, 2, SQL_C_SLONG, &rows[0].w, 0, nullptr);

	EXECUTE_AND_CHECK("SQLExecDirect", hstmt, SQLExecDirect, hstmt, ConvertToSQLCHAR(QUERY), SQL_NTS);

	SQLLEN first = 0;
	while (first < TOTAL_ROWS) {
		EXECUTE_AND_CHECK("SQLFetch", hstmt, SQLFetch, hstmt);
		SQLLEN expected = std::min<SQLLEN>(ROWSET, TOTAL_ROWS - first);
		REQUIRE(rows_fetched == static_cast<SQLULEN>(expected));
		for (SQLLEN r = 0; r < expected; r++) {
			SQLLEN row = first + r;
			REQUIRE(status[r] == SQL_ROW_SUCCESS);
			REQUIRE(rows[r].w == row);
			if (ExpectNull(row)) {
				REQUIRE(rows[r].v_ind == SQL_NULL_DATA);
			} else {
				REQUIRE(rows[r].v_ind == static_cast<SQLLEN>(sizeof(SQLINTEGER)));
				REQUIRE(rows[r].v == row);
			}
		}
		for (size_t g = ROWSET; g < ROWSET + GUARD; g++) {
			REQUIRE(rows[g].v == VAL_SENTINEL);
			REQUIRE(rows[g].v_ind == IND_SENTINEL);
			REQUIRE(rows[g].w == VAL_SENTINEL);
			REQUIRE(status[g] == 0xFFFF);
		}
		first += expected;
	}
	REQUIRE(SQLFetch(hstmt) == SQL_NO_DATA);

	EXECUTE_AND_CHECK("SQLFreeStmt (HSTMT)", hstmt, SQLFreeStmt, hstmt, SQL_CLOSE);
	EXECUTE_AND_CHECK("SQLFreeHandle (HSTMT)", hstmt, SQLFreeHandle, SQL_HANDLE_STMT, hstmt);
	DISCONNECT_FROM_DATABASE(env, dbc);
}

// Rowsets spanning several result chunks. DuckDB returns a result in chunks of arbitrary sizes (2048 rows for this
// query): a rowset used to end at the end of a chunk, returning fewer rows than the rowset size and more rowsets than
// needed. Positioning the cursor (SQL_FETCH_PRIOR, SQL_FETCH_ABSOLUTE) used to be relative to the current chunk.

static const SQLULEN SPANNING_ROWSET = 300;
static const SQLLEN SPANNING_TOTAL_ROWS = 5000;
static const char *SPANNING_QUERY = "SELECT i::INTEGER AS v FROM range(5000) t(i)";

struct SpanningFetch {
	std::vector<SQLINTEGER> v = std::vector<SQLINTEGER>(SPANNING_ROWSET);
	std::vector<SQLLEN> v_ind = std::vector<SQLLEN>(SPANNING_ROWSET);
	std::vector<SQLUSMALLINT> status = std::vector<SQLUSMALLINT>(SPANNING_ROWSET);
	SQLULEN rows_fetched = 0;

	void Bind(HSTMT hstmt) {
		EXECUTE_AND_CHECK("SQLSetStmtAttr (SQL_ATTR_ROW_ARRAY_SIZE)", hstmt, SQLSetStmtAttr, hstmt,
		                  SQL_ATTR_ROW_ARRAY_SIZE, ConvertToSQLPOINTER(SPANNING_ROWSET), 0);
		EXECUTE_AND_CHECK("SQLSetStmtAttr (SQL_ATTR_ROWS_FETCHED_PTR)", hstmt, SQLSetStmtAttr, hstmt,
		                  SQL_ATTR_ROWS_FETCHED_PTR, &rows_fetched, 0);
		EXECUTE_AND_CHECK("SQLSetStmtAttr (SQL_ATTR_ROW_STATUS_PTR)", hstmt, SQLSetStmtAttr, hstmt,
		                  SQL_ATTR_ROW_STATUS_PTR, status.data(), 0);
		EXECUTE_AND_CHECK("SQLBindCol (v)", hstmt, SQLBindCol, hstmt, 1, SQL_C_SLONG, v.data(), 0, v_ind.data());
	}

	// the rowset holds the rows from first (0-based) on, as many as the rowset size or up to the end of the result
	void Check(SQLLEN first) {
		SQLLEN expected = std::min<SQLLEN>(SPANNING_ROWSET, SPANNING_TOTAL_ROWS - first);
		REQUIRE(rows_fetched == static_cast<SQLULEN>(expected));
		for (SQLLEN r = 0; r < expected; r++) {
			REQUIRE(status[r] == SQL_ROW_SUCCESS);
			REQUIRE(v_ind[r] == static_cast<SQLLEN>(sizeof(SQLINTEGER)));
			REQUIRE(v[r] == first + r);
		}
		for (SQLLEN r = expected; r < static_cast<SQLLEN>(SPANNING_ROWSET); r++) {
			REQUIRE(status[r] == SQL_ROW_NOROW);
		}
	}

	void FetchScroll(HSTMT hstmt, SQLSMALLINT orientation, SQLLEN offset, SQLLEN expected_first) {
		EXECUTE_AND_CHECK("SQLFetchScroll", hstmt, SQLFetchScroll, hstmt, orientation, offset);
		Check(expected_first);
	}
};

TEST_CASE("Test block fetch with rowsets spanning result chunks", "[odbc]") {
	SQLHANDLE env;
	SQLHANDLE dbc;
	HSTMT hstmt = SQL_NULL_HSTMT;

	CONNECT_TO_DATABASE(env, dbc);
	EXECUTE_AND_CHECK("SQLAllocHandle (HSTMT)", hstmt, SQLAllocHandle, SQL_HANDLE_STMT, dbc, &hstmt);

	SpanningFetch fetch;
	fetch.Bind(hstmt);
	EXECUTE_AND_CHECK("SQLExecDirect", hstmt, SQLExecDirect, hstmt, ConvertToSQLCHAR(SPANNING_QUERY), SQL_NTS);

	for (SQLLEN first = 0; first < SPANNING_TOTAL_ROWS; first += SPANNING_ROWSET) {
		EXECUTE_AND_CHECK("SQLFetch", hstmt, SQLFetch, hstmt);
		fetch.Check(first);
	}
	REQUIRE(SQLFetch(hstmt) == SQL_NO_DATA);

	EXECUTE_AND_CHECK("SQLFreeStmt (HSTMT)", hstmt, SQLFreeStmt, hstmt, SQL_CLOSE);
	EXECUTE_AND_CHECK("SQLFreeHandle (HSTMT)", hstmt, SQLFreeHandle, SQL_HANDLE_STMT, hstmt);
	DISCONNECT_FROM_DATABASE(env, dbc);
}

TEST_CASE("Test scrolling with rowsets spanning result chunks", "[odbc]") {
	SQLHANDLE env;
	SQLHANDLE dbc;
	HSTMT hstmt = SQL_NULL_HSTMT;

	CONNECT_TO_DATABASE(env, dbc);
	EXECUTE_AND_CHECK("SQLAllocHandle (HSTMT)", hstmt, SQLAllocHandle, SQL_HANDLE_STMT, dbc, &hstmt);
	EXECUTE_AND_CHECK("SQLSetStmtAttr (SQL_ATTR_CURSOR_TYPE)", hstmt, SQLSetStmtAttr, hstmt, SQL_ATTR_CURSOR_TYPE,
	                  ConvertToSQLPOINTER(SQL_CURSOR_STATIC), 0);

	SpanningFetch fetch;
	fetch.Bind(hstmt);
	EXECUTE_AND_CHECK("SQLExecDirect", hstmt, SQLExecDirect, hstmt, ConvertToSQLCHAR(SPANNING_QUERY), SQL_NTS);

	// before the start: there is no prior rowset
	REQUIRE(SQLFetchScroll(hstmt, SQL_FETCH_PRIOR, 0) == SQL_NO_DATA);
	fetch.FetchScroll(hstmt, SQL_FETCH_NEXT, 0, 0);

	// 1800 - 2099 and 2100 - 2399 span the first two chunks
	fetch.FetchScroll(hstmt, SQL_FETCH_ABSOLUTE, 1801, 1800);
	fetch.FetchScroll(hstmt, SQL_FETCH_NEXT, 0, 2100);
	fetch.FetchScroll(hstmt, SQL_FETCH_PRIOR, 0, 1800);
	fetch.FetchScroll(hstmt, SQL_FETCH_PRIOR, 0, 1500);

	// counted from the end: the last 100 rows, the rowset is not full
	fetch.FetchScroll(hstmt, SQL_FETCH_ABSOLUTE, -100, 4900);
	fetch.FetchScroll(hstmt, SQL_FETCH_PRIOR, 0, 4600);

	// after the end, the prior rowset is the last full one
	REQUIRE(SQLFetchScroll(hstmt, SQL_FETCH_ABSOLUTE, SPANNING_TOTAL_ROWS + 1) == SQL_NO_DATA);
	REQUIRE(SQLFetchScroll(hstmt, SQL_FETCH_NEXT, 0) == SQL_NO_DATA);
	fetch.FetchScroll(hstmt, SQL_FETCH_PRIOR, 0, SPANNING_TOTAL_ROWS - SPANNING_ROWSET);

	// fewer rows than the rowset size before the current rowset: the first rowset, with a warning
	fetch.FetchScroll(hstmt, SQL_FETCH_ABSOLUTE, 101, 100);
	REQUIRE(SQLFetchScroll(hstmt, SQL_FETCH_PRIOR, 0) == SQL_SUCCESS_WITH_INFO);
	fetch.Check(0);
	REQUIRE(SQLFetchScroll(hstmt, SQL_FETCH_PRIOR, 0) == SQL_NO_DATA);

	fetch.FetchScroll(hstmt, SQL_FETCH_FIRST, 0, 0);
	REQUIRE(SQLFetchScroll(hstmt, SQL_FETCH_ABSOLUTE, 0) == SQL_NO_DATA);
	fetch.FetchScroll(hstmt, SQL_FETCH_NEXT, 0, 0);

	EXECUTE_AND_CHECK("SQLFreeStmt (HSTMT)", hstmt, SQLFreeStmt, hstmt, SQL_CLOSE);
	EXECUTE_AND_CHECK("SQLFreeHandle (HSTMT)", hstmt, SQLFreeHandle, SQL_HANDLE_STMT, hstmt);
	DISCONNECT_FROM_DATABASE(env, dbc);
}
