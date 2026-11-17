#ifndef ODBC_FETCH_HPP
#define ODBC_FETCH_HPP

#include "odbc_common.hpp"

#include <sqltypes.h>
#include <sqlext.h>

namespace duckdb_odbc {

struct OdbcHandleStmt;

class OdbcFetch {
public:
	const static SQLULEN SINGLE_VALUE_FETCH = 1;

	SQLULEN cursor_type;
	SQLULEN cursor_scrollable;
	SQLLEN row_count;

	struct {
		row_t col_idx;
		row_t row_idx;
		size_t length;
	} last_fetched_variable_val;

private:
	OdbcHandleStmt *hstmt_ref;
	// main structure to hold the fetched chunks, as DuckDB returns them: their sizes are arbitrary
	vector<unique_ptr<DataChunk>> chunks;
	// the rows of the result, read from DuckDB when the statement is executed, see Prefetch
	unique_ptr<cxx::ColumnDataCollection> collection;
	unique_ptr<cxx::ColumnDataCollection::AppendState> append_state;
	unique_ptr<cxx::ColumnDataCollection::SharedScanState> shared_scan;
	unique_ptr<cxx::ColumnDataCollection::WorkerScanState> worker_scan;
	vector<LogicalType> collection_types;
	// the result was read to its end by Prefetch, but is kept for its metadata until the next read releases it
	bool result_exhausted;
	// position in the result set of the first row of each chunk in chunks; a forward-only cursor drops the chunks it
	// has moved past, so the first one does not need to start at 0
	vector<idx_t> chunk_begins;
	// number of rows of the result set fetched from DuckDB so far
	idx_t fetched_rows;

	// the current row, the one SQLGetData reads: its chunk and its index in that chunk
	idx_t current_chunk_idx;
	DataChunk *current_chunk;
	row_t chunk_row;
	// position in the result set of the current row, -1 when there is none
	row_t current_row;

	// cursor position: position in the result set of the first row of the current rowset, or one of
	// ROWSET_BEFORE_START / ROWSET_AFTER_END
	row_t rowset_begin;
	// the rowset size used by the last fetch, SQL_FETCH_NEXT moves the cursor by it
	SQLULEN last_rowset_size;

	// flag the end of the result set has reached
	// it's important because ODBC can reuse the result set many times
	bool resultset_end;

	static constexpr row_t ROWSET_BEFORE_START = -1;
	static constexpr row_t ROWSET_AFTER_END = -2;

public:
	explicit OdbcFetch(OdbcHandleStmt *hstmt)
	    : cursor_type(SQL_CURSOR_FORWARD_ONLY), cursor_scrollable(SQL_NONSCROLLABLE), row_count(0), hstmt_ref(hstmt) {
		ClearChunks();
		ResetLastFetchedVariableVal();
	}
	~OdbcFetch();

	inline void AssertCurrentChunk() {
		D_ASSERT(chunk_row <= ((row_t)current_chunk->GetRowCount()));
	}

	SQLRETURN Fetch(OdbcHandleStmt *hstmt, SQLULEN fetch_orientation, SQLLEN fetch_offset);

	SQLRETURN FetchFirst(OdbcHandleStmt *hstmt);

	SQLRETURN DummyFetch();

	//! Whether the cursor is positioned on a row, the one GetValue reads
	bool HasCurrentRow() const {
		return current_chunk && chunk_row >= 0;
	}
	//! The value of a column in the current row
	Value GetValue(SQLUSMALLINT col_idx);

	void ClearChunks();

	//! Releases the statement's result, which Prefetch read in full, so that the connection can run another
	//! statement while this one is still being fetched from.
	SQLRETURN Materialize(OdbcHandleStmt *hstmt);

	//! Reads the result of a statement that was just executed into memory, as the chunks of a collection that are
	//! then fetched from: DuckDB runs a statement as its result is read, so this is what executes it, and reports its
	//! errors. The result is kept so that its metadata can still be read; ReleaseIfExhausted then releases it.
	SQLRETURN Prefetch(OdbcHandleStmt *hstmt);
	void ReleaseIfExhausted(OdbcHandleStmt *hstmt);

	void ResetLastFetchedVariableVal();
	void SetLastFetchedVariableVal(row_t col_idx);
	void SetLastFetchedLength(size_t new_len);
	size_t GetLastFetchedLength();

	bool IsInExecutionState();

	SQLLEN GetRowCount();

private:
	//! Computes the first row of the rowset to fetch, stores it in new_rowset_begin
	SQLRETURN ScrollRowsetBegin(OdbcHandleStmt *hstmt, SQLULEN fetch_orientation, SQLLEN fetch_offset,
	                            row_t &new_rowset_begin);

	//! Fills the bound buffers with the row at rowset_idx in the rowset, the row is the current one
	SQLRETURN ColumnWise(OdbcHandleStmt *hstmt, idx_t rowset_idx);

	SQLRETURN RowWise(OdbcHandleStmt *hstmt, idx_t rowset_idx);

	//! Fetches the next chunk from the query result, SQL_NO_DATA at its end
	SQLRETURN FetchNext(OdbcHandleStmt *hstmt);

	//! The next chunk of the result; null at its end. Throws on error.
	unique_ptr<DataChunk> ReadChunk(OdbcHandleStmt *hstmt);

	SQLRETURN ReportError(OdbcHandleStmt *hstmt, const std::exception &ex);

	//! Fetches from the query result until the row at position row is fetched, SQL_NO_DATA when there is no such row
	SQLRETURN FetchUntilRow(OdbcHandleStmt *hstmt, idx_t row);

	//! Fetches the whole query result
	SQLRETURN FetchAll(OdbcHandleStmt *hstmt);

	//! Makes the (fetched) row at position row the current one
	void SetCurrentRow(idx_t row);

	//! Drops the chunks entirely before the row at position row
	void DropChunksBefore(idx_t row);

	void SetRowStatus(idx_t row_idx, SQLINTEGER status);
};
} // namespace duckdb_odbc

#endif // ODBC_FETCH_HPP
