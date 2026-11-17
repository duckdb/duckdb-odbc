#include "odbc_fetch.hpp"
#include "duckdb_odbc.hpp"
#include "api_info.hpp"
#include "row_descriptor.hpp"
#include "statement_functions.hpp"
#include "handle_functions.hpp"

#include <sql.h>
#include <sqltypes.h>
#include <sqlext.h>

#include <algorithm>

using duckdb_odbc::OdbcFetch;
using duckdb_odbc::OdbcHandleStmt;

OdbcFetch::~OdbcFetch() {
	chunks.clear();
}

SQLRETURN OdbcFetch::Materialize(OdbcHandleStmt *hstmt) {
	// the fetched chunks are only appended to, the cursor position is preserved
	return FetchAll(hstmt);
}

SQLRETURN OdbcFetch::FetchNext(OdbcHandleStmt *hstmt) {
	// case hasn't reached the end of query result, then try to fetch
	if (resultset_end) {
		return SQL_NO_DATA;
	}
	try {
		auto chunk = hstmt->res->Fetch();
		if (hstmt->res->HasError()) {
			hstmt->open = false;
			return SetDiagnosticRecord(hstmt, SQL_ERROR, "FetchNext", hstmt->res->GetError(),
			                           duckdb_odbc::SQLStateType::ST_HY000, hstmt->dbc->GetDataSourceName());
		}
		if (!chunk || chunk->size() == 0) {
			resultset_end = true;
			return SQL_NO_DATA;
		}
		chunk_begins.push_back(fetched_rows);
		fetched_rows += chunk->size();
		chunks.emplace_back(std::move(chunk));
	} catch (duckdb::Exception &e) {
		// TODO this is quite dirty, we should have separate error holder
		hstmt->res->SetError(ErrorData(e));
		hstmt->open = false;
		return SetDiagnosticRecord(hstmt, SQL_ERROR, "FetchNext", hstmt->res->GetError(),
		                           duckdb_odbc::SQLStateType::ST_HY000, hstmt->dbc->GetDataSourceName());
	}
	return SQL_SUCCESS;
}

SQLRETURN OdbcFetch::FetchUntilRow(OdbcHandleStmt *hstmt, idx_t row) {
	while (fetched_rows <= row) {
		auto ret = FetchNext(hstmt);
		if (ret != SQL_SUCCESS) {
			return ret;
		}
	}
	return SQL_SUCCESS;
}

SQLRETURN OdbcFetch::FetchAll(OdbcHandleStmt *hstmt) {
	SQLRETURN ret;
	do {
		ret = FetchNext(hstmt);
	} while (ret == SQL_SUCCESS);
	D_ASSERT(ret != SQL_NO_DATA || resultset_end);
	return ret == SQL_NO_DATA ? SQL_SUCCESS : ret;
}

void OdbcFetch::SetCurrentRow(idx_t row) {
	D_ASSERT(!chunks.empty() && row >= chunk_begins.front() && row < fetched_rows);
	if (!current_chunk || row < chunk_begins[current_chunk_idx] ||
	    row >= chunk_begins[current_chunk_idx] + current_chunk->size()) {
		// the chunk holding the row is the last one beginning at or before it
		auto next_chunk = std::upper_bound(chunk_begins.begin(), chunk_begins.end(), row);
		current_chunk_idx = static_cast<idx_t>(next_chunk - chunk_begins.begin()) - 1;
		current_chunk = chunks[current_chunk_idx].get();
	}
	chunk_row = static_cast<row_t>(row - chunk_begins[current_chunk_idx]);
	current_row = static_cast<row_t>(row);
}

void OdbcFetch::DropChunksBefore(idx_t row) {
	idx_t drop_count = 0;
	while (drop_count < chunks.size() && chunk_begins[drop_count] + chunks[drop_count]->size() <= row) {
		drop_count++;
	}
	if (drop_count == 0) {
		return;
	}
	chunks.erase(chunks.begin(), chunks.begin() + static_cast<std::ptrdiff_t>(drop_count));
	chunk_begins.erase(chunk_begins.begin(), chunk_begins.begin() + static_cast<std::ptrdiff_t>(drop_count));
	// the current row may have been dropped
	current_chunk = nullptr;
	current_chunk_idx = 0;
	chunk_row = current_row = -1;
}

// See the tables "Cursor Positioning Rules" in
// https://learn.microsoft.com/en-us/sql/odbc/reference/syntax/sqlfetchscroll-function
// Positions there are 1-based, rowset_begin is 0-based.
SQLRETURN OdbcFetch::ScrollRowsetBegin(OdbcHandleStmt *hstmt, SQLULEN fetch_orientation, SQLLEN fetch_offset,
                                       row_t &new_rowset_begin) {
	auto rowset_size = static_cast<row_t>(hstmt_ref->row_desc->ard->header.sql_desc_array_size);
	SQLRETURN ret;

	switch (fetch_orientation) {
	case SQL_FETCH_NEXT:
		if (rowset_begin == ROWSET_AFTER_END) {
			return SQL_NO_DATA;
		}
		// the cursor moves by the rowset size of the previous fetch
		new_rowset_begin = rowset_begin == ROWSET_BEFORE_START ? 0 : rowset_begin + (row_t)last_rowset_size;
		return SQL_SUCCESS;
	case SQL_FETCH_PRIOR:
		if (rowset_begin == ROWSET_BEFORE_START) {
			return SQL_NO_DATA;
		}
		if (rowset_begin == ROWSET_AFTER_END) {
			// the last rowset: the size of the result set is needed
			ret = FetchAll(hstmt);
			if (ret != SQL_SUCCESS) {
				return ret;
			}
			new_rowset_begin = duckdb::MaxValue<row_t>(static_cast<row_t>(fetched_rows) - rowset_size, 0);
			return SQL_SUCCESS;
		}
		if (rowset_begin == 0) {
			rowset_begin = ROWSET_BEFORE_START;
			return SQL_NO_DATA;
		}
		if (rowset_begin < rowset_size) {
			new_rowset_begin = 0;
			return SetDiagnosticRecord(hstmt, SQL_SUCCESS_WITH_INFO, "SQLFetchScroll",
			                           "Attempt to fetch before the result set returned the first rowset",
			                           SQLStateType::ST_01S06, hstmt->dbc->GetDataSourceName());
		}
		new_rowset_begin = rowset_begin - rowset_size;
		return SQL_SUCCESS;
	case SQL_FETCH_ABSOLUTE: {
		if (fetch_offset == 0) {
			rowset_begin = ROWSET_BEFORE_START;
			return SQL_NO_DATA;
		}
		if (fetch_offset > 0) {
			// a row past the end of the result set positions the cursor after it, see Fetch
			new_rowset_begin = fetch_offset - 1;
			return SQL_SUCCESS;
		}
		// a negative offset counts from the end: the size of the result set is needed
		ret = FetchAll(hstmt);
		if (ret != SQL_SUCCESS) {
			return ret;
		}
		auto rows_from_end = static_cast<idx_t>(-(fetch_offset + 1)) + 1;
		if (rows_from_end <= fetched_rows) {
			new_rowset_begin = static_cast<row_t>(fetched_rows - rows_from_end);
			return SQL_SUCCESS;
		}
		if (rows_from_end > static_cast<idx_t>(rowset_size)) {
			rowset_begin = ROWSET_BEFORE_START;
			return SQL_NO_DATA;
		}
		new_rowset_begin = 0;
		return SetDiagnosticRecord(hstmt, SQL_SUCCESS_WITH_INFO, "SQLFetchScroll",
		                           "Attempt to fetch before the result set returned the first rowset",
		                           SQLStateType::ST_01S06, hstmt->dbc->GetDataSourceName());
	}
	case SQL_FETCH_FIRST:
		new_rowset_begin = 0;
		return SQL_SUCCESS;
	default:
		return SetDiagnosticRecord(hstmt, SQL_ERROR, "SQLFetchScroll", "Fetch type out of range",
		                           SQLStateType::ST_HY106, hstmt->dbc->GetDataSourceName());
	}
}

SQLRETURN OdbcFetch::DummyFetch() {
	if (hstmt_ref->retrieve_data == SQL_RD_OFF) {
		auto row_set_size = (SQLLEN)hstmt_ref->row_desc->ard->header.sql_desc_array_size;

		if (hstmt_ref->odbc_fetcher->chunk_row + row_set_size > hstmt_ref->odbc_fetcher->row_count) {
			row_set_size = hstmt_ref->odbc_fetcher->row_count - hstmt_ref->odbc_fetcher->chunk_row;
		}
		if (row_set_size <= 0) {
			return SQL_NO_DATA;
		}
		if (hstmt_ref->row_desc->ird->header.sql_desc_array_status_ptr) {
			duckdb::idx_t row_idx;
			for (row_idx = 0; row_idx < (duckdb::idx_t)row_set_size; row_idx++) {
				hstmt_ref->row_desc->ird->header.sql_desc_array_status_ptr[row_idx] = SQL_ROW_SUCCESS;
			}
			for (; row_idx < hstmt_ref->row_desc->ard->header.sql_desc_array_size; row_idx++) {
				hstmt_ref->row_desc->ird->header.sql_desc_array_status_ptr[row_idx] = SQL_ROW_NOROW;
			}
		}
		return SQL_SUCCESS;
	}
	return SQL_NEED_DATA;
}

SQLRETURN OdbcFetch::GetValue(SQLUSMALLINT col_idx, duckdb::Value &value) {
	if (!current_chunk || chunk_row < 0) {
		// the cursor is not positioned on a row
		return SQL_ERROR;
	}
	value = current_chunk->GetValue(col_idx, chunk_row);
	return SQL_SUCCESS;
}

SQLRETURN OdbcFetch::Fetch(OdbcHandleStmt *hstmt, SQLULEN fetch_orientation, SQLLEN fetch_offset) {
	if (cursor_type == SQL_CURSOR_FORWARD_ONLY && fetch_orientation != SQL_FETCH_NEXT) {
		return SetDiagnosticRecord(hstmt, SQL_ERROR, "Fetch",
		                           "Incorrect fetch orientation for cursor type: SQL_CURSOR_FORWARD_ONLY.",
		                           SQLStateType::ST_24000, hstmt->dbc->GetDataSourceName());
	}
	// SQLGetData starts over on the rows of the new rowset
	ResetLastFetchedVariableVal();

	row_t new_rowset_begin = 0;
	SQLRETURN ret = ScrollRowsetBegin(hstmt, fetch_orientation, fetch_offset, new_rowset_begin);
	if (!SQL_SUCCEEDED(ret)) {
		return ret;
	}
	D_ASSERT(new_rowset_begin >= 0);
	auto rowset_begin_row = static_cast<idx_t>(new_rowset_begin);

	if (cursor_type == SQL_CURSOR_FORWARD_ONLY) {
		// holding only the chunks of the rowsets to come, to avoid full materialization in memory
		DropChunksBefore(rowset_begin_row);
	}

	auto fetch_ret = FetchUntilRow(hstmt, rowset_begin_row);
	if (fetch_ret == SQL_NO_DATA) {
		rowset_begin = ROWSET_AFTER_END;
		return SQL_NO_DATA;
	}
	if (fetch_ret != SQL_SUCCESS) {
		return fetch_ret;
	}

	auto rowset_size = hstmt_ref->row_desc->ard->header.sql_desc_array_size;
	rowset_begin = new_rowset_begin;
	last_rowset_size = rowset_size;

	// the rows of the rowset are fetched as needed: a rowset may span several chunks
	idx_t rowset_idx;
	for (rowset_idx = 0; rowset_idx < rowset_size; rowset_idx++) {
		auto row = rowset_begin_row + rowset_idx;
		fetch_ret = FetchUntilRow(hstmt, row);
		if (fetch_ret == SQL_NO_DATA) {
			break;
		}
		if (fetch_ret != SQL_SUCCESS) {
			return fetch_ret;
		}
		SetCurrentRow(row);

		SQLRETURN row_ret;
		if (hstmt_ref->row_desc->ard->header.sql_desc_bind_type == SQL_BIND_BY_COLUMN) {
			row_ret = OdbcFetch::ColumnWise(hstmt, rowset_idx);
		} else {
			// sql_desc_bind_type should be greater than 0 because it contains the length of the row to be fetched
			D_ASSERT(hstmt->row_desc->ard->header.sql_desc_bind_type > 0);
			row_ret = OdbcFetch::RowWise(hstmt, rowset_idx);
		}
		// report the most severe outcome of the rowset's rows
		if (row_ret == SQL_ERROR || (row_ret == SQL_SUCCESS_WITH_INFO && ret != SQL_ERROR)) {
			ret = row_ret;
		}
	}
	for (idx_t norow_idx = rowset_idx; norow_idx < rowset_size; norow_idx++) {
		SetRowStatus(norow_idx, SQL_ROW_NOROW);
	}

	if (hstmt->rows_fetched_ptr) {
		*hstmt->rows_fetched_ptr = rowset_idx;
	}
	row_count = duckdb::MaxValue<SQLLEN>(row_count, static_cast<SQLLEN>(rowset_begin_row + rowset_idx));
	return ret;
}

SQLRETURN OdbcFetch::FetchFirst(OdbcHandleStmt *hstmt) {
	// fetch the first chunk, so that errors arising when running the query are reported by its execution; the cursor
	// stays positioned before the start of the result set
	auto ret = FetchUntilRow(hstmt, 0);
	return ret == SQL_NO_DATA ? SQL_SUCCESS : ret;
}

SQLRETURN OdbcFetch::ColumnWise(OdbcHandleStmt *hstmt, idx_t rowset_idx) {
	SQLRETURN ret = SQL_SUCCESS;

	SetRowStatus(rowset_idx, SQL_SUCCESS);
	// now fill buffers in fetch if set
	// TODO actually vectorize this
	for (duckdb::idx_t col_idx = 0; col_idx < hstmt->bound_cols.size(); col_idx++) {
		auto bound_col = hstmt->bound_cols[col_idx];

		if (!bound_col.IsBound() && !bound_col.IsVarcharBound()) {
			continue;
		}
		auto target_val_addr = bound_col.ptr;
		auto target_len_addr = bound_col.strlen_or_ind;

		if (hstmt_ref->row_desc->ard->header.sql_desc_array_size != SINGLE_VALUE_FETCH) {
			// the application's buffers hold ONE rowset: index them by the row's position in the rowset
			// need specialized pointer arithmetic according to the value type
			auto pointer_size = ApiInfo::PointerSizeOf(bound_col.type);
			if (pointer_size < 0) {
				pointer_size = bound_col.len;
			}
			target_val_addr = (uint8_t *)target_val_addr + (rowset_idx * pointer_size);
			// a column may be bound without a length/indicator buffer (NOT NULL, fixed width): keep it null
			if (target_len_addr) {
				target_len_addr += rowset_idx;
			}
		}

		ret = duckdb_odbc::GetDataStmtResult(hstmt, static_cast<SQLUSMALLINT>(col_idx + 1), bound_col.type,
		                                     target_val_addr, bound_col.len, target_len_addr);
		if (!SQL_SUCCEEDED(ret)) {
			SetRowStatus(rowset_idx, SQL_ROW_ERROR);
		}
	}

	return ret;
}

SQLRETURN OdbcFetch::RowWise(OdbcHandleStmt *hstmt, idx_t rowset_idx) {
	SQLRETURN ret = SQL_SUCCESS;
	SQLULEN row_size = hstmt->row_desc->ard->header.sql_desc_bind_type;

	SetRowStatus(rowset_idx, SQL_SUCCESS);
	// the application's buffers hold ONE rowset: index them by the row's position in the rowset
	auto row_offset = row_size * rowset_idx;
	for (duckdb::idx_t col_idx = 0; col_idx < hstmt->bound_cols.size(); col_idx++) {
		auto bound_col = hstmt->bound_cols[col_idx];
		if (!bound_col.IsBound()) {
			continue;
		}

		// the addresses must be byte addressable for row offset pointer arithmetic
		uint8_t *target_val_addr = (uint8_t *)bound_col.ptr + row_offset;
		// a column may be bound without a length/indicator buffer (NOT NULL, fixed width): offsetting a null
		// pointer would hand GetData a non-null garbage address from the second row on
		uint8_t *target_len_addr = bound_col.strlen_or_ind ? (uint8_t *)bound_col.strlen_or_ind + row_offset : nullptr;

		ret = duckdb_odbc::GetDataStmtResult(hstmt, static_cast<SQLUSMALLINT>(col_idx + 1), bound_col.type,
		                                     target_val_addr, bound_col.len, (SQLLEN *)target_len_addr);
		if (!SQL_SUCCEEDED(ret)) {
			SetRowStatus(rowset_idx, SQL_ROW_ERROR);
		}
	}

	return ret;
}

void OdbcFetch::ClearChunks() {
	chunks.clear();
	chunk_begins.clear();
	fetched_rows = 0;
	current_chunk = nullptr;
	current_chunk_idx = 0;
	chunk_row = current_row = -1;
	rowset_begin = ROWSET_BEFORE_START;
	last_rowset_size = 0;
	row_count = 0;
	resultset_end = false;
}

void OdbcFetch::ResetLastFetchedVariableVal() {
	last_fetched_variable_val.col_idx = -1;
	last_fetched_variable_val.row_idx = -1;
	last_fetched_variable_val.length = 0;
}

void OdbcFetch::SetLastFetchedVariableVal(row_t col_idx) {
	if (last_fetched_variable_val.col_idx != col_idx || last_fetched_variable_val.row_idx != current_row) {
		last_fetched_variable_val.length = 0;
	}
	last_fetched_variable_val.col_idx = col_idx;
	last_fetched_variable_val.row_idx = current_row;
}

void OdbcFetch::SetLastFetchedLength(size_t new_len) {
	last_fetched_variable_val.length = new_len;
}

size_t OdbcFetch::GetLastFetchedLength() {
	return last_fetched_variable_val.length;
}

bool OdbcFetch::IsInExecutionState() {
	return !chunks.empty();
}

SQLLEN OdbcFetch::GetRowCount() {
	// the chunk of the current row, or the first one fetched when the cursor is not on a row
	auto chunk = current_chunk ? current_chunk : (chunks.empty() ? nullptr : chunks.front().get());
	if (chunk) {
		return duckdb::MaxValue(row_count, (SQLLEN)chunk->size());
	}
	return row_count;
}

void OdbcFetch::SetRowStatus(idx_t row_idx, SQLINTEGER status) {
	if (hstmt_ref->row_desc->ird->header.sql_desc_array_status_ptr) {
		hstmt_ref->row_desc->ird->header.sql_desc_array_status_ptr[row_idx] = static_cast<SQLUSMALLINT>(status);
	}
}
