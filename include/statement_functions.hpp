#pragma once

#include "duckdb_odbc.hpp"

namespace duckdb {

static SQLRETURN PrepareStmt(SQLHSTMT StatementHandle, SQLCHAR *StatementText, SQLINTEGER TextLength) {
	return WithStatement(StatementHandle, [&](OdbcHandleStmt *stmt) {
		if(stmt->stmt) {
			stmt->stmt.reset();
		}
		if(stmt->res) {
			stmt->res.reset();
		}
		if(stmt->chunk) {
			stmt->chunk.reset();
		}
		stmt->params.resize(0);
		stmt->bound_cols.resize(0);

		auto query = OdbcUtils::ReadString(StatementText, TextLength);
		stmt->stmt = stmt->dbc->conn->Prepare(query);
		if (!stmt->stmt->success) {
			return SQL_ERROR;
		}
		stmt->params.resize(stmt->stmt->n_param);
		stmt->bound_cols.resize(stmt->stmt->ColumnCount());
		return SQL_SUCCESS;
	});
}

static SQLRETURN ExecuteStmt(SQLHSTMT StatementHandle) {
	return WithStatement(StatementHandle, [&](OdbcHandleStmt *stmt) {
		if(stmt->res) {
			stmt->res.reset();
		}
		if(stmt->chunk) {
			stmt->chunk.reset();
		}

		stmt->chunk_row = -1;
		stmt->open = false;
		if (stmt->rows_fetched_ptr) {
			*stmt->rows_fetched_ptr = 0;
		}
		stmt->res = stmt->stmt->Execute(stmt->params);
		if (!stmt->res->success) {
			return SQL_ERROR;
		}
		stmt->open = true;
		return SQL_SUCCESS;
	});
}

} // end namespace