#include "duckdb_odbc.hpp"
#include "api_info.hpp"
#include "descriptor.hpp"
#include "odbc_fetch.hpp"
#include "odbc_interval.hpp"
#include "parameter_descriptor.hpp"
#include "row_descriptor.hpp"

using duckdb_odbc::OdbcDiagnostic;
using duckdb_odbc::OdbcHandle;
using duckdb_odbc::OdbcHandleDbc;
using duckdb_odbc::OdbcHandleDesc;
using duckdb_odbc::OdbcHandleStmt;
using duckdb_odbc::OdbcHandleType;
using duckdb_odbc::OdbcPreparedStatement;

std::string duckdb_odbc::OdbcHandleTypeToString(OdbcHandleType type) {
	switch (type) {
	case OdbcHandleType::ENV:
		return "ENV";
	case OdbcHandleType::DBC:
		return "DBC";
	case OdbcHandleType::STMT:
		return "STMT";
	case OdbcHandleType::DESC:
		return "DESC";
	}
	return "INVALID";
}

//! OdbcHandle functions ***************************************************
OdbcHandle::OdbcHandle(OdbcHandleType type_p) : type(type_p) {
	odbc_diagnostic = make_uniq<OdbcDiagnostic>();
}

OdbcHandle::OdbcHandle(const OdbcHandle &other) {
	// calling copy assigment opetator;
	*this = other;
}

OdbcHandle &OdbcHandle::operator=(const OdbcHandle &other) {
	type = other.type;
	return *this;
}

//! OdbcHandleDbc functions ***************************************************
OdbcHandleDbc::~OdbcHandleDbc() {
	// this is needed because some applications may not call SQLFreeHandle
	for (auto stmt : vec_stmt_ref) {
		delete stmt;
	}
}

void OdbcHandleDbc::EraseStmtRef(OdbcHandleStmt *stmt) {
	// erase the reference from vec_stmt_ref
	for (duckdb_odbc::idx_t v_idx = 0; v_idx < vec_stmt_ref.size(); ++v_idx) {
		if (vec_stmt_ref[v_idx] == stmt) {
			vec_stmt_ref.erase(vec_stmt_ref.begin() + v_idx);
			break;
		}
	}
}

SQLRETURN OdbcHandleDbc::MaterializeResults(OdbcHandleStmt *except) {
	SQLRETURN ret = SQL_SUCCESS;
	for (auto stmt : vec_stmt_ref) {
		if (stmt == except || !stmt->res) {
			continue;
		}
		auto stmt_ret = stmt->MaterializeResult();
		if (!SQL_SUCCEEDED(stmt_ret)) {
			ret = stmt_ret;
		}
	}
	return ret;
}

void OdbcHandleDbc::ResetStmtDescriptors(OdbcHandleDesc *old_desc) {
	for (auto stmt : vec_stmt_ref) {
		if (stmt->param_desc->GetAPD() == old_desc) {
			stmt->param_desc->ResetCurrentAPD();
		}
		if (stmt->row_desc->GetARD() == old_desc) {
			stmt->row_desc->ResetCurrentARD();
		}
	}
}

void OdbcHandleDbc::SetDatabaseName(const string &db_name) {
	if (!db_name.empty()) {
		sql_attr_current_catalog = db_name;
	}
}

std::string OdbcHandleDbc::GetDatabaseName() {
	return sql_attr_current_catalog;
}

std::string OdbcHandleDbc::GetDataSourceName() {
	return dsn;
}

void OdbcHandleDbc::ExecuteInternal(const std::string &sql) {
	D_ASSERT(conn);
	MaterializeResults();
	conn->Execute(sql).Drain();
}

void OdbcHandleDbc::SetAutoCommit(bool autocommit_p) {
	if (autocommit_p == autocommit) {
		return;
	}
	if (conn) {
		// switching to auto-commit commits the open transaction, switching to manual-commit opens one
		ExecuteInternal(autocommit_p ? "COMMIT" : "BEGIN TRANSACTION");
	}
	autocommit = autocommit_p;
}

void OdbcHandleDbc::EndTransaction(bool commit) {
	if (autocommit) {
		if (commit) {
			// every statement was already committed
			return;
		}
		// fails: there is no transaction to roll back
		ExecuteInternal("ROLLBACK");
		return;
	}
	ExecuteInternal(commit ? "COMMIT" : "ROLLBACK");
	ExecuteInternal("BEGIN TRANSACTION");
}

void OdbcHandleDbc::AfterStatement(const cxx::SqlStatement &statement) {
	if (autocommit || statement.GetStatementType() != cxx::StatementType::TRANSACTION) {
		return;
	}
	// BEGIN, COMMIT / END, or ROLLBACK / ABORT
	auto text = StringUtil::Lower(OdbcUtils::TrimString(std::string(statement.GetText())));
	for (auto keyword : {"commit", "end", "rollback", "abort"}) {
		auto len = std::strlen(keyword);
		if (text.compare(0, len, keyword) == 0 && (text.size() == len || !std::isalpha(text[len]))) {
			ExecuteInternal("BEGIN TRANSACTION");
			return;
		}
	}
}

//! OdbcPreparedStatement functions *******************************************
duckdb_odbc::unique_ptr<duckdb_odbc::QueryResult> OdbcPreparedStatement::Execute(cxx::Connection &conn,
                                                                                 const vector<Value> &values) {
	if (prepared) {
		return make_uniq<QueryResult>(prepared->Execute(values.data(), values.size()));
	}
	return make_uniq<QueryResult>(conn.Execute(*statement, values.data(), values.size()));
}

//! OdbcHandleStmt functions **************************************************
OdbcHandleStmt::OdbcHandleStmt(OdbcHandleDbc *dbc_p)
    : OdbcHandle(OdbcHandleType::STMT), dbc(dbc_p), rows_fetched_ptr(nullptr) {
	D_ASSERT(dbc_p);
	D_ASSERT(dbc_p->conn);

	odbc_fetcher = make_uniq<OdbcFetch>(this);
	dbc->vec_stmt_ref.emplace_back(this);

	// Implicit parameter and row descriptor associated with this ODBC handle statement
	param_desc = make_uniq<ParameterDescriptor>(this);
	row_desc = make_uniq<RowDescriptor>(this);
}

OdbcHandleStmt::~OdbcHandleStmt() {
}

void OdbcHandleStmt::Close() {
	open = false;
	res.reset();
	has_result = false;
	result_error.clear();
	odbc_fetcher->ClearChunks();
	// the parameter values can be reused after
	param_desc->Reset();
	// stmt->stmt.reset(); // the statment can be reuse in prepared statement
}

SQLRETURN OdbcHandleStmt::MaterializeResult() {
	if (!res) {
		return SQL_SUCCESS;
	}
	return odbc_fetcher->Materialize(this);
}

void OdbcHandleStmt::SetARD(OdbcHandleDesc *new_ard) {
	row_desc->SetCurrentARD(new_ard);
}

void OdbcHandleStmt::SetAPD(OdbcHandleDesc *new_apd) {
	param_desc->SetCurrentAPD(new_apd);
}

bool OdbcHandleStmt::ReturnsRows() {
	if (has_result) {
		return result_type == cxx::ResultType::QUERY_RESULT;
	}
	if (!stmt) {
		return false;
	}
	switch (stmt->GetStatementType()) {
	case cxx::StatementType::CALL:
	case cxx::StatementType::EXECUTE:
	case cxx::StatementType::EXPLAIN:
	case cxx::StatementType::EXTENSION:
	case cxx::StatementType::PRAGMA:
	case cxx::StatementType::SELECT:
		return true;
	default:
		return false;
	}
}

duckdb_odbc::cxx::StatementType OdbcHandleStmt::GetStatementType() {
	if (has_result && result_statement_type != cxx::StatementType::INVALID) {
		return result_statement_type;
	}
	if (!stmt) {
		return cxx::StatementType::INVALID;
	}
	return stmt->GetStatementType();
}

void OdbcHandleStmt::FillIRD(const vector<OdbcType> &types, const vector<std::string> &names) {
	D_ASSERT(types.size() == names.size());
	auto ird = row_desc->GetIRD();
	ird->Reset();
	ird->header.sql_desc_count = 0;
	auto num_cols = types.size();
	for (duckdb_odbc::idx_t col_idx = 0; col_idx < num_cols; ++col_idx) {
		duckdb_odbc::DescRecord new_record;
		auto &col_type = types[col_idx];
		new_record.logical_type = col_type;

		// TODO: Make more specific?
		auto &name = names[col_idx];
		new_record.sql_desc_base_column_name = name;
		new_record.sql_desc_name = name;
		new_record.sql_desc_label = name;
		new_record.sql_desc_length = new_record.sql_desc_label.size();

		new_record.sql_desc_unnamed = new_record.sql_desc_name.empty() ? SQL_UNNAMED : SQL_NAMED;

		auto sql_type = duckdb_odbc::ApiInfo::FindRelatedSQLType(col_type.id);
		if (sql_type == SQL_INTERVAL) {
			// default mapping from Logical::Interval -> SQL_INTERVAL_DAY_TO_SECOND
			new_record.SetSqlDescType(SQL_INTERVAL_DAY_TO_SECOND);
		} else {
			new_record.SetSqlDescType(sql_type);
		}

		// Default cast unknown types (e.g. UUID) to VARCHAR. See api_info.cpp
		if (sql_type == SQL_UNKNOWN_TYPE) {
			new_record.sql_desc_type_name = "VARCHAR";
		} else {
			new_record.sql_desc_type_name = col_type.physical_type_name;
		}

		if (sql_type == SQL_DECIMAL || sql_type == SQL_NUMERIC) {
			// SetSqlDescType initializes precision=38, scale=0 for both types.
			// Only DECIMAL types need custom precision/scale from DecimalType.
			if (col_type.id == LogicalTypeId::DECIMAL) {
				new_record.sql_desc_precision = col_type.decimal_width;
				new_record.sql_desc_scale = col_type.decimal_scale;
			}
		} else if (sql_type == SQL_TYPE_TIME || sql_type == SQL_TYPE_TIMESTAMP) {
			uint8_t precision = duckdb_odbc::ApiInfo::GetTemporalPrecision(col_type.id);
			new_record.sql_desc_precision = precision;
			new_record.sql_desc_scale = precision; // ODBC uses scale for fractional seconds
		}

		new_record.sql_desc_display_size = duckdb_odbc::ApiInfo::GetDisplaySize(col_type);
		new_record.SetDescUnsignedField(col_type);

		// the name of DuckDB's system catalog
		new_record.sql_desc_catalog_name = "system";

		// TODO: this is not correct, we need to get the schema name from the table, but the docs say that if it cannot
		// be determined, to return an empty string
		new_record.sql_desc_schema_name = "";

		ird->records.emplace_back(new_record);
	}
}
