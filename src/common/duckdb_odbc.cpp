#include "duckdb_odbc.hpp"
#include "api_info.hpp"
#include "descriptor.hpp"
#include "odbc_fetch.hpp"
#include "odbc_interval.hpp"
#include "parameter_descriptor.hpp"
#include "row_descriptor.hpp"
#include "duckdb/main/database_manager.hpp"
#include "duckdb/main/attached_database.hpp"

using duckdb::OdbcDiagnostic;
using duckdb::OdbcHandle;
using duckdb::OdbcHandleDbc;
using duckdb::OdbcHandleDesc;
using duckdb::OdbcHandleStmt;
using duckdb::OdbcHandleType;

std::string duckdb::OdbcHandleTypeToString(OdbcHandleType type) {
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
	for (duckdb::idx_t v_idx = 0; v_idx < vec_stmt_ref.size(); ++v_idx) {
		if (vec_stmt_ref[v_idx] == stmt) {
			vec_stmt_ref.erase(vec_stmt_ref.begin() + v_idx);
			break;
		}
	}
}

SQLRETURN OdbcHandleDbc::MaterializeResult() {
	if (vec_stmt_ref.empty()) {
		return SQL_SUCCESS;
	}
	// only materializing the result set from the last statement
	return vec_stmt_ref.back()->MaterializeResult();
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
	odbc_fetcher->ClearChunks();
	// the parameter values can be reused after
	param_desc->Reset();
	// stmt->stmt.reset(); // the statment can be reuse in prepared statement
}

SQLRETURN OdbcHandleStmt::MaterializeResult() {
	if (!stmt || stmt->HasError()) {
		return SQL_SUCCESS;
	}
	if (!res || res->HasError()) {
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

void OdbcHandleStmt::FillIRD() {
	D_ASSERT(stmt);
	auto ird = row_desc->GetIRD();
	ird->Reset();
	ird->header.sql_desc_count = 0;
	auto num_cols = stmt->ColumnCount();
	for (duckdb::idx_t col_idx = 0; col_idx < num_cols; ++col_idx) {
		duckdb::DescRecord new_record;
		auto col_type = stmt->GetTypes()[col_idx];

		// TODO: Make more specific?
		auto name = stmt->GetNames()[col_idx];
		new_record.sql_desc_base_column_name = name;
		new_record.sql_desc_name = name;
		new_record.sql_desc_label = name;
		new_record.sql_desc_length = new_record.sql_desc_label.size();

		new_record.sql_desc_unnamed = new_record.sql_desc_name.empty() ? SQL_UNNAMED : SQL_NAMED;

		auto sql_type = duckdb::ApiInfo::FindRelatedSQLType(col_type.id());
		if (sql_type == SQL_INTERVAL) {
			// default mapping from Logical::Interval -> SQL_INTERVAL_DAY_TO_SECOND
			new_record.SetSqlDescType(SQL_INTERVAL_DAY_TO_SECOND);
		} else {
			new_record.SetSqlDescType(sql_type);
		}

		// Default cast unknown types (e.g. UUID) to VARCHAR. See api_info.cpp
		if (sql_type == SQL_UNKNOWN_TYPE) {
			new_record.sql_desc_type_name = duckdb::TypeIdToString(PhysicalType::VARCHAR);
		} else {
			new_record.sql_desc_type_name = duckdb::TypeIdToString(col_type.InternalType());
		}

		if (sql_type == SQL_DECIMAL || sql_type == SQL_NUMERIC) {
			// SetSqlDescType initializes precision=38, scale=0 for both types.
			// Only DECIMAL types need custom precision/scale from DecimalType.
			if (col_type.id() == LogicalTypeId::DECIMAL) {
				new_record.sql_desc_precision = duckdb::DecimalType::GetWidth(col_type);
				new_record.sql_desc_scale = duckdb::DecimalType::GetScale(col_type);
			}
		} else if (sql_type == SQL_TYPE_TIME || sql_type == SQL_TYPE_TIMESTAMP) {
			uint8_t precision = duckdb::ApiInfo::GetTemporalPrecision(col_type.id());
			new_record.sql_desc_precision = precision;
			new_record.sql_desc_scale = precision; // ODBC uses scale for fractional seconds
		}

		new_record.sql_desc_display_size = duckdb::ApiInfo::GetDisplaySize(col_type);
		new_record.SetDescUnsignedField(col_type);

		auto &db_manager = dbc->env->db->instance->GetDatabaseManager();
		auto &catalog_name = db_manager.GetSystemCatalog().GetAttached().GetName();

		new_record.sql_desc_catalog_name = catalog_name;

		// TODO: this is not correct, we need to get the schema name from the table, but the docs say that if it cannot
		// be determined, to return an empty string
		new_record.sql_desc_schema_name = "";

		ird->records.emplace_back(new_record);
	}
}
