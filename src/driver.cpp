#include "driver.hpp"

#include "duckdb/main/config.hpp"
#include "duckdb/main/db_instance_cache.hpp"
#include "duckdb_odbc.hpp"
#include "handle_functions.hpp"
#include "odbc_diagnostic.hpp"
#include "odbc_fetch.hpp"
#include "odbc_utils.hpp"
#include "widechar.hpp"

#include <locale>

using namespace duckdb;
using duckdb::OdbcDiagnostic;
using duckdb::OdbcUtils;
using duckdb::SQLStateType;

SQLRETURN duckdb::FreeHandle(SQLSMALLINT handle_type, SQLHANDLE handle) {
	if (!handle) {
		return SQL_INVALID_HANDLE;
	}

	switch (handle_type) {
	case SQL_HANDLE_DBC: {
		auto *hdl = static_cast<duckdb::OdbcHandleDbc *>(handle);
		delete hdl;
		return SQL_SUCCESS;
	}
	case SQL_HANDLE_DESC: {
		auto *hdl = static_cast<duckdb::OdbcHandleDesc *>(handle);
		hdl->dbc->ResetStmtDescriptors(hdl);
		delete hdl;
		return SQL_SUCCESS;
	}
	case SQL_HANDLE_ENV: {
		auto *hdl = static_cast<duckdb::OdbcHandleEnv *>(handle);
		delete hdl;
		return SQL_SUCCESS;
	}
	case SQL_HANDLE_STMT: {
		auto *hdl = static_cast<duckdb::OdbcHandleStmt *>(handle);
		hdl->dbc->EraseStmtRef(hdl);
		delete hdl;
		return SQL_SUCCESS;
	}
	default:
		return SQL_INVALID_HANDLE;
	}
}
