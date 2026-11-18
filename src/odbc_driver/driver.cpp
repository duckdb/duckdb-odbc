#include "driver.hpp"

#include "duckdb_odbc.hpp"
#include "handle_functions.hpp"
#include "odbc_diagnostic.hpp"
#include "odbc_fetch.hpp"
#include "odbc_utils.hpp"
#include "widechar.hpp"

#include <locale>

using namespace duckdb_odbc;
using duckdb_odbc::OdbcDiagnostic;
using duckdb_odbc::OdbcUtils;
using duckdb_odbc::SQLStateType;

SQLRETURN duckdb_odbc::FreeHandle(SQLSMALLINT handle_type, SQLHANDLE handle) {
	if (!handle) {
		return SQL_INVALID_HANDLE;
	}

	switch (handle_type) {
	case SQL_HANDLE_DBC: {
		auto *hdl = static_cast<duckdb_odbc::OdbcHandleDbc *>(handle);
		delete hdl;
		return SQL_SUCCESS;
	}
	case SQL_HANDLE_DESC: {
		auto *hdl = static_cast<duckdb_odbc::OdbcHandleDesc *>(handle);
		hdl->dbc->ResetStmtDescriptors(hdl);
		delete hdl;
		return SQL_SUCCESS;
	}
	case SQL_HANDLE_ENV: {
		auto *hdl = static_cast<duckdb_odbc::OdbcHandleEnv *>(handle);
		delete hdl;
		return SQL_SUCCESS;
	}
	case SQL_HANDLE_STMT: {
		auto *hdl = static_cast<duckdb_odbc::OdbcHandleStmt *>(handle);
		hdl->dbc->EraseStmtRef(hdl);
		delete hdl;
		return SQL_SUCCESS;
	}
	default:
		return SQL_INVALID_HANDLE;
	}
}
