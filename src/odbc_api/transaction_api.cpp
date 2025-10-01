#include "duckdb_odbc.hpp"
#include "api_info.hpp"
#include "odbc_diagnostic.hpp"
#include "odbc_utils.hpp"
#include "handle_functions.hpp"

using duckdb::SQLStateType;

//===--------------------------------------------------------------------===//
// SQLEndTran
//===--------------------------------------------------------------------===//

/**
 * <a
 * href="https://learn.microsoft.com/en-us/sql/odbc/reference/syntax/sqlendtran-function?view=sql-server-ver15">Docs</a>
 */
SQLRETURN SQL_API SQLEndTran(SQLSMALLINT handle_type, SQLHANDLE handle, SQLSMALLINT completion_type) {
	if (handle_type != SQL_HANDLE_DBC) { // theoretically this can also be done on env but no no no
		return duckdb::SetDiagnosticRecord(static_cast<duckdb::OdbcHandle *>(handle), SQL_ERROR, "SQLEndTran",
		                                   "Invalid handle type, must be SQL_HANDLE_DBC.", SQLStateType::ST_HY092, "");
	}

	duckdb::OdbcHandleDbc *dbc = nullptr;
	SQLRETURN ret = ConvertConnection(handle, dbc);
	if (ret != SQL_SUCCESS) {
		return ret;
	}

	switch (completion_type) {
	case SQL_COMMIT:
		// it needs to materialize the result set because ODBC can still fetch after a commit
		if (dbc->MaterializeResult() != SQL_SUCCESS) {
			// for some reason we couldn't materialize the result set
			return SQL_ERROR; // TODO add a proper error message
		}
		if (dbc->conn->IsAutoCommit()) {
			return SQL_SUCCESS;
		}
		dbc->conn->Commit();
		return SQL_SUCCESS;
	case SQL_ROLLBACK:
		try {
			dbc->conn->Rollback();
			return SQL_SUCCESS;
		} catch (std::exception &ex) {
			duckdb::ErrorData parsed_error(ex);
			return duckdb::SetDiagnosticRecord(dbc, SQL_ERROR, "SQLEndTran", parsed_error.RawMessage(),
			                                   SQLStateType::ST_HY115, dbc->GetDataSourceName());
		}
	default:
		return duckdb::SetDiagnosticRecord(dbc, SQL_ERROR, "SQLEndTran", "Invalid completion type.",
		                                   SQLStateType::ST_HY012, dbc->GetDataSourceName());
	}
}
