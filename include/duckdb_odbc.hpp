#ifndef DUCKDB_ODBC_HPP
#define DUCKDB_ODBC_HPP

// needs to be first because BOOL
#include "odbc_common.hpp"

#include "descriptor.hpp"
#include "odbc_diagnostic.hpp"
#include "odbc_utils.hpp"

#include <sqltypes.h>
#include <sqlext.h>

namespace duckdb_odbc {

class OdbcFetch;
class ParameterDescriptor;
class RowDescriptor;
class OdbcDiagnostic;

enum OdbcHandleType { ENV, DBC, STMT, DESC };
std::string OdbcHandleTypeToString(OdbcHandleType type);

struct OdbcHandle {
	explicit OdbcHandle(OdbcHandleType type_p);
	OdbcHandle(const OdbcHandle &other);
	OdbcHandle &operator=(const OdbcHandle &other);

	OdbcHandleType type;

	duckdb_odbc::unique_ptr<OdbcDiagnostic> odbc_diagnostic;
};

struct OdbcHandleEnv : public OdbcHandle {
	OdbcHandleEnv() : OdbcHandle(OdbcHandleType::ENV) {
	}

	SQLINTEGER odbc_version;
	SQLUINTEGER connection_pooling;
	SQLUINTEGER cp_match;
	SQLINTEGER output_nts;
};

struct OdbcHandleStmt;
struct OdbcHandleDesc;

struct OdbcHandleDbc : public OdbcHandle {
public:
	explicit OdbcHandleDbc(OdbcHandleEnv *env_p)
	    : OdbcHandle(OdbcHandleType::DBC), env(env_p), autocommit(true), sql_attr_access_mode(SQL_MODE_READ_WRITE) {
		D_ASSERT(env_p);
	};
	~OdbcHandleDbc();
	void EraseStmtRef(OdbcHandleStmt *stmt);
	//! Moves every open result set of the connection's statements, except `except`'s, into the statement that owns
	//! it, so that the connection can run another statement: DuckDB runs one statement at a time per connection.
	SQLRETURN MaterializeResults(OdbcHandleStmt *except = nullptr);
	void ResetStmtDescriptors(OdbcHandleDesc *old_desc);

	void SetDatabaseName(const string &db_name);
	std::string GetDatabaseName();
	std::string GetDataSourceName();

	//! Executes a statement on the connection for the driver itself, discarding any result. Throws on error.
	void ExecuteInternal(const std::string &sql);
	//! Switches between auto-commit and manual-commit mode (SQL_ATTR_AUTOCOMMIT). Throws on error.
	void SetAutoCommit(bool autocommit_p);
	//! Commits (or rolls back) the open transaction, starting the next one in manual-commit mode. Throws on error.
	void EndTransaction(bool commit);
	//! Called after the driver ran a statement: a COMMIT or ROLLBACK the application issued itself ends the open
	//! transaction, so in manual-commit mode the next one is started.
	void AfterStatement(const cxx::SqlStatement &statement);

public:
	OdbcHandleEnv *env;
	//! The database the connection is open on. Declared before `conn`, which must not outlive it.
	shared_ptr<cxx::Instance> db;
	duckdb_odbc::unique_ptr<cxx::Connection> conn;
	bool autocommit;
	SQLUINTEGER sql_attr_metadata_id;
	SQLUINTEGER sql_attr_access_mode;
	// this is the database name, see: SQLSetConnectAttr
	std::string sql_attr_current_catalog;
	// this is DSN get in string connection, see: SQLConnect
	// Ex: "DSN=DuckDB"
	std::string dsn;
	// reference to an open statement handled by this connection
	vector<OdbcHandleStmt *> vec_stmt_ref;
};

struct OdbcBoundCol {
	OdbcBoundCol() : type(SQL_UNKNOWN_TYPE), ptr(nullptr), len(0), strlen_or_ind(nullptr) {};

	bool IsBound() {
		return ptr != nullptr;
	}

	bool IsVarcharBound() {
		if (OdbcUtils::IsCharType(type)) {
			return strlen_or_ind != nullptr;
		}
		return false;
	}

	SQLSMALLINT type;
	SQLPOINTER ptr;
	SQLLEN len;
	SQLLEN *strlen_or_ind;
};

struct OdbcStmtClientAttrs {
public:
	SQLULEN max_len = 0;
};

//! A statement prepared by SQLPrepare or SQLExecDirect, with what the driver learned about it before executing it.
struct OdbcPreparedStatement {
	bool HasError() const {
		return !error.empty();
	}
	const std::string &GetError() const {
		return error;
	}
	cxx::StatementType GetStatementType() const {
		return statement_type;
	}
	idx_t ParameterCount() const {
		return parameter_count;
	}
	//! Executes the statement with positional parameters.
	unique_ptr<QueryResult> Execute(cxx::Connection &conn, const vector<Value> &values);

	//! The parsed statement.
	unique_ptr<cxx::SqlStatement> statement;
	//! The statement prepared for repeated execution, by SQLPrepare. SQLExecDirect executes `statement` directly.
	unique_ptr<cxx::PreparedStatement> prepared;
	//! Why the statement could not be prepared; empty when it was.
	std::string error;
	cxx::StatementType statement_type = cxx::StatementType::INVALID;
	idx_t parameter_count = 0;
	//! Whether the types of the parameters and of the result columns are known before executing. They are not when a
	//! parameter's type cannot be inferred, as in `SELECT ?`, and then come from each execution's result.
	bool bound_all_parameters = false;
	//! Whether to report a statement whose parameter types are unknown with SQL_SUCCESS_WITH_INFO.
	bool warn_unbound_parameters = false;
	//! The parameter types, in parameter order, when `bound_all_parameters`.
	vector<OdbcType> parameter_types;
	//! The result columns, when `bound_all_parameters`.
	vector<OdbcType> types;
	vector<std::string> names;
};

struct OdbcHandleStmt : public OdbcHandle {
public:
	explicit OdbcHandleStmt(OdbcHandleDbc *dbc_p);
	~OdbcHandleStmt();
	void Close();
	//! Reads the rest of the open result set into the fetcher and releases it, see OdbcHandleDbc::MaterializeResults.
	SQLRETURN MaterializeResult();
	void SetARD(OdbcHandleDesc *new_ard);
	void SetAPD(OdbcHandleDesc *new_apd);
	bool IsPrepared() {
		return stmt != nullptr;
	}
	void FillIRD(const vector<OdbcType> &types, const vector<std::string> &names);
	//! Whether the statement produces a result set (as opposed to a row count or nothing).
	bool ReturnsRows();
	//! The kind of statement, as executed when there is a result and as written otherwise.
	cxx::StatementType GetStatementType();

public:
	OdbcHandleDbc *dbc;
	duckdb_odbc::unique_ptr<OdbcPreparedStatement> stmt;
	//! The result of the last execution, while it is still being read from DuckDB. The fetcher holds the chunks read.
	duckdb_odbc::unique_ptr<QueryResult> res;
	//! Whether the statement was executed and has a result (that may since have been read in full).
	bool has_result = false;
	//! Why executing the statement, or reading its result, failed; empty when it did not.
	std::string result_error;
	cxx::ResultType result_type = cxx::ResultType::NOTHING;
	cxx::StatementType result_statement_type = cxx::StatementType::INVALID;
	vector<OdbcType> result_types;
	vector<std::string> result_names;

	vector<OdbcBoundCol> bound_cols;
	bool open;
	SQLULEN retrieve_data = SQL_RD_ON;
	SQLULEN *rows_fetched_ptr;

	// statement attributes required by clients but not used by the engine
	OdbcStmtClientAttrs client_attrs;

	// fetcher
	duckdb_odbc::unique_ptr<OdbcFetch> odbc_fetcher;

	duckdb_odbc::unique_ptr<ParameterDescriptor> param_desc;

	duckdb_odbc::unique_ptr<RowDescriptor> row_desc;
};

struct OdbcHandleDesc : public OdbcHandle {
	//! https://docs.microsoft.com/en-us/sql/odbc/reference/develop-app/descriptors?view=sql-server-ver15
	// TODO requires full implmentation
public:
	explicit OdbcHandleDesc(OdbcHandleDbc *dbc_ptr = nullptr, OdbcHandleStmt *stmt_ptr = nullptr,
	                        bool explicit_desc = false)
	    : OdbcHandle(OdbcHandleType::DESC), dbc(dbc_ptr), stmt(stmt_ptr) {
		header.sql_desc_alloc_type = SQL_DESC_ALLOC_AUTO;
		if (explicit_desc) {
			header.sql_desc_alloc_type = SQL_DESC_ALLOC_USER;
		}
	}
	OdbcHandleDesc(const OdbcHandleDesc &other);
	~OdbcHandleDesc() {
	}
	OdbcHandleDesc &operator=(const OdbcHandleDesc &other);
	void CopyOnlyOdbcFields(const OdbcHandleDesc &other);
	void CopyFieldByField(const OdbcHandleDesc &other);

	SQLRETURN ReturnInvalidFieldIdentifier(bool is_readonly);
	DescRecord *GetDescRecord(idx_t param_idx);
	SQLRETURN SetDescField(SQLSMALLINT rec_number, SQLSMALLINT field_identifier, SQLPOINTER value_ptr,
	                       SQLINTEGER buffer_length);

	idx_t GetRecordCount() {
		return records.size();
	}

	void Clear();
	void Reset();
	void Copy(OdbcHandleDesc &other);

	// verify Implementation Descriptor (ID)
	bool IsID();
	// verify Application Descriptor (AD)
	bool IsAD();
	bool IsIRD();
	bool IsIPD();

	void AddMoreRecords(SQLSMALLINT new_size);

public:
	DescHeader header;
	vector<DescRecord> records;
	OdbcHandleDbc *dbc;
	OdbcHandleStmt *stmt;
};

} // namespace duckdb_odbc

#endif // DUCKDB_ODBC_HPP
