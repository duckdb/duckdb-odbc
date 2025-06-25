#include "session_init.hpp"

#include <regex>
#include <string>
#include <vector>

#include "picosha2.h"

#include "file_io.hpp"
#include "handle_functions.hpp"

namespace duckdb {

const size_t SessionInitSQLFile::SQL_FILE_MAX_SIZE_BYTES = 1 << 20; // 1MB
const std::string SessionInitSQLFile::CONN_INIT_MARKER = "/\\*\\s*DUCKDB_CONNECTION_INIT_BELOW_MARKER\\s*\\*/";

static std::vector<std::string> SplitByMarker(const std::string &str) {
	std::regex regex(SessionInitSQLFile::CONN_INIT_MARKER);
	std::sregex_token_iterator first(str.begin(), str.end(), regex, -1);
	std::sregex_token_iterator last;
	return std::vector<std::string>(first, last);
}

SessionInitSQLFile::SessionInitSQLFile() {
}

SessionInitSQLFile::SessionInitSQLFile(std::string db_init_sql_in, std::string conn_init_sql_in,
                                       std::string orig_file_text_in)
    : db_init_sql(std::move(db_init_sql_in)), conn_init_sql(std::move(conn_init_sql_in)),
      orig_file_text(std::move(orig_file_text_in)) {
}

bool SessionInitSQLFile::IsEmpty() {
	return db_init_sql.empty() && conn_init_sql.empty() && orig_file_text.empty();
}

const std::string SessionInit::SQL_FILE_OPTION = "session_init_sql_file";
const std::string SessionInit::SQL_FILE_SHA256_OPTION = "session_init_sql_file_sha256";

SessionInitSQLFile SessionInit::ReadSQLFile(const std::string &session_init_sql_file,
                                            const std::string &session_init_sql_file_sha256) {
	if (session_init_sql_file.empty()) {
		return SessionInitSQLFile();
	}

	if (!FileExists(session_init_sql_file)) {
		throw FileIOException("Specified session init SQL file not found, path: " + session_init_sql_file);
	}

	size_t file_size = FileSizeBytes(session_init_sql_file);
	if (file_size > SessionInitSQLFile::SQL_FILE_MAX_SIZE_BYTES) {
		throw FileIOException(
		    "Specified session init SQL file size: " + std::to_string(file_size) +
		    " exceeds max allowed size: " + std::to_string(SessionInitSQLFile::SQL_FILE_MAX_SIZE_BYTES));
	}

	std::string orig_file_text = FileReadToString(session_init_sql_file, file_size);

	if (!session_init_sql_file_sha256.empty()) {
		std::string expected_sha256 = session_init_sql_file_sha256;
		std::transform(expected_sha256.begin(), expected_sha256.end(), expected_sha256.begin(),
		               [](char c) { return std::tolower(c); });
		std::vector<unsigned char> digest(picosha2::k_digest_size);
		picosha2::hash256(orig_file_text.begin(), orig_file_text.end(), digest.begin(), digest.end());
		std::string actual_sha256 = picosha2::bytes_to_hex_string(digest.begin(), digest.end());
		if (actual_sha256 != session_init_sql_file_sha256) {
			throw FileIOException("Session init SQL file SHA-256 mismatch, expected: " + expected_sha256 +
			                      ", actual: " + actual_sha256);
		}
	}

	std::vector<std::string> parts = SplitByMarker(orig_file_text);
	if (parts.size() > 2) {
		throw FileIOException("Connection init marker: '" + SessionInitSQLFile::CONN_INIT_MARKER +
		                      "' can only be specified once");
	}

	std::string db_init_sql = OdbcUtils::TrimString(parts.at(0));
	std::string conn_init_sql = parts.size() == 2 ? OdbcUtils::TrimString(parts.at(1)) : std::string();

	return SessionInitSQLFile(std::move(db_init_sql), std::move(conn_init_sql), std::move(orig_file_text));
}

static SQLRETURN RunQueryWithStmt(OdbcHandleDbc *dbc, const std::string &query) {
	HSTMT hstmt = nullptr;

	SQLRETURN alloc_ret = SQLAllocHandle(SQL_HANDLE_STMT, dbc, &hstmt);
	if (!SQL_SUCCEEDED(alloc_ret)) {
		return SetDiagnosticRecord(dbc, alloc_ret, "SQLDriverConnect",
		                           "Connection init error: cannot allocate statement handle, SQL:\n" + query,
		                           SQLStateType::ST_HY000, "");
	}

	SQLRETURN exec_ret = SQLExecDirect(hstmt, reinterpret_cast<SQLCHAR *>(const_cast<char *>(query.c_str())),
	                                   static_cast<SQLSMALLINT>(query.length()));
	if (!SQL_SUCCEEDED(exec_ret)) {
		std::vector<SQLCHAR> sqlstate;
		sqlstate.resize(6);
		SQLINTEGER native_error;
		std::vector<SQLCHAR> msg_buf;
		msg_buf.resize(1024);
		SQLSMALLINT msg_len;

		SQLRETURN diag_ret = SQLGetDiagRec(SQL_HANDLE_STMT, hstmt, 1, sqlstate.data(), &native_error, msg_buf.data(),
		                                   static_cast<SQLSMALLINT>(msg_buf.size()), &msg_len);
		std::string message = "N/A";
		if (SQL_SUCCEEDED(diag_ret)) {
			message = std::string(reinterpret_cast<char *>(sqlstate.data()), sqlstate.size() - 1);
			message += ": ";
			message += std::string(reinterpret_cast<char *>(msg_buf.data()), static_cast<size_t>(msg_len));
		}
		SetDiagnosticRecord(dbc, exec_ret, "SQLDriverConnect", "Connection init error:\n" + message + "\n" + query,
		                    SQLStateType::ST_42000, "");
	}

	SQLFreeStmt(hstmt, SQL_CLOSE);
	SQLFreeHandle(SQL_HANDLE_STMT, hstmt);

	return exec_ret;
}

SQLRETURN SessionInit::Run(OdbcHandleDbc *dbc, const SessionInitSQLFile &sql_file, bool db_created) {
	if (db_created && !sql_file.db_init_sql.empty()) {
		SQLRETURN ret = RunQueryWithStmt(dbc, sql_file.db_init_sql);
		if (!SQL_SUCCEEDED(ret)) {
			return ret;
		}
	}
	if (!sql_file.conn_init_sql.empty()) {
		SQLRETURN ret = RunQueryWithStmt(dbc, sql_file.conn_init_sql);
		if (!SQL_SUCCEEDED(ret)) {
			return ret;
		}
	}
	return SetDiagnosticRecord(dbc, SQL_SUCCESS_WITH_INFO, "SQLDriverConnect",
	                           "Session init SQL:\n" + sql_file.orig_file_text, SQLStateType::ST_01000, "");
}

} // namespace duckdb
