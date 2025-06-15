#ifndef DUCKDB_SESSION_INIT_H
#define DUCKDB_SESSION_INIT_H

#include <string>
  
#include "duckdb_odbc.hpp"

namespace duckdb {

class SessionInitSQLFile {
public:
  static const std::size_t SQL_FILE_MAX_SIZE_BYTES;
  static const std::string CONN_INIT_MARKER;

  std::string db_init_sql;
  std::string conn_init_sql;
  std::string orig_file_text;

  SessionInitSQLFile();

  SessionInitSQLFile(std::string db_init_sql_in, std::string conn_init_sql_in, std::string orig_file_text_in);

  bool IsEmpty();
};

class SessionInit {
public:
  static const std::string SQL_FILE_OPTION;
  static const std::string SQL_FILE_SHA256_OPTION;

  static SessionInitSQLFile ReadSQLFile(const std::string &session_init_sql_file, const std::string &session_init_sql_file_sha256);

  static SQLRETURN Run(OdbcHandleDbc *dbc, const SessionInitSQLFile &sql_file, bool db_created);
};

} // namespace duckdb

#endif // DUCKDB_SESSION_INIT_H 
