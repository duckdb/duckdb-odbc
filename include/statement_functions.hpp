#ifndef STATEMENT_FUNCTIONS_HPP
#define STATEMENT_FUNCTIONS_HPP

#pragma once

#include "duckdb_odbc.hpp"

namespace duckdb {

void PrepareQuery(OdbcHandleStmt *hstmt);

SQLRETURN FinalizeStmt(OdbcHandleStmt *hstmt);

SQLRETURN BatchExecuteStmt(OdbcHandleStmt *hstmt);
SQLRETURN SingleExecuteStmt(OdbcHandleStmt *hstmt);

SQLRETURN FetchStmtResult(OdbcHandleStmt *hstmt, SQLSMALLINT fetch_orientation = SQL_FETCH_NEXT,
                          SQLLEN fetch_offset = 0);

SQLRETURN GetDataStmtResult(OdbcHandleStmt *hstmt, SQLUSMALLINT col_or_param_num, SQLSMALLINT target_type,
                            SQLPOINTER target_value_ptr, SQLLEN buffer_length, SQLLEN *str_len_or_ind_ptr);

SQLRETURN ExecDirectStmt(OdbcHandleStmt *hstmt, const string &query);

SQLRETURN BindParameterStmt(OdbcHandleStmt *hstmt, SQLUSMALLINT parameter_number, SQLSMALLINT input_output_type,
                            SQLSMALLINT value_type, SQLSMALLINT parameter_type, SQLULEN column_size,
                            SQLSMALLINT decimal_digits, SQLPOINTER parameter_value_ptr, SQLLEN buffer_length,
                            SQLLEN *str_len_or_ind_ptr);

SQLRETURN CloseStmt(OdbcHandleStmt *hstmt);
} // namespace duckdb
#endif
