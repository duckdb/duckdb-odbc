#ifndef ROW_DESCRIPTOR_HPP
#define ROW_DESCRIPTOR_HPP

#include "duckdb_odbc.hpp"

namespace duckdb_odbc {
class RowDescriptor {
public:
	explicit RowDescriptor(OdbcHandleStmt *stmt_ptr);
	~RowDescriptor() {
	}
	OdbcHandleDesc *GetIRD();
	OdbcHandleDesc *GetARD();
	void Clear();
	void SetCurrentARD(OdbcHandleDesc *new_ard);
	void Reset();
	void ResetCurrentARD();

public:
	// implicitly allocated descriptors
	duckdb_odbc::unique_ptr<OdbcHandleDesc> ard;
	duckdb_odbc::unique_ptr<OdbcHandleDesc> ird;

private:
	OdbcHandleStmt *stmt;
	// pointer to the current ARD descriptor
	OdbcHandleDesc *cur_ard;
};
} // namespace duckdb_odbc

#endif
