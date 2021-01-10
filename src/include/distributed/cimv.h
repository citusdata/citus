#ifndef CIMV_H
#define CIMV_H

#include "postgres.h"
#include "nodes/plannodes.h"

#define CIMV_INTERNAL_SCHEMA "cimv_internal"
#define CITUS_NAMESPACE "citus"
#define MATERIALIZATION_TABLE_SUFFIX "mt"
#define LANDING_TABLE_SUFFIX "ld"
#define REFRESH_VIEW_SUFFIX "rv"

extern bool ProcessCreateMaterializedViewStmt(const CreateTableAsStmt *stmt, const
											  char *query_string, PlannedStmt *pstmt);
extern void ProcessDropMaterializedViewStmt(DropStmt *stmt);
extern void ProcessDropViewStmt(DropStmt *stmt);
extern bool ProcessRefreshMaterializedViewStmt(RefreshMatViewStmt *stmt);
extern void RefreshCimv(Form_pg_cimv formCimv, bool skipData, bool isCreate);

#endif
