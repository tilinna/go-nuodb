/*
    Copyright (C) 2013 Timo Linna. All Rights Reserved.

    C API wrapper for NuoDB C++ API

    The interface has been designed for Go's CGO in mind, but it should be
    usable from plain C too.

    To reduce CGO function call and memory allocation overhead, the API
    enforces the client to batch operations in:
    - fetch column names
    - bind parameters to a statement
    - fetch row values from a result set
*/
#pragma once

#ifdef __cplusplus
extern "C" {
#endif

#include <stdint.h>

struct nuodb;
struct nuodb_statement;
struct nuodb_resultset;

enum nuodb_value_type {
    NUODB_TYPE_NULL = 0,
    NUODB_TYPE_INT64,
    NUODB_TYPE_FLOAT64,
    NUODB_TYPE_BOOL,
    NUODB_TYPE_STRING, // used only for bind parameter
    NUODB_TYPE_BYTES,
    NUODB_TYPE_TIME
};

struct nuodb_value {
    int64_t i64;
    int32_t i32;
    enum nuodb_value_type vt;
};

void nuodb_init(struct nuodb **db);
const char *nuodb_error(const struct nuodb *db);
int nuodb_open(struct nuodb *db, const char *database, const char *username, const char *password, const char **props, int props_count);
int nuodb_close(struct nuodb **db);

int nuodb_autocommit(struct nuodb *db, int *state);
int nuodb_autocommit_set(struct nuodb *db, int state);
int nuodb_commit(struct nuodb *db);
int nuodb_rollback(struct nuodb *db);
int nuodb_execute(struct nuodb *db, const char *sql, int64_t *rows_affected, int64_t *last_insert_id, int64_t timeout_micro_seconds);

int nuodb_statement_prepare(struct nuodb *db, const char *sql, struct nuodb_statement **st, int *parameter_count);
int nuodb_statement_bind(struct nuodb *db, struct nuodb_statement *st, struct nuodb_value parameters[]);
int nuodb_statement_execute(struct nuodb *db, struct nuodb_statement *st, int64_t *rows_affected, int64_t *last_insert_id);
int nuodb_statement_query(struct nuodb *db, struct nuodb_statement *st, struct nuodb_resultset **rs, int *column_count);
int nuodb_statement_close(struct nuodb *db, struct nuodb_statement **st);
int nuodb_statement_set_query_micros(struct nuodb *db, struct nuodb_statement *st, int64_t timeout_micro_seconds);

int nuodb_resultset_column_names(struct nuodb *db, struct nuodb_resultset *rs, struct nuodb_value names[]);
int nuodb_resultset_next(struct nuodb *db, struct nuodb_resultset *rs, int *has_values, struct nuodb_value values[]);
int nuodb_resultset_close(struct nuodb *db, struct nuodb_resultset **rs);

#ifdef __cplusplus
}
#endif
