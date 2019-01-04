/*
    Copyright (C) 2013 Timo Linna. All Rights Reserved.
*/
#include "cnuodb.h"
#include "NuoDB.h"
#include <cstring>
#include <string>

using namespace NuoDB;

struct nuodb {
    Connection *conn;
    std::string error;
};

static int setError(struct nuodb *db, SQLException &e) {
    db->error.assign(e.getText());
    return e.getSqlcode();
}

static int closeDb(struct nuodb *db) {
    if (db->conn) {
        try {
            db->conn->close();
            db->conn = 0;
        } catch (SQLException &e) {
            return setError(db, e);
        }
    }
    return 0;
}

void nuodb_init(struct nuodb **db) {
    *db = new struct nuodb;
    (*db)->conn = 0;
}

const char *nuodb_error(const struct nuodb *db) {
    return db ? db->error.c_str() : "null db";
}

int nuodb_open(struct nuodb *db, const char *database, const char *username,
               const char *password, const char *schema, const char *timezone) {
    closeDb(db);
    Connection *conn = 0;
    try {
        conn = Connection::create();
        Properties *props = conn->allocProperties(); // TODO: freed on conn->close()?
        props->putValue("user", username);
        props->putValue("password", password);
        if (schema && std::strlen(schema) > 0) {
            props->putValue("schema", schema);
        }
        if (timezone && std::strlen(timezone) > 0) {
            props->putValue("timezone", timezone);
        }
        conn->openDatabase(database, props);
        conn->setAutoCommit(true); // enforce autocommit by default
        db->conn = conn;
        return 0;
    } catch (SQLException &e) {
        if (conn) {
            conn->close();
        }
        return setError(db, e);
    }
}

int nuodb_close(struct nuodb **db) {
    int rc = 0;
    if (db && *db) {
        rc = closeDb(*db);
        delete (*db);
        *db = 0;
    }
    return rc;
}

int nuodb_autocommit(struct nuodb *db, int *state) {
    try {
        *state = db->conn->getAutoCommit();
        return 0;
    } catch (SQLException &e) {
        return setError(db, e);
    }
}

int nuodb_autocommit_set(struct nuodb *db, int state) {
    try {
        db->conn->setAutoCommit(!!state);
        return 0;
    } catch (SQLException &e) {
        return setError(db, e);
    }
}

int nuodb_commit(struct nuodb *db) {
    try {
        db->conn->commit();
        return 0;
    } catch (SQLException &e) {
        return setError(db, e);
    }
}

int nuodb_rollback(struct nuodb *db) {
    try {
        db->conn->rollback();
        return 0;
    } catch (SQLException &e) {
        return setError(db, e);
    }
}

static int fetchExecuteResult(struct nuodb *db, Statement *stmt,
                              int64_t *rows_affected, int64_t *last_insert_id) {
    ResultSet *resultSet = 0;
    try {
        resultSet = stmt->getGeneratedKeys();
        // NuoDB uses -1 as a flag for zero-rows-affected
        *rows_affected = std::max(0, stmt->getUpdateCount());
        if (*rows_affected > 0 && resultSet->getMetaData()->getColumnCount() > 0) {
            while (resultSet->next()) {
                // TODO find out how to read the last id first
            }
            switch (resultSet->getMetaData()->getColumnType(1)) {
                 case NUOSQL_TINYINT:
                 case NUOSQL_SMALLINT:
                 case NUOSQL_INTEGER:
                 case NUOSQL_BIGINT:
                 case NUOSQL_FLOAT:
                 case NUOSQL_DOUBLE:
                 case NUOSQL_NUMERIC:
                 case NUOSQL_DECIMAL:
                    *last_insert_id = resultSet->getLong(1);
                    break;
                default:
                    // This is to avoid a failure when trying to call resultSet->getLong() when
                    // the generated column has a string type and a default sequence. If the user
                    // passes a string that cannot be converted to long, an exception is thrown.
                    //
                    // Since this only happens when the string is user-provided, we don't need to
                    // worry about trying to parse the returned value to return to the user.
                    //
                    // See TestStringSequence for more details.
                    *last_insert_id = 0;
                    break;
            }
        } else {
            *last_insert_id = 0;
        }
        resultSet->close();
        return 0;
    } catch (SQLException &e) {
        if (resultSet) {
            resultSet->close();
        }
        return setError(db, e);
    }
}

int nuodb_execute(struct nuodb *db, const char *sql,
                  int64_t *rows_affected, int64_t *last_insert_id) {
    Statement *stmt = 0;
    try {
        stmt = db->conn->createStatement();
        stmt->executeUpdate(sql, RETURN_GENERATED_KEYS);
        int rc = fetchExecuteResult(db, stmt, rows_affected, last_insert_id);
        stmt->close();
        return rc;
    } catch (SQLException &e) {
        if (stmt) {
            stmt->close();
        }
        return setError(db, e);
    }
}

int nuodb_statement_prepare(struct nuodb *db, const char *sql,
                            struct nuodb_statement **st, int *parameter_count) {
    PreparedStatement *stmt = 0;
    try {
        stmt = db->conn->prepareStatement(sql, RETURN_GENERATED_KEYS);
        *parameter_count = stmt->getParameterMetaData()->getParameterCount();
        *st = reinterpret_cast<struct nuodb_statement *>(stmt);
        return 0;
    } catch (SQLException &e) {
        if (stmt) {
            stmt->close();
        }
        return setError(db, e);
    }
}

int nuodb_statement_bind(struct nuodb *db, struct nuodb_statement *st,
                         struct nuodb_value parameters[]) {
    PreparedStatement *stmt = reinterpret_cast<PreparedStatement *>(st);
    try {
        int parameterCount = stmt->getParameterMetaData()->getParameterCount();
        for (int i=0; i < parameterCount; ++i) {
            int parameterIndex = i+1;
            switch (parameters[i].vt) {
                case NUODB_TYPE_NULL:
                    stmt->setNull(parameterIndex, NUOSQL_NULL);
                    break;
                case NUODB_TYPE_INT64:
                    stmt->setLong(parameterIndex, parameters[i].i64);
                    break;
                case NUODB_TYPE_FLOAT64: {
                    union {
                        int64_t i64;
                        double float64;
                    } value = { parameters[i].i64 };
                    stmt->setDouble(parameterIndex, value.float64);
                    break;
                }
                case NUODB_TYPE_BOOL:
                    stmt->setBoolean(parameterIndex, !!parameters[i].i64);
                    break;
                case NUODB_TYPE_STRING: {
                    size_t length = parameters[i].i32;
                    const char *s = reinterpret_cast<const char*>(parameters[i].i64);
                    // Extra conversion due to missing length param in the setString API
                    const std::string str(s, length);
                    stmt->setString(parameterIndex, str.c_str());
                    break;
                }
                case NUODB_TYPE_BYTES: {
                    int length = parameters[i].i32;
                    const unsigned char *bytes = reinterpret_cast<const unsigned char*>(parameters[i].i64);
                    stmt->setBytes(parameterIndex, length, bytes);
                    break;
                }
                case NUODB_TYPE_TIME: {
                    int64_t seconds = parameters[i].i64;
                    int32_t nanos = parameters[i].i32;
                    SqlTimestamp ts(seconds, nanos);
                    stmt->setTimestamp(parameterIndex, &ts);
                    break;
                }
            }
        }
        return 0;
    } catch  (SQLException &e) {
        return setError(db, e);
    }
}

int nuodb_statement_execute(struct nuodb *db, struct nuodb_statement *st,
                            int64_t *rows_affected, int64_t *last_insert_id) {
    PreparedStatement *stmt = reinterpret_cast<PreparedStatement *>(st);
    try {
        stmt->executeUpdate();
        return fetchExecuteResult(db, stmt, rows_affected, last_insert_id);
    } catch (SQLException &e) {
        return setError(db, e);
    }
}

int nuodb_statement_query(struct nuodb *db, struct nuodb_statement *st,
                          struct nuodb_resultset **rs, int *column_count) {
    ResultSet *resultSet = 0;
    PreparedStatement *stmt = reinterpret_cast<PreparedStatement *>(st);
    try {
        bool hasResults = stmt->execute();
        if (hasResults) {
            resultSet = stmt->getResultSet();
        } else {
            resultSet = stmt->getGeneratedKeys();
        }
        *column_count = resultSet->getMetaData()->getColumnCount();
        *rs = reinterpret_cast<struct nuodb_resultset *>(resultSet);
        return 0;
    } catch (SQLException &e) {
        if (resultSet) {
            resultSet->close();
        }
        return setError(db, e);
    }
}

int nuodb_statement_close(struct nuodb *db, struct nuodb_statement **st) {
    try {
        if (st && *st) {
            PreparedStatement *stmt = reinterpret_cast<PreparedStatement *>(*st);
            stmt->close();
            *st = 0;
        }
        return 0;
    } catch (SQLException &e) {
        return setError(db, e);
    }
}

int nuodb_resultset_column_names(struct nuodb *db, struct nuodb_resultset *rs,
                                 struct nuodb_value names[]) {
    ResultSet *resultSet = reinterpret_cast<ResultSet *>(rs);
    try {
        ResultSetMetaData *resultSetMetaData = resultSet->getMetaData();
        int columnCount = resultSetMetaData->getColumnCount();
        for (int i=0; i < columnCount; ++i) {
            int columnIndex = i+1;
            const char *string = resultSetMetaData->getColumnLabel(columnIndex);
            names[i].i64 = reinterpret_cast<int64_t>(string);
            names[i].i32 = std::strlen(string);
        }
        return 0;
    } catch (SQLException &e) {
        return setError(db, e);
    }
}

int nuodb_resultset_next(struct nuodb *db, struct nuodb_resultset *rs,
                         int *has_values, struct nuodb_value values[]) {
    ResultSet *resultSet = reinterpret_cast<ResultSet *>(rs);
    try {
        *has_values = resultSet->next();
        if (*has_values) {
            ResultSetMetaData *resultSetMetaData = resultSet->getMetaData();
            int columnCount = resultSetMetaData->getColumnCount();
            for (int i=0; i < columnCount; ++i) {
                int64_t i64 = 0;
                int32_t i32 = 0;
                enum nuodb_value_type vt = NUODB_TYPE_NULL;
                int columnIndex = i+1;
                switch (resultSetMetaData->getColumnType(columnIndex)) {
                    case NUOSQL_NULL:
                        vt = NUODB_TYPE_NULL;
                        break;
                    case NUOSQL_TINYINT:
                    case NUOSQL_SMALLINT:
                    case NUOSQL_INTEGER:
                    case NUOSQL_BIGINT:
                        if (resultSetMetaData->getScale(columnIndex) == 0) {
                            i64 = resultSet->getLong(columnIndex);
                            if (!resultSet->wasNull()) {
                                vt = NUODB_TYPE_INT64;
                            }
                            break;
                        }
                        // fallthrough; must be fetched as a string
                    case NUOSQL_NUMERIC:
                    case NUOSQL_DECIMAL: {
                        const char *string = resultSet->getString(columnIndex);
                        if (!resultSet->wasNull()) {
                            vt = NUODB_TYPE_BYTES; // strings are returned as bytes
                            i64 = reinterpret_cast<int64_t>(string);
                            i32 = std::strlen(string);
                        }
                        break;
                    }
                    case NUOSQL_FLOAT:
                    case NUOSQL_DOUBLE: {
                        union {
                            double float64;
                            int64_t i64;
                        } value = { resultSet->getDouble(columnIndex) };
                        if (!resultSet->wasNull()) {
                            vt = NUODB_TYPE_FLOAT64;
                            i64 = value.i64;
                        }
                        break;
                    }
                    case NUOSQL_BIT:
                    case NUOSQL_BOOLEAN:
                        i64 = resultSet->getBoolean(columnIndex);
                        if (!resultSet->wasNull()) {
                            vt = NUODB_TYPE_BOOL;
                        }
                        break;
                    case NUOSQL_DATE:
                    case NUOSQL_TIME:
                    case NUOSQL_TIMESTAMP: {
                        Timestamp *ts = resultSet->getTimestamp(columnIndex);
                        if (ts && !resultSet->wasNull()) {
                            vt = NUODB_TYPE_TIME;
                            i64 = ts->getSeconds();
                            i32 = ts->getNanos();
                        }
                        break;
                    }
                    default: {
                        const Bytes b = resultSet->getBytes(columnIndex);
                        if (!resultSet->wasNull()) {
                            vt = NUODB_TYPE_BYTES;
                            i64 = reinterpret_cast<int64_t>(b.data);
                            i32 = b.length;
                        }
                        break;
                    }
                }
                values[i].i64 = i64;
                values[i].i32 = i32;
                values[i].vt = vt;
            }
        }
        return 0;
    } catch (SQLException &e) {
        return setError(db, e);
    }
}

int nuodb_resultset_close(struct nuodb *db, struct nuodb_resultset **rs) {
    try {
        if (rs && *rs) {
            ResultSet *resultSet = reinterpret_cast<ResultSet *>(*rs);
            resultSet->close();
            *rs = 0;
        }
        return 0;
    } catch (SQLException &e) {
        return setError(db, e);
    }
}
