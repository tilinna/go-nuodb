// Copyright (C) 2013 Timo Linna. All Rights Reserved.
package nuodb

// #cgo LDFLAGS: -L. -lcnuodb -L/opt/nuodb/lib64/ -lNuoRemote
// #include "cnuodb.h"
// #include <stdlib.h>
import "C"
import (
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"io"
	"net/url"
	"path"
	"regexp"
	"time"
	"unsafe"
)

type nuodbDriver struct{}

type Conn struct {
	db *C.struct_nuodb
}

type Stmt struct {
	c              *Conn
	st             *C.struct_nuodb_statement
	parameterCount C.int
	ddlStatement   bool
}

type Result struct {
	rowsAffected C.int64_t
	lastInsertId C.int64_t
}

type Rows struct {
	c           *Conn
	rs          *C.struct_nuodb_resultset
	rowValues   []C.struct_nuodb_value
	columnNames []string
}

type Tx struct {
	c          *Conn
	autoCommit C.int
}

var errUninitialized = errors.New("nuodb: uninitialized connection")
var errClosed = errors.New("nuodb: connection is closed")

var dmlStatementRegexp = regexp.MustCompile(`^\s*(?i:DELETE|EXPLAIN|INSERT|REPLACE|SELECT|TRUNCATE|UPDATE)\s+`)

func ddlStatement(sql string) bool {
	return !dmlStatementRegexp.MatchString(sql)
}

func init() {
	sql.Register("nuodb", &nuodbDriver{})
}

func (d *nuodbDriver) Open(dsn string) (conn driver.Conn, err error) {
	var url *url.URL
	if url, err = url.Parse(dsn); err == nil {
		if url.Scheme == "nuodb" && url.User != nil {
			query := url.Query()
			database := fmt.Sprintf("%s@%s", path.Base(url.Path), url.Host)
			username := url.User.Username()
			password, _ := url.User.Password()
			schema := query.Get("schema")
			timezone := query.Get("timezone")
			conn, err = newConn(database, username, password, schema, timezone)
		} else {
			err = fmt.Errorf("nuodb: invalid dsn: %s", dsn)
		}
	}
	return
}

func newConn(database, username, password, schema, timezone string) (*Conn, error) {
	c := &Conn{}
	C.nuodb_init(&c.db)
	cdatabase := C.CString(database)
	defer C.free(unsafe.Pointer(cdatabase))
	cusername := C.CString(username)
	defer C.free(unsafe.Pointer(cusername))
	cpassword := C.CString(password)
	defer C.free(unsafe.Pointer(cpassword))
	cschema := C.CString(schema)
	defer C.free(unsafe.Pointer(cschema))
	ctimezone := C.CString(timezone)
	defer C.free(unsafe.Pointer(ctimezone))
	if C.nuodb_open(c.db, cdatabase, cusername, cpassword, cschema, ctimezone) != 0 {
		lastError := c.lastError()
		C.nuodb_close(&c.db)
		return nil, lastError
	}
	return c, nil
}

func (c *Conn) lastError() error {
	if c == nil || c.db == nil {
		return errUninitialized
	}
	return fmt.Errorf("nuodb: %s", C.GoString(C.nuodb_error(c.db)))
}

func (c *Conn) Prepare(sql string) (driver.Stmt, error) {
	if c == nil || c.db == nil {
		return nil, errUninitialized
	}
	csql := C.CString(sql)
	defer C.free(unsafe.Pointer(csql))
	stmt := &Stmt{c: c}
	if C.nuodb_statement_prepare(c.db, csql, &stmt.st, &stmt.parameterCount) != 0 {
		return nil, c.lastError()
	}
	stmt.ddlStatement = ddlStatement(sql)
	return stmt, nil
}

func (c *Conn) Begin() (driver.Tx, error) {
	if c == nil || c.db == nil {
		return nil, errUninitialized
	}
	tx := &Tx{c: c}
	// TODO: should use "START TRANSACTION"
	if C.nuodb_autocommit(c.db, &tx.autoCommit) != 0 ||
		C.nuodb_autocommit_set(c.db, 0) != 0 {
		return nil, c.lastError()
	}
	return tx, nil
}

func (c Conn) Exec(sql string, args []driver.Value) (driver.Result, error) {
	if len(args) > 0 {
		return nil, driver.ErrSkip
	}
	csql := C.CString(sql)
	defer C.free(unsafe.Pointer(csql))
	result := &Result{}
	if C.nuodb_execute(c.db, csql, &result.rowsAffected, &result.lastInsertId) != 0 {
		return nil, c.lastError()
	}
	if result.rowsAffected == 0 && ddlStatement(sql) {
		return driver.ResultNoRows, nil
	}
	return result, nil
}

func (c *Conn) Close() error {
	if c != nil && c.db != nil {
		if rc := C.nuodb_close(&c.db); rc != 0 {
			// can't use lastError here
			return fmt.Errorf("nuodb: conn close failed: %d", rc)
		}
	}
	return nil
}

func (stmt *Stmt) NumInput() int {
	return int(stmt.parameterCount)
}

func (stmt *Stmt) bind(args []driver.Value) error {
	c := stmt.c
	parameterCount := int(stmt.parameterCount)
	if parameterCount == 0 || len(args) == 0 {
		return nil
	}
	parameters := make([]C.struct_nuodb_value, parameterCount)
	for i, v := range args {
		if i >= parameterCount {
			break // go1.0.3 allowed extra args; ignore
		}
		var vt C.enum_nuodb_value_type
		var i32 C.int32_t
		var i64 C.int64_t
		switch v := v.(type) {
		case int64:
			vt = C.NUODB_TYPE_INT64
			i64 = C.int64_t(v)
		case float64:
			vt = C.NUODB_TYPE_FLOAT64
			i64 = *(*C.int64_t)(unsafe.Pointer(&v))
		case bool:
			vt = C.NUODB_TYPE_BOOL
			if v {
				i64 = 1
			} else {
				i64 = 0
			}
		case string:
			vt = C.NUODB_TYPE_STRING
			b := []byte(v)
			args[i] = b // ensure the b is not GC'ed before the _bind
			i32 = C.int32_t(len(v))
			i64 = C.int64_t(uintptr(unsafe.Pointer(&b[0])))
		case []byte:
			vt = C.NUODB_TYPE_BYTES
			i32 = C.int32_t(len(v))
			i64 = C.int64_t(uintptr(unsafe.Pointer(&v[0])))
		case time.Time:
			vt = C.NUODB_TYPE_TIME
			i32 = C.int32_t(v.Nanosecond())
			i64 = C.int64_t(v.Unix()) // seconds
		default:
			vt = C.NUODB_TYPE_NULL
		}
		parameters[i].i64 = i64
		parameters[i].i32 = i32
		parameters[i].vt = vt
	}
	if C.nuodb_statement_bind(c.db, stmt.st,
		(*C.struct_nuodb_value)(unsafe.Pointer(&parameters[0]))) != 0 {
		return c.lastError()
	}
	return nil
}

func (stmt *Stmt) Exec(args []driver.Value) (driver.Result, error) {
	var err error
	c := stmt.c
	if c.db == nil {
		return nil, errClosed
	}
	if err = stmt.bind(args); err != nil {
		return nil, fmt.Errorf("bind: %s", err)
	}
	result := &Result{}
	var resultSet *C.struct_nuodb_resultset
	var columnCount C.int
	if C.nuodb_statement_execute(c.db, stmt.st, &resultSet, &columnCount, &result.rowsAffected) != 0 {
		return nil, c.lastError()
	}
	if !stmt.ddlStatement && result.rowsAffected > 0 && columnCount == 1 &&
		C.nuodb_resultset_last_insert_id(c.db, resultSet, &result.lastInsertId) != 0 {
		err = c.lastError()
	}
	// Always close the resultSet, but retain previous err value
	if C.nuodb_resultset_close(c.db, &resultSet) != 0 && err == nil {
		err = c.lastError()
	}
	if stmt.ddlStatement {
		return driver.ResultNoRows, err
	}
	return result, err
}

func (stmt *Stmt) Query(args []driver.Value) (driver.Rows, error) {
	var err error
	c := stmt.c
	if c.db == nil {
		return nil, errClosed
	}
	if err = stmt.bind(args); err != nil {
		return nil, fmt.Errorf("bind: %s", err)
	}
	rows := &Rows{c: c}
	var columnCount C.int
	var rowsAffected C.int64_t
	if C.nuodb_statement_execute(c.db, stmt.st, &rows.rs, &columnCount, &rowsAffected) != 0 {
		return nil, c.lastError()
	}
	if columnCount > 0 {
		cc := int(columnCount)
		rows.rowValues = make([]C.struct_nuodb_value, cc)
		if C.nuodb_resultset_column_names(c.db, rows.rs,
			(*C.struct_nuodb_value)(unsafe.Pointer(&rows.rowValues[0]))) != 0 {
			return nil, c.lastError()
		}
		rows.columnNames = make([]string, cc)
		for i, value := range rows.rowValues {
			rows.columnNames[i] = C.GoString((*C.char)(unsafe.Pointer(uintptr(value.i64))))
		}
	}
	return rows, nil
}

func (stmt *Stmt) Close() error {
	if stmt != nil && stmt.c.db != nil && C.nuodb_statement_close(stmt.c.db, &stmt.st) != 0 {
		return stmt.c.lastError()
	}
	return nil
}

func (result *Result) LastInsertId() (int64, error) {
	return int64(result.lastInsertId), nil
}

func (result *Result) RowsAffected() (int64, error) {
	return int64(result.rowsAffected), nil
}

func (rows *Rows) Columns() []string {
	return rows.columnNames
}

func (rows *Rows) Next(dest []driver.Value) error {
	c := rows.c
	var hasValues C.int
	var bytesCount C.int
	if len(rows.rowValues) == 0 {
		return io.EOF
	}
	if C.nuodb_resultset_next(c.db, rows.rs, &hasValues, &bytesCount,
		(*C.struct_nuodb_value)(unsafe.Pointer(&rows.rowValues[0]))) != 0 {
		return c.lastError()
	}
	if hasValues == 0 {
		return io.EOF
	}
	var rowBytes []byte
	var rowBytesOffset int
	if bytesCount > 0 {
		rowBytes = make([]byte, bytesCount)
		if C.nuodb_resultset_bytes(c.db, rows.rs,
			(*C.uchar)(unsafe.Pointer(&rowBytes[0]))) != 0 {
			return c.lastError()
		}
	}
	for i, value := range rows.rowValues {
		switch value.vt {
		case C.NUODB_TYPE_NULL:
			dest[i] = nil
		case C.NUODB_TYPE_INT64:
			dest[i] = int64(value.i64)
		case C.NUODB_TYPE_FLOAT64:
			dest[i] = *(*float64)(unsafe.Pointer(&value.i64))
		case C.NUODB_TYPE_BOOL:
			dest[i] = value.i64 != 0
		case C.NUODB_TYPE_TIME:
			seconds := int64(value.i64)
			nanos := int64(value.i32)
			dest[i] = time.Unix(seconds, nanos).UTC()
		default:
			// byte slice
			length := int(value.i32)
			if length > 0 {
				dest[i] = rowBytes[rowBytesOffset : rowBytesOffset+length]
				rowBytesOffset += length
			} else {
				dest[i] = []byte{}
			}
		}
	}
	return nil
}

func (rows *Rows) Close() error {
	if rows != nil && rows.c.db != nil &&
		C.nuodb_resultset_close(rows.c.db, &rows.rs) != 0 {
		return rows.c.lastError()
	}
	return nil
}

func (tx *Tx) restoreAutoCommit() {
	_ = C.nuodb_autocommit_set(tx.c.db, tx.autoCommit)
}

func (tx *Tx) Commit() error {
	if tx.c.db == nil {
		return errClosed
	}
	defer tx.restoreAutoCommit()
	if C.nuodb_commit(tx.c.db) != 0 {
		return tx.c.lastError()
	}
	return nil
}

func (tx *Tx) Rollback() error {
	if tx.c.db == nil {
		return errClosed
	}
	defer tx.restoreAutoCommit()
	if C.nuodb_rollback(tx.c.db) != 0 {
		return tx.c.lastError()
	}
	return nil
}
