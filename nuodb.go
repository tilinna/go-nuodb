// Copyright (C) 2013 Timo Linna. All Rights Reserved.

package nuodb

// #cgo CPPFLAGS: -I/opt/nuodb/include
// #cgo LDFLAGS: -L. -lcnuodb -L/opt/nuodb/lib64/ -lNuoRemote
// #include "cnuodb.h"
// #include <stdlib.h>
import "C"
import (
	"context"
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
	db  *C.struct_nuodb
	loc *time.Location
}

type Stmt struct {
	c              *Conn
	st             *C.struct_nuodb_statement
	parameterCount C.int
	ddlStatement   bool
}

var _ interface {
	driver.Stmt
	driver.StmtQueryContext
	// driver.StmtExecContext
} = (*Stmt)(nil)

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
			database := fmt.Sprintf("%s@%s", path.Base(url.Path), url.Host)
			username := url.User.Username()
			password, _ := url.User.Password()

			query := url.Query()
			props := make(map[string]string, len(query))
			for key := range query {
				props[key] = query.Get(key) // Get the first value for the key
			}

			conn, err = newConn(database, username, password, props)
		} else {
			err = fmt.Errorf("nuodb: invalid dsn: %s", dsn)
		}
	}
	return
}

func newConn(database, username, password string, props map[string]string) (*Conn, error) {
	location := props["timezone"]
	if location == "" {
		location = "Local"
	}
	loc, err := time.LoadLocation(location)
	if err != nil {
		return nil, fmt.Errorf("nuodb: %s", err)
	}
	c := &Conn{loc: loc}
	C.nuodb_init(&c.db)
	cdatabase := C.CString(database)
	defer C.free(unsafe.Pointer(cdatabase))
	cusername := C.CString(username)
	defer C.free(unsafe.Pointer(cusername))
	cpassword := C.CString(password)
	defer C.free(unsafe.Pointer(cpassword))

	cprops := make([]*C.char, 2*len(props))
	i := 0
	for k, v := range props {
		key := C.CString(k)
		val := C.CString(v)
		defer C.free(unsafe.Pointer(key))
		defer C.free(unsafe.Pointer(val))

		cprops[i] = key
		cprops[i+1] = val
		i += 2
	}

	var cpropsPtr **C.char
	if len(cprops) > 0 {
		cpropsPtr = (**C.char)(unsafe.Pointer(&cprops[0]))
	}
	if rc := C.nuodb_open(c.db, cdatabase, cusername, cpassword, cpropsPtr, C.int(len(cprops))); rc != 0 {
		lastError := c.lastError(rc)
		C.nuodb_close(&c.db)
		return nil, lastError
	}
	return c, nil
}

func (c *Conn) lastError(sqlCode C.int) error {
	if c == nil || c.db == nil {
		return errUninitialized
	}
	return &Error{
		Code:    ErrorCode(sqlCode),
		Message: C.GoString(C.nuodb_error(c.db)),
	}
}

func (c *Conn) Prepare(sql string) (driver.Stmt, error) {
	if c == nil || c.db == nil {
		return nil, errUninitialized
	}
	csql := C.CString(sql)
	defer C.free(unsafe.Pointer(csql))
	stmt := &Stmt{c: c}
	if rc := C.nuodb_statement_prepare(c.db, csql, &stmt.st, &stmt.parameterCount); rc != 0 {
		return nil, c.lastError(rc)
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
	if rc1 := C.nuodb_autocommit(c.db, &tx.autoCommit); rc1 != 0 {
		return nil, c.lastError(rc1)
	} else if rc2 := C.nuodb_autocommit_set(c.db, 0); rc2 != 0 {
		return nil, c.lastError(rc2)
	}
	return tx, nil
}

func (c *Conn) Exec(sql string, args []driver.Value) (driver.Result, error) {
	if len(args) > 0 {
		return nil, driver.ErrSkip
	}
	return c.ExecContext(context.Background(), sql, nil)
}

func (c *Conn) ExecContext(ctx context.Context, sql string, args []driver.NamedValue) (driver.Result, error) {
	if len(args) > 0 {
		return nil, driver.ErrSkip
	}
	csql := C.CString(sql)
	defer C.free(unsafe.Pointer(csql))
	result := &Result{}

	uSec, err := getMicrosecondsUntilDeadline(ctx)
	if err != nil {
		return nil, err
	}

	if rc := C.nuodb_execute(c.db, csql, &result.rowsAffected, &result.lastInsertId, uSec); rc != 0 {
		return nil, c.lastError(rc)
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
			if len(b) > 0 {
				i64 = C.int64_t(uintptr(unsafe.Pointer(&b[0])))
			}
		case []byte:
			vt = C.NUODB_TYPE_BYTES
			i32 = C.int32_t(len(v))
			if len(v) > 0 {
				i64 = C.int64_t(uintptr(unsafe.Pointer(&v[0])))
			}
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
	if rc := C.nuodb_statement_bind(c.db, stmt.st,
		(*C.struct_nuodb_value)(unsafe.Pointer(&parameters[0]))); rc != 0 {
		return c.lastError(rc)
	}
	return nil
}

func (stmt *Stmt) Exec(args []driver.Value) (driver.Result, error) {
	return stmt.execQuery(context.Background(), args)
}

func (stmt *Stmt) ExecQuery(ctx context.Context, args []driver.NamedValue) (driver.Result, error) {
	values, err := namedValuesToValues(args)
	if err != nil {
		return nil, err
	}

	return stmt.execQuery(ctx, values)
}

func (stmt *Stmt) execQuery(ctx context.Context, args []driver.Value) (driver.Result, error) {
	var err error
	c := stmt.c
	if c.db == nil {
		return nil, errClosed
	}
	if err = stmt.bind(args); err != nil {
		return nil, fmt.Errorf("bind: %s", err)
	}
	if err = stmt.addTimeoutFromContext(ctx); err != nil {
		return nil, err
	}
	result := &Result{}
	if rc := C.nuodb_statement_execute(c.db, stmt.st, &result.rowsAffected, &result.lastInsertId); rc != 0 {
		return nil, c.lastError(rc)
	}
	if result.rowsAffected == 0 && stmt.ddlStatement {
		return driver.ResultNoRows, err
	}
	return result, err
}

func (stmt *Stmt) Query(args []driver.Value) (driver.Rows, error) {
	return stmt.queryContext(context.Background(), args)
}

func (stmt *Stmt) QueryContext(ctx context.Context, args []driver.NamedValue) (driver.Rows, error) {
	values, err := namedValuesToValues(args)
	if err != nil {
		return nil, err
	}
	return stmt.queryContext(ctx, values)
}

func (stmt *Stmt) queryContext(ctx context.Context, args []driver.Value) (driver.Rows, error) {
	var err error
	c := stmt.c
	if c.db == nil {
		return nil, errClosed
	}
	if err = stmt.bind(args); err != nil {
		return nil, fmt.Errorf("bind: %s", err)
	}
	if err = stmt.addTimeoutFromContext(ctx); err != nil {
		return nil, err
	}
	rows := &Rows{c: c}
	var columnCount C.int
	if rc := C.nuodb_statement_query(c.db, stmt.st, &rows.rs, &columnCount); rc != 0 {
		return nil, c.lastError(rc)
	}
	if columnCount > 0 {
		cc := int(columnCount)
		rows.rowValues = make([]C.struct_nuodb_value, cc)
		if rc := C.nuodb_resultset_column_names(c.db, rows.rs,
			(*C.struct_nuodb_value)(unsafe.Pointer(&rows.rowValues[0]))); rc != 0 {
			return nil, c.lastError(rc)
		}
		rows.columnNames = make([]string, cc)
		for i, value := range rows.rowValues {
			if length := (C.int)(value.i32); length > 0 {
				cstr := (*C.char)(unsafe.Pointer(uintptr(value.i64)))
				rows.columnNames[i] = C.GoStringN(cstr, length)
			}
		}
	}
	return rows, nil
}

func (stmt *Stmt) addTimeoutFromContext(ctx context.Context) error {
	uSec, err := getMicrosecondsUntilDeadline(ctx)
	if err != nil {
		return err
	}

	C.nuodb_statement_set_query_micros(stmt.c.db, stmt.st, uSec)

	return nil
}

// getMicrosecondsUntilDeadline returns the number of micro seconds until the context's deadline is reached.
// Returns an error if the context is already done.
// N.B. A value of zero means no limit.
func getMicrosecondsUntilDeadline(ctx context.Context) (uSec C.int64_t, err error) {
	if deadline, ok := ctx.Deadline(); ok {
		uSec = C.int64_t(time.Until(deadline).Microseconds())
	}

	if err = ctx.Err(); err != nil {
		return 0, err
	}

	return uSec, nil
}

func namedValuesToValues(namedValues []driver.NamedValue) ([]driver.Value, error) {
	values := make([]driver.Value, 0, len(namedValues))
	for _, namedValue := range namedValues {
		if len(namedValue.Name) != 0 {
			return nil, fmt.Errorf("sql driver doesn't support named values")
		}
		values = append(values, namedValue.Value)
	}
	return values, nil
}

func (stmt *Stmt) Close() error {
	if stmt != nil && stmt.c.db != nil {
		if rc := C.nuodb_statement_close(stmt.c.db, &stmt.st); rc != 0 {
			return stmt.c.lastError(rc)
		}
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
	if len(rows.rowValues) == 0 {
		return io.EOF
	}
	if rc := C.nuodb_resultset_next(c.db, rows.rs, &hasValues,
		(*C.struct_nuodb_value)(unsafe.Pointer(&rows.rowValues[0]))); rc != 0 {
		return c.lastError(rc)
	}
	if hasValues == 0 {
		return io.EOF
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
			dest[i] = time.Unix(seconds, nanos).In(c.loc)
		default:
			// byte slice
			length := (C.int)(value.i32)
			if length > 0 {
				dest[i] = C.GoBytes(unsafe.Pointer((uintptr)(value.i64)), length)
			} else {
				dest[i] = []byte{}
			}
		}
	}
	return nil
}

func (rows *Rows) Close() error {
	if rows != nil && rows.c.db != nil {
		if rc := C.nuodb_resultset_close(rows.c.db, &rows.rs); rc != 0 {
			return rows.c.lastError(rc)
		}
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
	if rc := C.nuodb_commit(tx.c.db); rc != 0 {
		return tx.c.lastError(rc)
	}
	return nil
}

func (tx *Tx) Rollback() error {
	if tx.c.db == nil {
		return errClosed
	}
	defer tx.restoreAutoCommit()
	if rc := C.nuodb_rollback(tx.c.db); rc != 0 {
		return tx.c.lastError(rc)
	}
	return nil
}
