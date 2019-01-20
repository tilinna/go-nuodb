// Copyright (C) 2013 Timo Linna. All Rights Reserved.

package nuodb

import (
	"fmt"
)

// Error is an error type which represents a single instance of a NuoDB error
type Error struct {
	Code    ErrorCode
	Message string
}

func (e *Error) Error() string {
	return fmt.Sprintf("nuodb: %s", e.Message)
}

// ErrorCode represents an error defined by NuoDB
// Definitions can be found here: http://doc.nuodb.com/Latest/Default.htm#SQL-Error-Codes.htm
type ErrorCode int

// Name returns a short name for the error code
func (c *ErrorCode) Name() string {
	name, ok := errorCodeNames[*c]
	if !ok {
		return "UNKNOWN_ERROR"
	}
	return name
}

var errorCodeNames = map[ErrorCode]string{
	-1:  "SYNTAX_ERROR",
	-2:  "FEATURE_NOT_YET_IMPLEMENTED",
	-3:  "BUG_CHECK",
	-4:  "COMPILE_ERROR",
	-5:  "RUNTIME_ERROR",
	-6:  "OCS_ERROR",
	-7:  "NETWORK_ERROR",
	-8:  "CONVERSION_ERROR",
	-9:  "TRUNCATION_ERROR",
	-10: "CONNECTION_ERROR",
	-11: "DDL_ERROR",
	-12: "APPLICATION_ERROR",
	-13: "SECURITY_ERROR",
	-14: "DATABASE_CORRUPTION",
	-15: "VERSION_ERROR",
	-16: "LICENSE_ERROR",
	-17: "INTERNAL_ERROR",
	-18: "DEBUG_ERROR",
	-19: "LOST_BLOB",
	-20: "INCONSISTENT_BLOB",
	-21: "DELETED_BLOB",
	-22: "LOG_ERROR",
	-23: "DATABASE_DAMAGED",
	-24: "UPDATE_CONFLICT",
	-25: "NO_SUCH_TABLE",
	-26: "INDEX_OVERFLOW",
	-27: "UNIQUE_DUPLICATE",
	-29: "DEADLOCK",
	-30: "OUT_OF_MEMORY_ERROR",
	-31: "OUT_OF_RECORD_MEMORY_ERROR",
	-32: "LOCK_TIMEOUT",
	-36: "PLATFORM_ERROR",
	-37: "NO_SCHEMA",
	-38: "CONFIGURATION_ERROR",
	-39: "READ_ONLY_ERROR",
	-40: "NO_GENERATED_KEYS",
	-41: "THROWN_EXCEPTION",
	-42: "INVALID_TRANSACTION_ISOLATION",
	-43: "UNSUPPORTED_TRANSACTION_ISOLATION",
	-44: "INVALID_UTF8",
	-45: "CONSTRAINT_ERROR",
	-46: "UPDATE_ERROR",
	-47: "I18N_ERROR",
	-48: "OPERATION_KILLED",
	-49: "INVALID_STATEMENT",
	-50: "IS_SHUTDOWN",
	-51: "IN_QUOTED_STRING",
	-52: "BATCH_UPDATE_ERROR",
	-53: "JAVA_ERROR",
	-54: "INVALID_FIELD",
	-55: "INVALID_INDEX_NULL",
	-56: "INVALID_OPERATION",
	-57: "INVALID_STATISTICS",
	-58: "INVALID_GENERATOR",
	-59: "OPERATION_TIMEOUT",
	-60: "NO_SUCH_INDEX",
	-61: "NO_SUCH_SEQUENCE",
	-62: "XAER_PROTO",
	-63: "UNKNOWN_ERROR",
}
