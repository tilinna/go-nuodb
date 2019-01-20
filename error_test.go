// Copyright (C) 2013 Timo Linna. All Rights Reserved.

package nuodb

import (
	"testing"
)

func TestErrorString(t *testing.T) {
	err := &Error{
		Code:    ErrorCode(-1),
		Message: "Some sort of error",
	}
	if err.Error() != "nuodb: Some sort of error" {
		t.Fatalf("Unexpected error string: '%s'", err.Error())
	}
}

func TestErrorCodeName(t *testing.T) {
	err := &Error{
		Code:    ErrorCode(-1),
		Message: "Some sort of error",
	}
	if err.Code.Name() != "SYNTAX_ERROR" {
		t.Fatalf("Expected 'SYNTAX_ERROR', got '%s'", err.Code.Name())
	}

	err = &Error{
		Message: "Some sort of error",
	}
	if err.Code.Name() != "UNKNOWN_ERROR" {
		t.Fatalf("Expected 'UNKNOWN_ERROR', got '%s'", err.Code.Name())
	}

	err = &Error{
		Code:    ErrorCode(1000),
		Message: "Some sort of error",
	}
	if err.Code.Name() != "UNKNOWN_ERROR" {
		t.Fatalf("Expected 'UNKNOWN_ERROR', got '%s'", err.Code.Name())
	}
}
