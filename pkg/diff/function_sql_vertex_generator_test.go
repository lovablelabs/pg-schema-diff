package diff

import (
	"strings"
	"testing"

	"github.com/stripe/pg-schema-diff/internal/schema"
)

func TestHasReturnTypeChanged(t *testing.T) {
	tests := []struct {
		name           string
		oldReturnType  string
		newReturnType  string
		expectedChange bool
	}{
		{
			name:           "OUT parameters changed - added column",
			oldReturnType:  "TABLE(total_pages integer, total_books integer)",
			newReturnType:  "TABLE(total_pages integer, total_books integer, avg_pages_per_book numeric)",
			expectedChange: true,
		},
		{
			name:           "Simple RETURNS type changed",
			oldReturnType:  "integer",
			newReturnType:  "text",
			expectedChange: true,
		},
		{
			name:           "RETURNS TABLE columns changed",
			oldReturnType:  "TABLE(total_books_read bigint, total_pages_read bigint, total_minutes_read bigint)",
			newReturnType:  "TABLE(total_books_read bigint, total_pages_read bigint)",
			expectedChange: true,
		},
		{
			name:           "RETURNS TABLE column type changed",
			oldReturnType:  "TABLE(id integer, name text)",
			newReturnType:  "TABLE(id bigint, name text)",
			expectedChange: true,
		},
		{
			name:           "No change",
			oldReturnType:  "integer",
			newReturnType:  "integer",
			expectedChange: false,
		},
		{
			name:           "RETURNS SETOF type changed",
			oldReturnType:  "SETOF record",
			newReturnType:  "SETOF text",
			expectedChange: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			oldF := schema.Function{ReturnType: tt.oldReturnType}
			newF := schema.Function{ReturnType: tt.newReturnType}
			got := hasReturnTypeChanged(oldF, newF)
			if got != tt.expectedChange {
				t.Errorf("hasReturnTypeChanged() = %v, want %v\nOld: %q\nNew: %q",
					got, tt.expectedChange, tt.oldReturnType, tt.newReturnType)
			}
		})
	}
}

func TestAlterWithReturnTypeChange(t *testing.T) {
	gen := &functionSQLVertexGenerator{}

	oldFunc := schema.Function{
		SchemaQualifiedName: schema.SchemaQualifiedName{
			SchemaName:  "public",
			EscapedName: "\"get_value\"()",
		},
		FunctionDef: "CREATE FUNCTION get_value() RETURNS integer LANGUAGE SQL IMMUTABLE RETURN 42;",
		ReturnType:  "integer",
	}

	newFunc := schema.Function{
		SchemaQualifiedName: schema.SchemaQualifiedName{
			SchemaName:  "public",
			EscapedName: "\"get_value\"()",
		},
		FunctionDef: "CREATE FUNCTION get_value() RETURNS text LANGUAGE SQL IMMUTABLE RETURN '42';",
		ReturnType:  "text",
	}

	diff := oldAndNew[schema.Function]{old: oldFunc, new: newFunc}
	functionDiff := struct{ oldAndNew[schema.Function] }{diff}

	stmts, err := gen.Alter(functionDiff)
	if err != nil {
		t.Fatalf("Alter() error = %v", err)
	}

	if len(stmts) != 2 {
		t.Fatalf("Expected 2 statements (DROP + CREATE), got %d", len(stmts))
	}

	// First statement should be DROP
	if !strings.Contains(stmts[0].DDL, "DROP FUNCTION") {
		t.Errorf("First statement should be DROP, got: %s", stmts[0].DDL)
	}

	// Second statement should be CREATE
	if !strings.Contains(stmts[1].DDL, "CREATE") {
		t.Errorf("Second statement should be CREATE, got: %s", stmts[1].DDL)
	}
}

func TestAlterWithReturnTableChange(t *testing.T) {
	gen := &functionSQLVertexGenerator{}

	// Test the original user's scenario: RETURNS TABLE with columns changed
	oldFunc := schema.Function{
		SchemaQualifiedName: schema.SchemaQualifiedName{
			SchemaName:  "public",
			EscapedName: "\"get_reading_stats\"(uuid, date, date)",
		},
		FunctionDef: `CREATE FUNCTION public.get_reading_stats(_user_id uuid, _start_date date, _end_date date)
 RETURNS TABLE(total_books_read bigint, total_pages_read bigint)
 LANGUAGE sql AS $function$ SELECT 1,2 $function$`,
		ReturnType: "TABLE(total_books_read bigint, total_pages_read bigint)",
	}

	newFunc := schema.Function{
		SchemaQualifiedName: schema.SchemaQualifiedName{
			SchemaName:  "public",
			EscapedName: "\"get_reading_stats\"(uuid, date, date)",
		},
		FunctionDef: `CREATE FUNCTION public.get_reading_stats(_user_id uuid, _start_date date, _end_date date)
 RETURNS TABLE(total_books_read bigint, total_pages_read bigint, total_minutes_read bigint)
 LANGUAGE sql AS $function$ SELECT 1,2,3 $function$`,
		ReturnType: "TABLE(total_books_read bigint, total_pages_read bigint, total_minutes_read bigint)",
	}

	diff := oldAndNew[schema.Function]{old: oldFunc, new: newFunc}
	functionDiff := struct{ oldAndNew[schema.Function] }{diff}

	stmts, err := gen.Alter(functionDiff)
	if err != nil {
		t.Fatalf("Alter() error = %v", err)
	}

	if len(stmts) != 2 {
		t.Fatalf("Expected 2 statements (DROP + CREATE), got %d", len(stmts))
	}

	// First statement should be DROP
	if !strings.Contains(stmts[0].DDL, "DROP FUNCTION") {
		t.Errorf("First statement should be DROP, got: %s", stmts[0].DDL)
	}

	// Second statement should be CREATE
	if !strings.Contains(stmts[1].DDL, "CREATE") {
		t.Errorf("Second statement should be CREATE, got: %s", stmts[1].DDL)
	}
}
