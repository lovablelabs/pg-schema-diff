package diff

import (
	"fmt"

	"github.com/google/go-cmp/cmp"
	"github.com/stripe/pg-schema-diff/internal/schema"
)


type functionSQLVertexGenerator struct {
	// functionsInNewSchemaByName is a map of function name to functions in the new schema.
	// These functions are not necessarily new
	functionsInNewSchemaByName map[string]schema.Function
}

func newFunctionSqlVertexGenerator(functionsInNewSchemaByName map[string]schema.Function) sqlVertexGenerator[schema.Function, functionDiff] {
	return legacyToNewSqlVertexGenerator[schema.Function, functionDiff](&functionSQLVertexGenerator{
		functionsInNewSchemaByName: functionsInNewSchemaByName,
	})
}

func (f *functionSQLVertexGenerator) Add(function schema.Function) ([]Statement, error) {
	var hazards []MigrationHazard
	if !canFunctionDependenciesBeTracked(function) {
		hazards = append(hazards, MigrationHazard{
			Type: MigrationHazardTypeHasUntrackableDependencies,
			Message: "Dependencies, i.e. other functions used in the function body, of non-sql functions cannot be tracked. " +
				"As a result, we cannot guarantee that function dependencies are ordered properly relative to this " +
				"statement. For adds, this means you need to ensure that all functions this function depends on are " +
				"created/altered before this statement.",
		})
	}
	return []Statement{{
		DDL:         function.FunctionDef,
		Timeout:     statementTimeoutDefault,
		LockTimeout: lockTimeoutDefault,
		Hazards:     hazards,
	}}, nil
}

func (f *functionSQLVertexGenerator) Delete(function schema.Function) ([]Statement, error) {
	var hazards []MigrationHazard
	if !canFunctionDependenciesBeTracked(function) {
		hazards = append(hazards, MigrationHazard{
			Type: MigrationHazardTypeHasUntrackableDependencies,
			Message: "Dependencies, i.e. other functions used in the function body, of non-sql functions cannot be " +
				"tracked. As a result, we cannot guarantee that function dependencies are ordered properly relative to " +
				"this statement. For drops, this means you need to ensure that all functions this function depends on " +
				"are dropped after this statement.",
		})
	}
	return []Statement{{
		DDL:         fmt.Sprintf("DROP FUNCTION %s", function.GetFQEscapedName()),
		Timeout:     statementTimeoutDefault,
		LockTimeout: lockTimeoutDefault,
		Hazards:     hazards,
	}}, nil
}

func (f *functionSQLVertexGenerator) Alter(diff functionDiff) ([]Statement, error) {
	// We are assuming the function has been normalized, i.e., we don't have to worry DependsOnFunctions ordering
	// causing a false positive diff detected.
	if cmp.Equal(diff.old, diff.new) {
		return nil, nil
	}

	// Check if the signature has changed (parameters or return type).
	// PostgreSQL does not allow changing a function's signature with CREATE OR REPLACE.
	// We need to DROP and CREATE instead.
	signatureChanged := hasSignatureChanged(diff.old, diff.new)
	returnTypeChanged := hasReturnTypeChanged(diff.old, diff.new)

	if signatureChanged || returnTypeChanged {
		// Build hazards - same for both DROP and CREATE
		var hazards []MigrationHazard
		if !canFunctionDependenciesBeTracked(diff.new) {
			hazards = append(hazards, MigrationHazard{
				Type: MigrationHazardTypeHasUntrackableDependencies,
				Message: "Dependencies, i.e. other functions used in the function body, of non-sql functions cannot be tracked. " +
					"As a result, we cannot guarantee that function dependencies are ordered properly relative to this " +
					"statement.",
			})
		}

		// Return DROP first, then CREATE, within a single ALTER vertex
		return []Statement{
			{
				DDL:         fmt.Sprintf("DROP FUNCTION %s", diff.old.GetFQEscapedName()),
				Timeout:     statementTimeoutDefault,
				LockTimeout: lockTimeoutDefault,
				Hazards:     hazards,
			},
			{
				DDL:         diff.new.FunctionDef,
				Timeout:     statementTimeoutDefault,
				LockTimeout: lockTimeoutDefault,
				Hazards:     hazards,
			},
		}, nil
	}

	return f.Add(diff.new)
}

// hasSignatureChanged checks if the function signature has changed between old and new versions.
// This includes changes to:
// - Parameters (types, names, IN/OUT/INOUT modifiers)
// - Return type (RETURNS clause or OUT/INOUT parameters)
//
// When the signature changes, PostgreSQL doesn't allow CREATE OR REPLACE and requires DROP + CREATE.
func hasSignatureChanged(oldFunc, newFunc schema.Function) bool {
	// The EscapedName includes the full function signature with parameters
	// If the signatures differ, we need DROP + CREATE
	return oldFunc.EscapedName != newFunc.EscapedName
}

// hasReturnTypeChanged checks if the return type of a function has changed between old and new versions.
// It compares the canonical return type signature from pg_get_function_result, which includes:
// - RETURNS TABLE(...) columns
// - OUT/INOUT parameters
// - Simple return types (RETURNS type, RETURNS SETOF type)
func hasReturnTypeChanged(oldFunc, newFunc schema.Function) bool {
	return oldFunc.ReturnType != newFunc.ReturnType
}

func canFunctionDependenciesBeTracked(function schema.Function) bool {
	return function.Language == "sql"
}

func (f *functionSQLVertexGenerator) GetSQLVertexId(function schema.Function, diffType diffType) sqlVertexId {
	return buildFunctionVertexId(function.SchemaQualifiedName, diffType)
}

func buildFunctionVertexId(name schema.SchemaQualifiedName, diffType diffType) sqlVertexId {
	return buildSchemaObjVertexId("function", name.GetFQEscapedName(), diffType)
}

func (f *functionSQLVertexGenerator) GetAddAlterDependencies(newFunction, oldFunction schema.Function) ([]dependency, error) {
	var deps []dependency
	for _, depFunction := range newFunction.DependsOnFunctions {
		deps = append(deps, mustRun(f.GetSQLVertexId(newFunction, diffTypeAddAlter)).after(buildFunctionVertexId(depFunction, diffTypeAddAlter)))
	}

	if !cmp.Equal(oldFunction, schema.Function{}) {
		// If the function is being altered:
		// If the old version of the function calls other functions that are being deleted, those deletions
		// must come after the function is altered, so it is no longer dependent on those dropped functions
		for _, depFunction := range oldFunction.DependsOnFunctions {
			deps = append(deps, mustRun(f.GetSQLVertexId(newFunction, diffTypeAddAlter)).before(buildFunctionVertexId(depFunction, diffTypeDelete)))
		}
	}

	return deps, nil
}

func (f *functionSQLVertexGenerator) GetDeleteDependencies(function schema.Function) ([]dependency, error) {
	var deps []dependency
	for _, depFunction := range function.DependsOnFunctions {
		deps = append(deps, mustRun(f.GetSQLVertexId(function, diffTypeDelete)).before(buildFunctionVertexId(depFunction, diffTypeDelete)))
	}
	return deps, nil
}
