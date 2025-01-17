package query

import (
	"fmt"
	"simple-db-go/record"
	"simple-db-go/types"
)

type TermCannnotEquatesWithConstantError struct {
	lhs Expression
	rhs Expression
}

func (e *TermCannnotEquatesWithConstantError) Error() string {
	return fmt.Sprintf("Termの EquatesWithConstant に失敗しました。lhs=%+v, rhs=%+v", e.lhs, e.rhs)
}

type TermCannnotEquatesWithFieldNameError struct {
	lhs Expression
	rhs Expression
}

func (e *TermCannnotEquatesWithFieldNameError) Error() string {
	return fmt.Sprintf("Termの EquatesWithFieldName に失敗しました。lhs=%+v, rhs=%+v", e.lhs, e.rhs)
}

type NotFoundSubPredicateError struct {
	predicate *Predicate
	schema    *record.Schema
}

func (e *NotFoundSubPredicateError) Error() string {
	return fmt.Sprintf("Predicateの SelectSubPred に失敗しました。predicate=%+v, schema=%+v", e.predicate, e.schema)
}

type CannotJoinSubPredicateError struct {
	predicate *Predicate
	schema1   *record.Schema
	schema2   *record.Schema
}

func (e *CannotJoinSubPredicateError) Error() string {
	return fmt.Sprintf("Predicateの JoinSubPred に失敗しました。predicate=%+v, schema1=%+v, schema2=%+v", e.predicate, e.schema1, e.schema2)
}

type PredicateEquatesWithConstantError struct {
	predicate *Predicate
	fieldName types.FieldName
}

func (e *PredicateEquatesWithConstantError) Error() string {
	return fmt.Sprintf("Predicateの EquatesWithConstant に失敗しました。predicate=%+v, fieldName=%s", e.predicate, e.fieldName)
}

type PredicateEquatesWithFieldNameError struct {
	predicate *Predicate
	fieldName types.FieldName
}

func (e *PredicateEquatesWithFieldNameError) Error() string {
	return fmt.Sprintf("Predicateの EquatesWithFieldName に失敗しました。predicate=%+v, fieldName=%s", e.predicate, e.fieldName)
}

type UnknownFieldInProjectScanError struct {
	fieldName   types.FieldName
	projectScan *ProjectScan
}

func (e *UnknownFieldInProjectScanError) Error() string {
	return fmt.Sprintf("ProjectScan に不明なフィールドが指定されました。field_name=%s, project_scan=%+v", e.fieldName, e.projectScan)
}
