package whc

import (
        "errors"
)

var (
        WhcDup  = errors.New("Duplicate where clause")
        WhcInvalid  = errors.New("Invalid operation type")
)

type WhereClauseType struct {
        ColumnName   string
        RelationType string
        ColumnValue  interface{}
}

type UpdateClauseType struct {
	ColumnName string
	UpdateType string
	ColumnValue interface{}
}

func NewUpdate() UpdateClauseType {
	return UpdateClauseType{}
}

func (u *UpdateClauseType) AddUpdate(fieldName string, val interface{}, updateType string)  {
	u.ColumnName = fieldName
	u.UpdateType = updateType
	u.ColumnValue = val
}

func (u *UpdateClauseType) GetUpdateColumnName() string {
	return u.ColumnName
}

func (u *UpdateClauseType) GetUpdateColumnVal() string {
	return u.ColumnName
}

func (u *UpdateClauseType) GetUpdateType() string {
	return u.UpdateType
}
