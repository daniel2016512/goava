package ops

import ()


// DatabaseError represents error.
type DatabaseError struct {
	ErrorString string
}

func (pe *DatabaseError) Error() string { return pe.ErrorString }

var (
	ErrDBUnsupported = &DatabaseError{"cannot find implementation of the database type"}
	ErrTableExist    = &DatabaseError{"table exists"}
	ErrInvalidKeyspace = &DatabaseError{"keyspace is nil"}
	ErrTableNA = &DatabaseError{"table not available"}
)
