package memgraph

import "errors"

var (
	ErrNotFound       = errors.New("memgraph: not found")
	ErrConflict       = errors.New("memgraph: lineage has unresolved conflicts")
	// ErrConflictManual is returned by PutNode when a concurrent write is
	// detected under ConflictPolicyManual. The node was written (and is
	// returned alongside the error) as a sibling head; resolution requires
	// a follow-up PutNode that explicitly supersedes both siblings.
	ErrConflictManual = errors.New("memgraph: concurrent write under manual conflict policy; conflict recorded")
	ErrKindNotAllowed = errors.New("memgraph: kind not in graph whitelist")
	ErrInvalidInput   = errors.New("memgraph: invalid input")
	ErrNotImplemented = errors.New("memgraph: not implemented")
)
