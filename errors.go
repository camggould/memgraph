package memgraph

import "errors"

var (
	ErrNotFound       = errors.New("memgraph: not found")
	ErrConflict       = errors.New("memgraph: lineage has unresolved conflicts")
	ErrKindNotAllowed = errors.New("memgraph: kind not in graph whitelist")
	ErrInvalidInput   = errors.New("memgraph: invalid input")
	ErrNotImplemented = errors.New("memgraph: not implemented")
)
