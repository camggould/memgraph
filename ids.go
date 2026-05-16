package memgraph

import (
	"crypto/rand"
	"time"

	"github.com/oklog/ulid/v2"
)

type (
	GraphID   string
	NodeID    string
	EdgeID    string
	LineageID string
)

func NewGraphID() GraphID     { return GraphID(newULID()) }
func NewNodeID() NodeID       { return NodeID(newULID()) }
func NewEdgeID() EdgeID       { return EdgeID(newULID()) }
func NewLineageID() LineageID { return LineageID(newULID()) }

func newULID() string {
	return ulid.MustNew(ulid.Timestamp(time.Now()), rand.Reader).String()
}
