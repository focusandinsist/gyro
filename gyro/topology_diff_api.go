package gyro

// TopologyDiff describes member changes between two complete snapshots.
// The comparison implementation is owned by internal/topology.
type TopologyDiff struct {
	From    Revision
	To      Revision
	Added   []Member
	Removed []Member
	Updated []Member
}
