package gyro

// TopologyDiff is a pure comparison of two complete normalized snapshots.
// Added, Removed, and Updated are sorted by stable Member.ID and detached from
// both input snapshots.
type TopologyDiff struct {
	From    Revision
	To      Revision
	Added   []Member
	Removed []Member
	Updated []Member
}

// DiffTopologySnapshots computes a resource-neutral change plan. A source
// reset treats every old member as removed and every new member as added,
// because identities from different source epochs are not comparable.
func DiffTopologySnapshots(previous, current TopologySnapshot) TopologyDiff {
	diff := TopologyDiff{From: previous.Revision, To: current.Revision}
	if previous.Revision.Source != current.Revision.Source {
		diff.Removed = cloneMembers(previous.Members)
		diff.Added = cloneMembers(current.Members)
		return diff
	}

	previousByID := make(map[string]Member, len(previous.Members))
	for _, member := range previous.Members {
		previousByID[member.ID] = member
	}
	currentByID := make(map[string]Member, len(current.Members))
	for _, member := range current.Members {
		currentByID[member.ID] = member
		old, exists := previousByID[member.ID]
		if !exists {
			diff.Added = append(diff.Added, cloneMember(member))
			continue
		}
		if !membersEqual(old, member) {
			diff.Updated = append(diff.Updated, cloneMember(member))
		}
	}
	for _, member := range previous.Members {
		if _, exists := currentByID[member.ID]; !exists {
			diff.Removed = append(diff.Removed, cloneMember(member))
		}
	}
	return diff
}

func membersEqual(left, right Member) bool {
	if left.ID != right.ID || !equalStringMaps(left.Attributes, right.Attributes) || len(left.Endpoints) != len(right.Endpoints) {
		return false
	}
	for i, endpoint := range left.Endpoints {
		other := right.Endpoints[i]
		if endpoint.Address != other.Address || !equalStringMaps(endpoint.Attributes, other.Attributes) {
			return false
		}
	}
	return true
}

func cloneMembers(members []Member) []Member {
	result := make([]Member, len(members))
	for i, member := range members {
		result[i] = cloneMember(member)
	}
	return result
}

func cloneMember(member Member) Member {
	result := Member{
		ID:         member.ID,
		Endpoints:  make([]Endpoint, len(member.Endpoints)),
		Attributes: cloneStringMap(member.Attributes),
	}
	for i, endpoint := range member.Endpoints {
		result.Endpoints[i] = Endpoint{
			Address:    endpoint.Address,
			Attributes: cloneStringMap(endpoint.Attributes),
		}
	}
	return result
}
