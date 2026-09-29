package topology

import "github.com/focusandinsist/gyro/gyro"

// Diff computes a resource-neutral change plan between complete snapshots.
func Diff(previous, current gyro.TopologySnapshot) gyro.TopologyDiff {
	diff := gyro.TopologyDiff{From: previous.Revision, To: current.Revision}
	if previous.Revision.Source != current.Revision.Source {
		diff.Removed = cloneMembers(previous.Members)
		diff.Added = cloneMembers(current.Members)
		return diff
	}
	previousByID := make(map[string]gyro.Member, len(previous.Members))
	for _, member := range previous.Members {
		previousByID[member.ID] = member
	}
	currentByID := make(map[string]gyro.Member, len(current.Members))
	for _, member := range current.Members {
		currentByID[member.ID] = member
		old, exists := previousByID[member.ID]
		if !exists {
			diff.Added = append(diff.Added, cloneMember(member))
		} else if !equalMember(old, member) {
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

func equalMember(left, right gyro.Member) bool {
	if left.ID != right.ID || len(left.Endpoints) != len(right.Endpoints) || len(left.Attributes) != len(right.Attributes) {
		return false
	}
	for key, value := range left.Attributes {
		if right.Attributes[key] != value {
			return false
		}
	}
	for i, endpoint := range left.Endpoints {
		other := right.Endpoints[i]
		if endpoint.Address != other.Address || len(endpoint.Attributes) != len(other.Attributes) {
			return false
		}
		for key, value := range endpoint.Attributes {
			if other.Attributes[key] != value {
				return false
			}
		}
	}
	return true
}

func cloneMembers(members []gyro.Member) []gyro.Member {
	result := make([]gyro.Member, len(members))
	for i, member := range members {
		result[i] = cloneMember(member)
	}
	return result
}

func cloneMember(member gyro.Member) gyro.Member {
	result := member
	result.Endpoints = make([]gyro.Endpoint, len(member.Endpoints))
	for i, endpoint := range member.Endpoints {
		result.Endpoints[i] = gyro.Endpoint{Address: endpoint.Address, Attributes: cloneMap(endpoint.Attributes)}
	}
	result.Attributes = cloneMap(member.Attributes)
	return result
}
