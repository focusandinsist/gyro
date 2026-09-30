package client

import (
	"sort"

	"gyro/gyro"
)

func cloneNodeInfo(node gyro.NodeInfo) gyro.NodeInfo {
	result := node
	result.Metadata = cloneStringMap(node.Metadata)
	return result
}

func cloneStringMap(values map[string]string) map[string]string {
	if values == nil {
		return nil
	}
	result := make(map[string]string, len(values))
	for key, value := range values {
		result[key] = value
	}
	return result
}

func sortNodeInfosByID(nodes []gyro.NodeInfo) {
	sort.Slice(nodes, func(left, right int) bool { return nodes[left].ID < nodes[right].ID })
}
