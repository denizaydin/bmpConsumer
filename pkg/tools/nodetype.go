package tools

// Map for AS node types
var nodeTypeMap = map[uint16]string{
	0:   "LEAF",
	200: "SPINE",
	300: "SUPERSPINE",
	301: "SUPERSPINE",
	302: "SUPERSPINE",
	777: "LB",
}

// Function to extract the node type from AS number
func GetNodeType(asNumber uint32) string {
	// Masking to get the last 3 digits
	nodeType := asNumber % 1000
	if nodeType <= 99 {
		nodeType = 0 // Leaf type
	}

	// Return the node type using the map
	if deviceType, ok := nodeTypeMap[uint16(nodeType)]; ok {
		return deviceType
	}
	return "UNDEFINED"
}
