package message

type CapabilityData struct {
	Value       []byte `json:"capability_value,omitempty"`
	Description string `json:"capability_descr,omitempty"`
}

// Capability Defines a structure for BGP Capability TLV which is sent as a part
// Informational TLVs in Open Message
// Known capability codes: https://www.iana.org/assignments/capability-codes/capability-codes.xhtml
// Capability structure: https://tools.ietf.org/html/rfc5492#section-4
type Capability map[uint8][]*CapabilityData

type Host struct {
	Mac         string `json:"mac,omitempty"`
	IPv4Address string `json:"ipv4,omitempty"`
	IPv6Address string `json:"ipv6,omitempty"`
	Connected   uint32 `json:"connected,omitempty"`
}
type Connection struct {
	Source      uint32 `json:"source,omitempty"`
	Destination uint32 `json:"destination,omitempty"`
}
type Connections []Connection

type BaseAttributes struct {
	BaseAttrHash     string   `json:"base_attr_hash,omitempty"`
	Origin           string   `json:"origin,omitempty"`
	ASPath           []uint32 `json:"as_path,omitempty"`
	ASPathCount      int32    `json:"as_path_count,omitempty"`
	Nexthop          string   `json:"nexthop,omitempty"`
	MED              uint32   `json:"med,omitempty"`
	LocalPref        uint32   `json:"local_pref,omitempty"`
	IsAtomicAgg      bool     `json:"is_atomic_agg"`
	Aggregator       []byte   `json:"aggregator,omitempty"`
	CommunityList    []string `json:"community_list,omitempty"`
	OriginatorID     string   `json:"originator_id,omitempty"`
	ClusterList      string   `json:"cluster_list,omitempty"`
	ExtCommunityList []string `json:"ext_community_list,omitempty"`
	RTList           []string `json:"route_targets,omitempty"`
	AS4Path          []uint32 `json:"as4_path,omitempty"`
	AS4PathCount     int32    `json:"as4_path_count,omitempty"`
	AS4Aggregator    []byte   `json:"as4_aggregator,omitempty"`
	// PMSITunnel
	TunnelEncapAttr []byte `json:"-"`
	// TraficEng
	// IPv6SpecExtCommunity
	// AIGP
	// PEDistinguisherLable
	LgCommunityList []string `json:"large_community_list,omitempty"`
	// SecPath
	// AttrSet
}

// PeerStateChange defines a message format sent to as a result of BMP Peer Up or Peer Down message
type PeerStateChange struct {
	Key             string     `json:"_key,omitempty"`
	ID              string     `json:"_id,omitempty"`
	Rev             string     `json:"_rev,omitempty"`
	Action          string     `json:"action,omitempty"` // Action can be "add" for peer up and "del" for peer down message
	Sequence        int        `json:"sequence,omitempty"`
	Hash            string     `json:"hash,omitempty"`
	RouterHash      string     `json:"router_hash,omitempty"`
	Name            string     `json:"name,omitempty"`
	RemoteBGPID     string     `json:"remote_bgp_id,omitempty"`
	RouterIP        string     `json:"router_ip,omitempty"`
	Timestamp       string     `json:"timestamp,omitempty"`
	RemoteASN       uint32     `json:"remote_asn,omitempty"`
	RemoteIP        string     `json:"remote_ip,omitempty"`
	PeerType        uint8      `json:"peer_type"`
	PeerRD          string     `json:"peer_rd,omitempty"`
	RemotePort      int        `json:"remote_port,omitempty"`
	LocalASN        uint32     `json:"local_asn,omitempty"`
	LocalIP         string     `json:"local_ip,omitempty"`
	LocalPort       int        `json:"local_port,omitempty"`
	LocalBGPID      string     `json:"local_bgp_id,omitempty"`
	InfoData        []byte     `json:"info_data,omitempty"`
	AdvCapabilities Capability `json:"adv_cap,omitempty"`
	RcvCapabilities Capability `json:"recv_cap,omitempty"`
	RemoteHolddown  int        `json:"remote_holddown,omitempty"`
	AdvHolddown     int        `json:"adv_holddown,omitempty"`
	BMPReason       int        `json:"bmp_reason,omitempty"`
	BMPErrorCode    int        `json:"bmp_error_code,omitempty"`
	BMPErrorSubCode int        `json:"bmp_error_sub_code,omitempty"`
	ErrorText       string     `json:"error_text,omitempty"`
	IsL3VPN         bool       `json:"is_l"`
	IsPrepolicy     bool       `json:"is_prepolicy"`
	IsIPv4          bool       `json:"is_ipv4"`
	TableName       string     `json:"table_name,omitempty"`
	// Values are assigned based on PerPeerHeader flas
	IsAdjRIBInPost   bool `json:"is_adj_rib_in_post_policy"`
	IsAdjRIBOutPost  bool `json:"is_adj_rib_out_post_policy"`
	IsLocRIBFiltered bool `json:"is_loc_rib_filtered"`
}

// EVPNPrefix defines the structure of EVPN message
type EVPNPrefix struct {
	Key            string         `json:"_key,omitempty"`
	ID             string         `json:"_id,omitempty"`
	Rev            string         `json:"_rev,omitempty"`
	Action         string         `json:"action,omitempty"` // Action can be "add" or "del"
	Sequence       int            `json:"sequence,omitempty"`
	Hash           string         `json:"hash,omitempty"`
	RouterHash     string         `json:"router_hash,omitempty"`
	RouterIP       string         `json:"router_ip,omitempty"`
	BaseAttributes BaseAttributes `json:"base_attrs,omitempty"`
	PeerHash       string         `json:"peer_hash,omitempty"`
	RemoteBGPID    string         `json:"remote_bgp_id,omitempty"`
	PeerIP         string         `json:"peer_ip,omitempty"`
	PeerType       uint8          `json:"peer_type"`
	PeerASN        uint32         `json:"peer_asn,omitempty"`
	Timestamp      string         `json:"timestamp,omitempty"`
	IsIPv4         bool           `json:"is_ipv4"`
	OriginAS       int32          `json:"origin_as,omitempty"`
	Nexthop        string         `json:"nexthop,omitempty"`
	ClusterList    string         `json:"cluster_list,omitempty"`
	IsNexthopIPv4  bool           `json:"is_nexthop_ipv4"`
	PathID         int32          `json:"path_id,omitempty"`
	Labels         []uint32       `json:"labels,omitempty"`
	RawLabels      []uint32       `json:"rawlabels,omitempty"`
	VPNRD          string         `json:"vpn_rd,omitempty"`
	VPNRDType      uint16         `json:"vpn_rd_type"`
	ESI            string         `json:"eth_segment_id,omitempty"`
	EthTag         []byte         `json:"eth_tag,omitempty"`
	IPAddress      string         `json:"ip_address,omitempty"`
	IPLength       uint8          `json:"ip_len,omitempty"`
	GWAddress      string         `json:"gw_address,omitempty"`
	MAC            string         `json:"mac,omitempty"`
	MACLength      uint8          `json:"mac_len,omitempty"`
	RouteType      uint8          `json:"route_type,omitempty"`
	RTList         []string       `json:"route_targets,omitempty"`
	Host           Host           `json:"host,omitempty"`
	Connections    Connections    `json:"connections,omitempty"`
	// TODO Type 3 carries nlri 22
	// https://tools.ietf.org/html/rfc6514
	// Add to the message
	// Values are assigned based on PerPeerHeader flas
	IsAdjRIBInPost   bool `json:"is_adj_rib_in_post_policy"`
	IsAdjRIBOutPost  bool `json:"is_adj_rib_out_post_policy"`
	IsLocRIBFiltered bool `json:"is_loc_rib_filtered"`
}
