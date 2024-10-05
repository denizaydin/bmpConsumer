package consumer

import (
	cfg "bmpConsumer/config"
	"bmpConsumer/internal/redis"      // Your Redis package
	message "bmpConsumer/pkg/message" // Import your protobuf package here
	"bmpConsumer/pkg/tools"
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/golang/glog"
	"github.com/neo4j/neo4j-go-driver/v5/neo4j"
)

// Node is a graph node
type Node struct {
	Type       string
	Name       string
	ID         string
	Status     string
	Properties []Attribute
}

// Edge is a graph edge
type Edge struct {
	Type        string
	Source      *Node
	Destination *Node
	Status      string
}

// Attribute define the tags/attributes of the node
type Attribute struct {
	Name  string
	Value string
}

type Consumer struct {
	kafkaConsumer *kafka.Consumer
}
type ProcessEVPNPrefixFunc func(ctx context.Context, redisClient *redis.RedisClient, evpnPrefix *message.EVPNPrefix, neo4j *neo4j.DriverWithContext, nmap *cfg.NetworkMap) error

var processFuncMap = map[uint8]ProcessEVPNPrefixFunc{
	2: processEVPNType2Prefix,
	3: processEVPNType3Prefix,
	5: processEVPNType5Prefix,
}

// NewConsumer initializes a new Kafka consumer
func NewConsumer(config *cfg.Config, ctx context.Context) (*Consumer, error) {
	c, err := kafka.NewConsumer(config.Kafka.ConfigMap)
	if err != nil {
		return nil, err
	}
	return &Consumer{
		kafkaConsumer: c,
	}, nil
}

// Close gracefully shuts down the Kafka consumer and Redis client
func (c *Consumer) Close() {
	glog.Info("closing Kafka consumer")
	// Close Kafka consumer
	if err := c.kafkaConsumer.Close(); err != nil {
		glog.Errorf("failed to close Kafka consumer: %v", err)
	} else {
		glog.Info("kafka consumer closed successfully.")
	}
}

// ProcessMessages consumes messages from Kafka and processes them
func (c *Consumer) ProcessMessages(ctx context.Context, topics []string, r *redis.RedisClient, neo4j *neo4j.DriverWithContext, nmap *cfg.NetworkMap) {
	err := c.kafkaConsumer.SubscribeTopics(topics, nil)
	if err != nil {
		glog.Fatalf("failed to subscribe to topics: %v", err)
	}

	for {
		select {
		case <-ctx.Done():
			glog.Info("received shutdown signal, stopping message processing...")
			return
		default:
			msg, err := c.kafkaConsumer.ReadMessage(100 * time.Millisecond) // Use a short timeout for graceful shutdown
			if err != nil {
				if kafkaError, ok := err.(kafka.Error); ok && kafkaError.Code() == kafka.ErrTimedOut {
					continue
				}
				glog.Errorf("error reading message: %v", err)
				continue
			}

			// Process based on topic
			switch *msg.TopicPartition.Topic {
			case "gobmp.parsed.peer":
				glog.Infof("handling msg:%v from topic:%v", msg.Value, *msg.TopicPartition.Topic)
				processPeerMessage(ctx, msg.Value, r, neo4j, nmap)
			case "gobmp.parsed.evpn":
				glog.Infof("handling msg:%v from topic:%v", msg.Value, *msg.TopicPartition.Topic)
				processEVPNPrefix(ctx, msg.Value, r, neo4j, nmap)
			default:
				glog.Errorf("unhandled topic:%v", *msg.TopicPartition.Topic)
			}
		}
	}
}

// processPeerMessage Deserialize and process peer message (PeerStateChange)
func processPeerMessage(ctx context.Context, data []byte, redisClient *redis.RedisClient, neo4j *neo4j.DriverWithContext, nmap *cfg.NetworkMap) {
	var peerMessage *message.PeerStateChange
	err := json.Unmarshal(data, &peerMessage)
	if err != nil {
		glog.Errorf("failed to deserialize PeerStateChange: %v", err)
		return
	}
	// Only interested in "up" messages
	if peerMessage.Action == "down" {
		glog.Infof("unhandled peer down message")
		return
	}

	// Create and cache remote node
	remoteNode := Node{
		Type:       tools.GetNodeType(peerMessage.RemoteASN),
		Name:       strconv.FormatUint(uint64(peerMessage.RemoteASN), 10),
		ID:         peerMessage.RemoteBGPID,
		Status:     "up",
		Properties: []Attribute{{Name: "MGMT_IP", Value: tools.MgmtIPfromBGPID(peerMessage.RemoteASN, peerMessage.RemoteBGPID, nmap)}},
	}
	processNode(ctx, redisClient, neo4j, &remoteNode)

	// Create and cache local node
	localNode := Node{
		Type:       tools.GetNodeType(peerMessage.LocalASN),
		Name:       strconv.FormatUint(uint64(peerMessage.LocalASN), 10),
		ID:         peerMessage.LocalBGPID,
		Status:     "up",
		Properties: []Attribute{{Name: "MGMT_IP", Value: tools.MgmtIPfromBGPID(peerMessage.LocalASN, peerMessage.LocalBGPID, nmap)}},
	}
	processNode(ctx, redisClient, neo4j, &localNode)

	// Cache Edge between local and remote ASN
	if peerMessage.LocalASN > peerMessage.RemoteASN {
		processEdge(ctx, redisClient, &remoteNode, &localNode, neo4j, "up")
		return
	}
	processEdge(ctx, redisClient, &localNode, &remoteNode, neo4j, "up")

}

// Deserialize and process EVPN message (EVPNPrefix)
func processEVPNPrefix(ctx context.Context, data []byte, redisClient *redis.RedisClient, neo4j *neo4j.DriverWithContext, nmap *cfg.NetworkMap) {
	var evpnPrefix *message.EVPNPrefix
	err := json.Unmarshal(data, &evpnPrefix)
	if err != nil {
		glog.Errorf("failed to deserialize evpn prefix: %v", err)
		return
	}
	// Continue processing the message...
	// currently only evpn route-type 2 is processed
	if processFunc, ok := processFuncMap[evpnPrefix.RouteType]; ok {
		glog.Infof("processing message : %+v", evpnPrefix)
		err = processFunc(ctx, redisClient, evpnPrefix, neo4j, nmap)
		if err != nil {
			glog.Errorf("error processing evpn route-type %d message: %v", evpnPrefix.RouteType, err)
		}
		return
	} else {
		glog.Infof("unsupported route-type: %+v", evpnPrefix.RouteType)
	}
}
func processEVPNType2Prefix(ctx context.Context, redisClient *redis.RedisClient, message *message.EVPNPrefix, neo4j *neo4j.DriverWithContext, nmap *cfg.NetworkMap) error {
	glog.Infof("processing message: %v", message)
	// EVPN Route-Type 2 contains information about the attached hosts.
	if message.MAC == "" {
		return fmt.Errorf("empty mac %v for route-type 2", message.MAC)
	}

	status := "down"
	if message.Action == "add" {
		status = "up"
	}
	host := Node{
		Type:   "HOST",
		Name:   message.MAC,
		ID:     message.MAC,
		Status: status,
	}

	processNode(ctx, redisClient, neo4j, &host)

	//some values are only included in prefix add/up bgp updates like BaseAttributes.ASPath[1], remoteASN, as-path e.t.c
	if status == "up" {
		// we can say that out peer is up and set our hostleaf to remote peer
		remotePeer := Node{
			Type:       tools.GetNodeType(message.PeerASN),
			Name:       strconv.FormatUint(uint64(message.PeerASN), 10),
			ID:         message.RemoteBGPID,
			Status:     status,
			Properties: []Attribute{{Name: "MGMT_IP", Value: tools.MgmtIPfromBGPID(message.PeerASN, message.RemoteBGPID, nmap)}},
		}

		processNode(ctx, redisClient, neo4j, &remotePeer)
		hostleaf := remotePeer //assuming that remotePeer is the origin of the update
		// remote peer is not the origion of the update, we should process the origin as as well
		if message.PeerASN != uint32(message.OriginAS) {
			originBGPID, err := tools.GetBGPIDfromAS(uint32(message.OriginAS), nmap)
			if err == nil {
				originAS := Node{
					Type:       tools.GetNodeType(uint32(message.OriginAS)),
					Name:       strconv.FormatUint(uint64(uint32(message.OriginAS)), 10),
					ID:         originBGPID,
					Status:     status,
					Properties: []Attribute{{Name: "MGMT_IP", Value: tools.MgmtIPfromBGPID(uint32(message.OriginAS), originBGPID, nmap)}},
				}
				processNode(ctx, redisClient, neo4j, &originAS)
				hostleaf = originAS
				// Our peer and origin as are directly connected. As we have enough information, bgpid, about origin as we can connect them
				processEdge(ctx, redisClient, &host, &hostleaf, neo4j, "up")
				if message.BaseAttributes.ASPathCount == 2 {
					// Cache Edge between local and remote ASN as they are directly connected and we have enough information about the originAS, bgpid
					if message.PeerASN > uint32(message.OriginAS) {
						processEdge(ctx, redisClient, &originAS, &remotePeer, neo4j, "up")
					} else {
						processEdge(ctx, redisClient, &remotePeer, &originAS, neo4j, "up")
					}
				}
			} else {
				glog.Errorf("failed to find bgpid for the origin as: %v, can not bind host", err)
			}
		}
		processEdge(ctx, redisClient, &host, &hostleaf, neo4j, "up")
		//if peer as == origin as than we can create an edge between them, but as edges should be handled by the peer messages not a bgp updates.
	}

	// evpn route-type 2 with ip address, mac-ip update
	if message.IPAddress != "" {
		ipkey := message.IPAddress + "/" + fmt.Sprintf("%d", message.IPLength)
		//create or update node for prefix
		prefix := Node{
			Type:   "PREFIX",
			Name:   ipkey,
			ID:     ipkey,
			Status: status,
			Properties: []Attribute{
				{Name: "Prefix", Value: message.IPAddress},
				{Name: "Length", Value: strconv.FormatUint(uint64(message.IPLength), 10)},
			},
		}
		processNode(ctx, redisClient, neo4j, &prefix)
		processEdge(ctx, redisClient, &prefix, &host, neo4j, status)
	}
	return nil
}
func processEVPNType3Prefix(ctx context.Context, redisClient *redis.RedisClient, message *message.EVPNPrefix, neo4j *neo4j.DriverWithContext, nmap *cfg.NetworkMap) error {
	glog.Infof("processing message: %v", message)

	status := "down"
	if message.Action == "add" {
		status = "up"
	}
	//some values are only included in prefix add/up bgp updates like originAS, remoteASN, as-path e.t.c
	if status == "up" {
		// we can say that out peer is up and set our hostleaf to remote peer
		remotePeer := Node{
			Type:       tools.GetNodeType(message.PeerASN),
			Name:       strconv.FormatUint(uint64(message.PeerASN), 10),
			ID:         message.RemoteBGPID,
			Status:     status,
			Properties: []Attribute{{Name: "MGMT_IP", Value: tools.MgmtIPfromBGPID(message.PeerASN, message.RemoteBGPID, nmap)}},
		}

		processNode(ctx, redisClient, neo4j, &remotePeer)
		// remote peer is not the origion of the update, we should process the origin as as well
		if message.PeerASN != uint32(message.OriginAS) {
			originBGPID, err := tools.GetBGPIDfromAS(uint32(message.OriginAS), nmap)
			if err == nil {
				originAS := Node{
					Type:       tools.GetNodeType(uint32(message.OriginAS)),
					Name:       strconv.FormatUint(uint64(uint32(message.OriginAS)), 10),
					ID:         originBGPID,
					Status:     status,
					Properties: []Attribute{{Name: "MGMT_IP", Value: tools.MgmtIPfromBGPID(uint32(message.OriginAS), originBGPID, nmap)}},
				}
				processNode(ctx, redisClient, neo4j, &originAS)
				if message.BaseAttributes.ASPathCount == 2 {
					// Cache Edge between local and remote ASN as they are directly connected and we have enough information about the originAS, bgpid
					if message.PeerASN > uint32(message.OriginAS) {
						processEdge(ctx, redisClient, &originAS, &remotePeer, neo4j, "up")
					} else {
						processEdge(ctx, redisClient, &remotePeer, &originAS, neo4j, "up")
					}
				}
			} else {
				glog.Errorf("failed to find bgpid for the origin as: %v, can not bind host", err)
			}
		}
	}
	return nil
}
func processEVPNType5Prefix(ctx context.Context, redisClient *redis.RedisClient, message *message.EVPNPrefix, neo4j *neo4j.DriverWithContext, nmap *cfg.NetworkMap) error {
	glog.Infof("processing message: %v", message)

	status := "down"
	if message.Action == "add" {
		status = "up"
	}
	//some values are only included in prefix add/up bgp updates like originAS, remoteASN, as-path e.t.c
	if status == "up" {
		// we can say that out peer is up and set our hostleaf to remote peer
		remotePeer := Node{
			Type:       tools.GetNodeType(message.PeerASN),
			Name:       strconv.FormatUint(uint64(message.PeerASN), 10),
			ID:         message.RemoteBGPID,
			Status:     status,
			Properties: []Attribute{{Name: "MGMT_IP", Value: tools.MgmtIPfromBGPID(message.PeerASN, message.RemoteBGPID, nmap)}},
		}

		processNode(ctx, redisClient, neo4j, &remotePeer)
		// remote peer is not the origion of the update, we should process the origin as as well
		if message.PeerASN != uint32(message.OriginAS) {
			originBGPID, err := tools.GetBGPIDfromAS(uint32(message.OriginAS), nmap)
			if err == nil {
				originAS := Node{
					Type:       tools.GetNodeType(uint32(message.OriginAS)),
					Name:       strconv.FormatUint(uint64(uint32(message.OriginAS)), 10),
					ID:         originBGPID,
					Status:     status,
					Properties: []Attribute{{Name: "MGMT_IP", Value: tools.MgmtIPfromBGPID(uint32(message.OriginAS), originBGPID, nmap)}},
				}
				processNode(ctx, redisClient, neo4j, &originAS)
				if message.IPAddress != "" {
					if tools.GetNodeType(uint32(message.OriginAS)) == "LEAF" {
						//Create prefix and bind it to origin
						ipkey := message.IPAddress + "/" + fmt.Sprintf("%d", message.IPLength)
						//create or update node for prefix
						prefix := Node{
							Type:   "PREFIX",
							Name:   ipkey,
							ID:     ipkey,
							Status: status,
							Properties: []Attribute{
								{Name: "Prefix", Value: message.IPAddress},
								{Name: "Length", Value: strconv.FormatUint(uint64(message.IPLength), 10)},
							},
						}
						processNode(ctx, redisClient, neo4j, &prefix)
						processEdge(ctx, redisClient, &prefix, &originAS, neo4j, status)
					}
				}
				if message.BaseAttributes.ASPathCount == 2 {
					// Cache Edge between local and remote ASN as they are directly connected and we have enough information about the originAS, bgpid
					if message.PeerASN > uint32(message.OriginAS) {
						processEdge(ctx, redisClient, &originAS, &remotePeer, neo4j, "up")
					} else {
						processEdge(ctx, redisClient, &remotePeer, &originAS, neo4j, "up")
					}
				}
			} else {
				glog.Errorf("failed to find bgpid for the origin as: %v, can not bind host", err)
			}
			// update from other fabrics
			if message.BaseAttributes.ASPathCount > 2 {
				if message.PeerASN == message.BaseAttributes.ASPath[0] {
					if tools.GetNodeType(message.BaseAttributes.ASPath[1]) == "SUPERSPINE" {
						//create super spine and give it a number correponding to current peer/spine
						parsedIPParts := strings.Split(message.RemoteBGPID, ".")
						SuperSpine := Node{
							Type:   tools.GetNodeType(message.BaseAttributes.ASPath[1]),
							Name:   strconv.FormatUint(uint64(message.BaseAttributes.ASPath[1]), 10),
							ID:     parsedIPParts[3],
							Status: status,
						}
						processNode(ctx, redisClient, neo4j, &SuperSpine)
						processEdge(ctx, redisClient, &remotePeer, &SuperSpine, neo4j, "up")
					}
				}
				if message.BaseAttributes.ASPathCount == 3 && tools.GetNodeType(message.BaseAttributes.ASPath[1]) == "LEAF" {
					leafbgpid, err := tools.GetBGPIDfromAS(message.BaseAttributes.ASPath[1], nmap)
					if err == nil {
						leaf := Node{
							Type:   "LEAF",
							Name:   strconv.FormatUint(uint64(message.BaseAttributes.ASPath[1]), 10),
							ID:     leafbgpid,
							Status: status,
						}
						processNode(ctx, redisClient, neo4j, &leaf)
						leafConnectedAS := Node{
							Type:   tools.GetNodeType(message.BaseAttributes.ASPath[2]),
							Name:   strconv.FormatUint(uint64(message.BaseAttributes.ASPath[2]), 10),
							ID:     strconv.FormatUint(uint64(message.BaseAttributes.ASPath[2]), 10),
							Status: status,
						}
						processNode(ctx, redisClient, neo4j, &leafConnectedAS)
						processEdge(ctx, redisClient, &leaf, &leafConnectedAS, neo4j, "up")

						ipkey := message.IPAddress + "/" + fmt.Sprintf("%d", message.IPLength)
						//create or update node for prefix
						prefix := Node{
							Type:   "PREFIX",
							Name:   ipkey,
							ID:     ipkey,
							Status: status,
							Properties: []Attribute{
								{Name: "Prefix", Value: message.IPAddress},
								{Name: "Length", Value: strconv.FormatUint(uint64(message.IPLength), 10)},
							},
						}
						processNode(ctx, redisClient, neo4j, &prefix)
						processEdge(ctx, redisClient, &prefix, &leafConnectedAS, neo4j, status)
					} else {
						glog.Errorf("failed to find bgpid for the leaf as: %v, %v", message.BaseAttributes.ASPath[1], err)
					}
				}
			}
		}

	}
	return nil
}
func processNode(ctx context.Context, redisClient *redis.RedisClient, neo4j *neo4j.DriverWithContext, node *Node) {
	// Convert the struct to a JSON string - we may use hash of value if we consider memory usage.
	valueData, err := json.Marshal(node)
	if err != nil {
		glog.Errorf("error marshalling struct:%v", err)
		return
	}
	value := string(valueData)
	result, err := redisClient.Cache(ctx, node.ID, value)
	if err != nil {
		glog.Errorf("failed to cache node %s: %v", node.Name, err)
		return
	}
	if result {
		glog.Infof("inserting node into db: %s", node.Name)
		mergeNode(ctx, *neo4j, node)
	}
}

func processEdge(ctx context.Context, redisClient *redis.RedisClient, sourceNode, destinationNode *Node, neo4j *neo4j.DriverWithContext, status string) {
	key := sourceNode.ID + "->" + destinationNode.ID
	edge := Edge{
		Type:        "CONNECTION",
		Source:      sourceNode,
		Destination: destinationNode,
		Status:      status,
	}
	// Convert the struct to a JSON string - we may use hash of value if we consider memory usage.
	valueData, err := json.Marshal(edge)
	if err != nil {
		glog.Errorf("error marshalling struct:%v", err)
	}
	value := string(valueData)
	result, err := redisClient.Cache(ctx, key, value)
	if err != nil {
		glog.Errorf("failed to cache edge: %s", key)
		return
	}
	if result {
		glog.Infof("inserting to edge into db: %s", key)
		mergeEdge(ctx, *neo4j, &edge)
	}
}

// MergeNode merges the node into Neo4j (inserts if it doesn't exist or updates if it does).
func mergeNode(ctx context.Context, driver neo4j.DriverWithContext, node *Node) {
	query := fmt.Sprintf(`
		MERGE (n:%s {id: $id})
		ON CREATE SET n.name = $name, n.status = $status, n.type = $type
		ON MATCH SET n.status = $status, n.name = $name, n.type = $type
		RETURN n`, node.Type)

	glog.Infof("node query into db: %s", query)

	params := map[string]any{
		"id":     node.ID,
		"name":   node.Name,
		"status": node.Status,
		"type":   node.Type,
	}

	// Use ExecuteQuery to run the query
	result, err := neo4j.ExecuteQuery(ctx, driver, query, params, neo4j.EagerResultTransformer, neo4j.ExecuteQueryWithDatabase("neo4j"))
	if err != nil {
		glog.Errorf("failed to execute query: %v", err)
		return
	}

	// Process the result and summary
	summary := result.Summary
	glog.Infof("Created or updated %v nodes in %+v.\n",
		summary.Counters().NodesCreated(),
		summary.ResultAvailableAfter())
}

// Function to merge an edge between two nodes based on their IDs
func mergeEdge(ctx context.Context, driver neo4j.DriverWithContext, edge *Edge) {
	// Construct the query to merge the edge between the source and destination nodes
	query := fmt.Sprintf(`
		MATCH (a:%s {id: '%s'}), (b:%s {id: '%s'})
		MERGE (a)-[r:%s]->(b)
		ON CREATE SET r.status = '%s'
		ON MATCH SET r.status = '%s'
		RETURN r`, edge.Source.Type, edge.Source.ID, edge.Destination.Type, edge.Destination.ID, edge.Type, edge.Status, edge.Status) // Using edge.Type directly in the query

	glog.Infof("edge query into db: %s", query)

	// Execute the query, passing the necessary parameters
	result, err := neo4j.ExecuteQuery(ctx, driver, query, map[string]any{
		"sourceID": edge.Source.ID,
		"destID":   edge.Destination.ID,
		"type":     edge.Type,
		"status":   edge.Status,
	}, neo4j.EagerResultTransformer, neo4j.ExecuteQueryWithDatabase("neo4j"))

	if err != nil {
		glog.Errorf("failed to execute query: %v", err)
		return
	}

	// Process the result and print the merged edge details
	for _, record := range result.Records {
		relationship := record.Values[0].(neo4j.Relationship)
		glog.Infof("Merged Edge: ID=%v, Type=%v, Properties=%v\n", relationship.ElementId, relationship.Type, relationship.Props)
	}

}
