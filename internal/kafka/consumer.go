/*
consumer, consumes gobmp evpn prefix messages written into kakfa by a gobmp client. It tries to build a logical map of the data center network and servers.
In our setup we are retrving information not directly from the devices. Thus we can not get enough information about the source of the BGP update like BGP id e.t.c.
This may a problem while binding the prefix e.t.c to a remote peer if we have multiplepaths. It is not possible which of the remote peer is advertising the a particular multipath candidate (BGP anycast)
*/
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
	//	2: processEVPNType2Prefix,
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
	// Subscribe to topics
	err := c.kafkaConsumer.SubscribeTopics(topics, nil)
	if err != nil {
		glog.Fatalf("failed to subscribe to topics: %v", err)
	}

	// Graceful shutdown setup: create a channel to listen for context cancellation
	messageProcessingDone := make(chan struct{})

	// Goroutine for message processing
	go func() {
		defer close(messageProcessingDone)
		for {
			select {
			case <-ctx.Done(): // context has been cancelled or timeout
				glog.Info("received shutdown signal, stopping message processing...")
				return
			default:
				// Read a message from Kafka with a timeout
				msg, err := c.kafkaConsumer.ReadMessage(100 * time.Millisecond) // Use a short timeout for graceful shutdown
				if err != nil {
					if kafkaError, ok := err.(kafka.Error); ok && kafkaError.Code() == kafka.ErrTimedOut {
						continue
					}
					glog.Errorf("error reading message: %v", err) // should we backoff or retry?
					continue
				}

				// Process the message based on the topic
				switch *msg.TopicPartition.Topic {
				case "gobmp.parsed.evpn":
					glog.Infof("handling msg: %v from topic: %v", msg.Value, *msg.TopicPartition.Topic)
					processEVPNPrefix(ctx, msg.Value, r, neo4j, nmap)
				default:
					glog.Errorf("unhandled topic: %v", *msg.TopicPartition.Topic)
				}
			}
		}
	}()

	// Wait for message processing to stop when the context is done
	<-ctx.Done()
	glog.Info("context cancelled, waiting for message processing to stop...")
	<-messageProcessingDone // Wait for the goroutine to finish
	glog.Info("message processing stopped")
}

// processEVPNPrefix deserialize and process EVPN message (EVPNPrefix)
func processEVPNPrefix(ctx context.Context, data []byte, redisClient *redis.RedisClient, neo4j *neo4j.DriverWithContext, nmap *cfg.NetworkMap) error {
	var evpnPrefix *message.EVPNPrefix
	err := json.Unmarshal(data, &evpnPrefix)
	if err != nil {
		return fmt.Errorf("failed to deserialize evpn prefix: %v", err)
	}
	if processFunc, ok := processFuncMap[evpnPrefix.RouteType]; ok {
		err = processFunc(ctx, redisClient, evpnPrefix, neo4j, nmap)
		if err != nil {
			return fmt.Errorf("error processing evpn route type %d message: %v", evpnPrefix.RouteType, err)
		}
		return nil
	} else {
		return fmt.Errorf("unsupported route type %+v", evpnPrefix.RouteType)
	}
}

// processEVPNType2Prefix handles BGP EVPN Type 2 NLRI which includes host/mac/server information
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
		// in our design remotePeer is a Spine
		remotePeer := Node{
			Type:       tools.GetNodeType(message.PeerASN),
			Name:       strconv.FormatUint(uint64(message.PeerASN), 10),
			ID:         message.RemoteBGPID,
			Status:     status,
			Properties: []Attribute{{Name: "MGMT_IP", Value: tools.MgmtIPfromBGPID(message.PeerASN, message.RemoteBGPID, nmap)}},
		}
		processNode(ctx, redisClient, neo4j, &remotePeer)

		// check origin-as, it should not be sth and we must know its bgp-id which should be unique inside the data center and it can be used as id the database
		if message.OriginAS > 0 {
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
				// binding origin-as, leaf and host
				processEdge(ctx, redisClient, &host, &originAS, neo4j, "up")
				// binding remote-as and origin-as (spine and leaf)
				if message.BaseAttributes.ASPathCount == 2 {
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

// processEVPNType3Prefix handles BGP EVPN Type 3 NLRI basically all leafs or vxlan end points in the EVPN domain.
func processEVPNType3Prefix(ctx context.Context, redisClient *redis.RedisClient, message *message.EVPNPrefix, neo4j *neo4j.DriverWithContext, nmap *cfg.NetworkMap) error {
	glog.Infof("processing message: %v", message)
	if message.Action == "add" && message.BaseAttributes.ASPathCount == 2 {
		remotePeer := Node{
			Type:       tools.GetNodeType(message.PeerASN),
			Name:       strconv.FormatUint(uint64(message.PeerASN), 10),
			ID:         message.RemoteBGPID,
			Status:     "up",
			Properties: []Attribute{{Name: "MGMT_IP", Value: tools.MgmtIPfromBGPID(message.PeerASN, message.RemoteBGPID, nmap)}},
		}
		processNode(ctx, redisClient, neo4j, &remotePeer)

		originBGPID, err := tools.GetBGPIDfromAS(message.BaseAttributes.ASPath[1], nmap)
		//We need an id for node for insterting into database
		if err == nil {
			originAS := Node{
				Type:       tools.GetNodeType(message.BaseAttributes.ASPath[1]),
				Name:       strconv.FormatUint(uint64(message.BaseAttributes.ASPath[1]), 10),
				ID:         originBGPID,
				Status:     "up",
				Properties: []Attribute{{Name: "MGMT_IP", Value: tools.MgmtIPfromBGPID(message.BaseAttributes.ASPath[1], originBGPID, nmap)}},
			}
			processNode(ctx, redisClient, neo4j, &originAS)
			if message.BaseAttributes.ASPathCount == 2 {
				if message.PeerASN > uint32(message.OriginAS) {
					processEdge(ctx, redisClient, &originAS, &remotePeer, neo4j, "up")
				} else {
					processEdge(ctx, redisClient, &remotePeer, &originAS, neo4j, "up")
				}
			}
			return nil
		} else {
			return fmt.Errorf("failed to find bgpid for the origin as: %v, can not bind host", err)
		}
	}
	return fmt.Errorf("as path count is more than in our structure, message: %v", message)
}

// processEVPNType5Prefix handles BGP EVPN Type 5 NLRI, external information and connected ip interfaces/networks
func processEVPNType5Prefix(ctx context.Context, redisClient *redis.RedisClient, message *message.EVPNPrefix, neo4j *neo4j.DriverWithContext, nmap *cfg.NetworkMap) error {
	if message.Action == "add" {
		//We are not instrested in spine connected prefixes.
		if message.BaseAttributes.ASPathCount > 1 {
			for i := 0; i < len(message.BaseAttributes.ASPath)-1; i++ {
				sourceAS := message.BaseAttributes.ASPath[i]
				destinationAS := message.BaseAttributes.ASPath[i+1]

				if isIntraFabric(sourceAS, destinationAS) {
					glog.Infof("intra fabric link source/spine as:%v destination as:%v", sourceAS, destinationAS)
					var sourceBGPID string
					if i == 0 {
						//first as must be remote peer/spine
						sourceBGPID = message.RemoteBGPID
					} else {
						bgpid, err := tools.GetBGPIDfromAS(sourceAS, nmap)
						if err != nil {
							if tools.GetNodeType(sourceAS) != "LEAF" && tools.GetNodeType(sourceAS) != "SPINE" {
								sourceBGPID = bgpid
							} else {
								return fmt.Errorf("can not set bgp id for destination as:%v and nodeType:%v", sourceAS, tools.GetNodeType(sourceAS))
							}
						}
					}

					destinationBGPID, err := tools.GetBGPIDfromAS(destinationAS, nmap)
					if err != nil {
						if tools.GetNodeType(destinationAS) != "LEAF" && tools.GetNodeType(destinationAS) != "SPINE" {
							destinationBGPID = strconv.FormatUint(uint64(destinationAS), 10)
						} else {
							return fmt.Errorf("can not set bgp id for destination as:%v and nodeType:%v", destinationAS, tools.GetNodeType(destinationAS))
						}
					}

					source := Node{
						Type:       tools.GetNodeType(sourceAS),
						Name:       strconv.FormatUint(uint64(sourceAS), 10),
						ID:         sourceBGPID,
						Status:     "up",
						Properties: []Attribute{{Name: "MGMT_IP", Value: tools.MgmtIPfromBGPID(sourceAS, message.RemoteBGPID, nmap)}},
					}
					processNode(ctx, redisClient, neo4j, &source)

					destination := Node{
						Type:       tools.GetNodeType(destinationAS),
						Name:       strconv.FormatUint(uint64(destinationAS), 10),
						ID:         destinationBGPID,
						Status:     "up",
						Properties: []Attribute{{Name: "MGMT_IP", Value: tools.MgmtIPfromBGPID(destinationAS, destinationBGPID, nmap)}},
					}

					processNode(ctx, redisClient, neo4j, &destination)
					processEdge(ctx, redisClient, &destination, &source, neo4j, "up")

					if i == len(message.BaseAttributes.ASPath)-2 {
						ipkey := message.IPAddress + "/" + fmt.Sprintf("%d", message.IPLength)
						//create or update node for prefix
						prefix := Node{
							Type:   "PREFIX",
							Name:   ipkey,
							ID:     ipkey,
							Status: "up",
							Properties: []Attribute{
								{Name: "Prefix", Value: message.IPAddress},
								{Name: "Length", Value: strconv.FormatUint(uint64(message.IPLength), 10)},
							},
						}
						processNode(ctx, redisClient, neo4j, &prefix)
						processEdge(ctx, redisClient, &prefix, &destination, neo4j, "up")
					}

				}
				if isInterFabric(sourceAS, destinationAS) {
					glog.Infof("inter fabric link source/spine as:%v destination as:%v", sourceAS, destinationAS)
					//interfabris requires different handling depending on the interfabric design! full-mesh other?
					//we can only build interfabric links if know source and destination node id(bgpid) as we are using same as numbers on spine and superspines
					//in our design remotePeer is a Spine
					source := Node{
						Type:       tools.GetNodeType(message.PeerASN),
						Name:       strconv.FormatUint(uint64(message.PeerASN), 10),
						ID:         message.RemoteBGPID,
						Status:     "up",
						Properties: []Attribute{{Name: "MGMT_IP", Value: tools.MgmtIPfromBGPID(message.PeerASN, message.RemoteBGPID, nmap)}},
					}
					processNode(ctx, redisClient, neo4j, &source)
					//dedicated id for each superspine corresponing the spine
					parsedIPParts := strings.Split(message.RemoteBGPID, ".")
					destination := Node{
						Type:       tools.GetNodeType(destinationAS),
						Name:       strconv.FormatUint(uint64(destinationAS), 10),
						ID:         parsedIPParts[3],
						Status:     "up",
						Properties: []Attribute{{Name: "MGMT_IP", Value: tools.MgmtIPfromBGPID(destinationAS, message.RemoteBGPID, nmap)}},
					}
					processNode(ctx, redisClient, neo4j, &destination)
					processEdge(ctx, redisClient, &source, &destination, neo4j, "up")
					/* full mesh inter-site
					create mergeEdge function that will add links between each source and destination
					MATCH (spine:Spine), (superspine:SuperSpine)
					WHERE spine.superSpineID = superspine.ID
					CREATE (spine)-[:CONNECTS_TO]->(superspine)
					*/
					return nil
				}

				if !isPrivateASN(destinationAS) {
					glog.Infof("public link source/spine as:%v destination as:%v", sourceAS, destinationAS)

					//assume source is public asn
					source := Node{
						Type:   tools.GetNodeType(sourceAS),
						Name:   strconv.FormatUint(uint64(sourceAS), 10),
						ID:     strconv.FormatUint(uint64(sourceAS), 10),
						Status: "up",
					}
					if isPrivateASN(sourceAS) {
						sourceBGPID, sourceErr := tools.GetBGPIDfromAS(sourceAS, nmap)
						if sourceErr != nil {
							if tools.GetNodeType(sourceAS) != "LEAF" && tools.GetNodeType(sourceAS) != "SPINE" {
								sourceBGPID = strconv.FormatUint(uint64(sourceAS), 10)
							} else {
								return fmt.Errorf("can not set bgp id for source as:%v and nodeType:%v", sourceAS, tools.GetNodeType(sourceAS))
							}
						}
						source = Node{
							Type:       tools.GetNodeType(sourceAS),
							Name:       strconv.FormatUint(uint64(sourceAS), 10),
							ID:         sourceBGPID,
							Status:     "up",
							Properties: []Attribute{{Name: "MGMT_IP", Value: tools.MgmtIPfromBGPID(sourceAS, sourceBGPID, nmap)}},
						}
					}
					destination := Node{
						Type:   "public",
						Name:   strconv.FormatUint(uint64(destinationAS), 10),
						ID:     strconv.FormatUint(uint64(destinationAS), 10),
						Status: "up",
					}
					processNode(ctx, redisClient, neo4j, &source)
					processNode(ctx, redisClient, neo4j, &destination)
					processEdge(ctx, redisClient, &destination, &source, neo4j, "up")

					if i+1 == len(message.BaseAttributes.ASPath)-1 {
						ipkey := message.IPAddress + "/" + fmt.Sprintf("%d", message.IPLength)
						//create or update node for prefix
						prefix := Node{
							Type:   "PREFIX",
							Name:   ipkey,
							ID:     ipkey,
							Status: "up",
							Properties: []Attribute{
								{Name: "Prefix", Value: message.IPAddress},
								{Name: "Length", Value: strconv.FormatUint(uint64(message.IPLength), 10)},
							},
						}
						processNode(ctx, redisClient, neo4j, &prefix)
						processEdge(ctx, redisClient, &prefix, &destination, neo4j, "up")
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

// isPrivateASN returns truee if its a private ASN number
func isPrivateASN(asn uint32) bool {
	privateRanges := [][2]uint32{
		{64512, 65534},           // 16-bit private ASN range
		{4200000000, 4294967294}, // 32-bit private ASN range
	}

	// Check if the ASN falls within any of the private ranges
	for _, r := range privateRanges {
		if asn >= r[0] && asn <= r[1] {
			return true
		}
	}
	return false
}

// isIntraFabric returns true if both as are in the same data center and same fabric
func isIntraFabric(a, b uint32) bool {
	if !isPrivateASN(a) || !isPrivateASN(b) {
		return false
	}
	// Convert uint32 numbers to strings
	strA := strconv.FormatUint(uint64(a), 10)
	strB := strconv.FormatUint(uint64(b), 10)

	// If either string is shorter than 10 characters, return false
	if len(strA) < 10 || len(strB) < 10 {
		return false
	}
	// Compare the first four characters of each string
	return strA[:7] == strB[:7]
}

// isInterFabric return true if both as are in the same datacenter
func isInterFabric(a, b uint32) bool {
	if !isPrivateASN(a) || !isPrivateASN(b) {
		return false
	}
	strA := strconv.FormatUint(uint64(a), 10)
	strB := strconv.FormatUint(uint64(b), 10)

	// If either string is shorter than 10 characters, return false
	if len(strA) < 10 || len(strB) < 10 {
		return false
	}
	// Compare the first four characters of each string
	return (strA[:4] == strB[:4]) && (strA[:7] != strB[:7])
}
