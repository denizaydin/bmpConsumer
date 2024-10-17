package config

import (
	"encoding/json"
	"fmt"
	"os"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/golang/glog"
)

// Config holds the overall configuration for the service
type Config struct {
	Kafka     KafkaConfig
	Redis     RedisConfig
	Neo4j     Neo4jConfig // Added Neo4j config
	DCDetails NetworkMap
}

// KafkaConfig represents the Kafka-related configuration
type KafkaConfig struct {
	Broker    string
	GroupID   string
	ConfigMap *kafka.ConfigMap
}

// RedisConfig represents the Redis-related configuration
type RedisConfig struct {
	Address  string
	CacheTTL int
}

// Neo4jConfig represents the Neo4j-related configuration
type Neo4jConfig struct {
	Address  string
	Username string
	Password string
}

// NetworkDetails holds the IGP and MGMT network info for an AS
type NetworkDetails struct {
	IGPNetwork  string `json:"igpNetwork"`
	MGMTNetwork string `json:"mgmtNetwork"`
}

// NetworkMap holds the mapping of AS numbers to NetworkDetails
type NetworkMap map[uint32]NetworkDetails

// LoadConfig loads the configuration from environment variables and returns the Config struct
func LoadConfig() (Config, error) {
	kafkaBroker := getEnv("KAFKA_BROKER", "")
	kafkaGroupID := getEnv("KAFKA_GROUP_ID", "")
	redisAddress := getEnv("REDIS_ADDRESS", "")
	neo4jAddress := getEnv("NEO4J_ADDRESS", "")
	neo4jUsername := getEnv("NEO4J_USERNAME", "")
	neo4jPassword := getEnv("NEO4J_PASSWORD", "")

	// Get the environment variable for NETWORK_MAP
	envConfig := os.Getenv("NETWORK_MAP")
	if envConfig == "" {
		fmt.Println("Environment variable NETWORK_MAP is not set.")
	}

	// Create a variable to hold the parsed data
	var networkMap NetworkMap

	// Parse the JSON into the map
	err := json.Unmarshal([]byte(envConfig), &networkMap)
	if err != nil {
		fmt.Printf("Error parsing NETWORK_MAP: %v\n", err)
	}

	// Validate mandatory environment variables
	if kafkaBroker == "" || kafkaGroupID == "" || redisAddress == "" || neo4jAddress == "" || neo4jUsername == "" || neo4jPassword == "" {
		return Config{}, fmt.Errorf("one or more environment variables are missing")
	}

	// Create Kafka ConfigMap to pass to the consumer
	kafkaConfigMap := &kafka.ConfigMap{
		"bootstrap.servers": kafkaBroker,
		"group.id":          kafkaGroupID,
		"auto.offset.reset": "earliest", // Optionally configure other Kafka settings here
	}

	// Log the loaded configuration
	glog.Infof("Loaded configuration: Kafka Broker=%s, Kafka GroupID=%s, Redis Address=%s, Neo4j Address=%s", kafkaBroker, kafkaGroupID, redisAddress, neo4jAddress)
	if len(networkMap) == 0 {
		glog.Warning("empty networkmap")
	}
	// Example: Walk through the map and print the AS numbers and their details
	for asNumber, details := range networkMap {
		glog.Infof("AS: %v, IGP Network: %s, MGMT Network: %s\n", asNumber, details.IGPNetwork, details.MGMTNetwork)
	}

	// Return the configuration struct
	return Config{
		Kafka: KafkaConfig{
			Broker:    kafkaBroker,
			GroupID:   kafkaGroupID,
			ConfigMap: kafkaConfigMap, // Pass the config map here
		},
		Redis: RedisConfig{
			Address: redisAddress,
		},
		Neo4j: Neo4jConfig{
			Address:  neo4jAddress,
			Username: neo4jUsername,
			Password: neo4jPassword,
		},
		DCDetails: networkMap,
	}, nil
}

// getEnv is a helper function that reads environment variables with an optional fallback
func getEnv(key, fallback string) string {
	if value, exists := os.LookupEnv(key); exists {
		return value
	}
	return fallback
}
