package main

import (
	"bmpConsumer/config"
	consumer "bmpConsumer/internal/kafka"
	redisClient "bmpConsumer/internal/redis"
	"context"
	"flag"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/golang/glog"
	"github.com/neo4j/neo4j-go-driver/v5/neo4j"
)

func main() {
	// Initialize glog
	flag.Parse()
	defer glog.Flush()

	// Set logtostderr flag
	_ = flag.Set("logtostderr", "true")

	// Load the configuration
	cfg, err := config.LoadConfig()
	if err != nil {
		glog.Errorf("Failed to load configuration: %v", err)
		os.Exit(1)
	}
	glog.Infof("Full Config: %+v", cfg)

	// Create context that listens for the interrupt signal from the OS.
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	// Initialize Kafka consumer
	kafkaConsumer, err := consumer.NewConsumer(&cfg, ctx)
	if err != nil {
		glog.Fatalf("Failed to initialize Kafka consumer: %v", err)
	}
	// Initialize Redis client
	redisClient := redisClient.NewRedisClient(&cfg)
	// Initialize Neo4j client

	neo4jClient, err := neo4j.NewDriverWithContext(
		cfg.Neo4j.Address,
		neo4j.BasicAuth(cfg.Neo4j.Username, cfg.Neo4j.Password, ""))
	if err != nil {
		panic(err)
	}
	defer neo4jClient.Close(ctx)

	err = neo4jClient.VerifyConnectivity(ctx)
	if err != nil {
		panic(err)
	}
	glog.Info("neo4j connection established.")

	// Define the topics to subscribe to
	topics := []string{"gobmp.parsed.peer", "gobmp.parsed.evpn"}
	// Start processing Kafka messages in a separate goroutine
	go kafkaConsumer.ProcessMessages(ctx, topics, redisClient, &neo4jClient, &cfg.DCDetails)

	// Wait for the context to be canceled (when Ctrl+C is pressed)
	<-ctx.Done()

	glog.Info("Received termination signal, shutting down...")

	// Allow for a graceful shutdown
	kafkaConsumer.Close()
	redisClient.Close()
	glog.Flush()

	// Wait for a short period to ensure cleanup
	time.Sleep(2 * time.Second)
}
