package main

import (
	"bmpConsumer/internal/kafka"
	"bmpConsumer/internal/redis"
	"context"
	"log"
	"os"
)

func main() {
	ctx := context.Background()

	kafkaBrokers := os.Getenv("KAFKA_BROKER")
	redisAddr := os.Getenv("REDIS_ADDRESS")

	kafkaConsumer, err := kafka.NewKafkaConsumer(kafkaBrokers, "group-1")
	if err != nil {
		log.Fatalf("Failed to create Kafka consumer: %v\n", err)
	}

	redisClient := redis.NewRedisClient(redisAddr, "", 0)

	topics := []string{"gopeer", "goevpn"}
	kafkaConsumer.SubscribeTopics(topics)

	processMessage := func(message []byte) {
		topic := kafkaConsumer.Consumer.Assignment()[0].Topic // Get topic name
		kafka.DecodeMessage(*topic, message)

		// Example of writing to Redis
		redisClient.WriteMessage("some-key", "some-value") // Modify based on your logic
	}

	log.Println("Starting Kafka consumer...")
	if err := kafkaConsumer.ConsumeMessages(ctx, processMessage); err != nil {
		log.Fatalf("Failed to consume messages: %v\n", err)
	}
}
