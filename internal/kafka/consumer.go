package kafka

import (
	"bmpConsumer/proto"
	"context"
	"log"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	"google.golang.org/protobuf/proto"
)

type KafkaConsumer struct {
	Consumer *kafka.Consumer
}

func NewKafkaConsumer(brokers string, groupID string) (*KafkaConsumer, error) {
	c, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers": brokers,
		"group.id":          groupID,
		"auto.offset.reset": "earliest",
	})
	if err != nil {
		return nil, err
	}
	return &KafkaConsumer{Consumer: c}, nil
}

func (kc *KafkaConsumer) SubscribeTopics(topics []string) error {
	return kc.Consumer.SubscribeTopics(topics, nil)
}

func (kc *KafkaConsumer) ConsumeMessages(ctx context.Context, processMessage func([]byte)) error {
	for {
		msg, err := kc.Consumer.ReadMessage(-1)
		if err != nil {
			log.Printf("Error reading message: %v\n", err)
			continue
		}
		processMessage(msg.Value)
	}
}

func DecodeMessage(topic string, msg []byte) {
	if topic == "goevpn" {
		evpn := &proto.EVPNPrefix{}
		if err := proto.Unmarshal(msg, evpn); err != nil {
			log.Printf("Error unmarshalling EVPNPrefix: %v\n", err)
		} else {
			log.Printf("EVPNPrefix message: %+v\n", evpn)
		}
	} else if topic == "gopeer" {
		peerChange := &proto.PeerStateChange{}
		if err := proto.Unmarshal(msg, peerChange); err != nil {
			log.Printf("Error unmarshalling PeerStateChange: %v\n", err)
		} else {
			log.Printf("PeerStateChange message: %+v\n", peerChange)
		}
	}
}
