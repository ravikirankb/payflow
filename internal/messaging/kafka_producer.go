package messaging

import (
	"context"

	"github.com/twmb/franz-go/pkg/kgo"
)

type KafkaProducer struct {
	client *kgo.Client
}

func NewKafkaProducer(brokers []string) (*KafkaProducer, error) {
	client, err := kgo.NewClient(
		kgo.SeedBrokers(brokers...),
	)
	if err != nil {
		return nil, err
	}

	return &KafkaProducer{client: client}, nil
}

// Publish sends an event to Kafka.
//
// The payment ID is used as the Kafka record key so all events for
// the same payment are routed to the same partition, preserving
// ordering for that payment.
//
// The event ID is added as a Kafka header so consumers can identify
// the exact outbox event independently from the payment itself.
func (p *KafkaProducer) Publish(
	ctx context.Context,
	topic string,
	key string,
	eventID string,
	value []byte,
) error {

	record := &kgo.Record{
		Topic: topic,
		Key:   []byte(key),
		Value: value,

		// Headers allow us to attach metadata to the Kafka record
		// without modifying the event payload itself.
		Headers: []kgo.RecordHeader{
			{
				Key:   "event_id",
				Value: []byte(eventID),
			},
		},
	}

	return p.client.ProduceSync(ctx, record).FirstErr()
}

func (p *KafkaProducer) Close() {
	p.client.Close()
}
