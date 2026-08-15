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

func (p *KafkaProducer) Publish(
	ctx context.Context,
	topic string,
	key string,
	value []byte,
) error {

	record := &kgo.Record{
		Topic: topic,
		Key:   []byte(key),
		Value: value,
	}

	return p.client.ProduceSync(ctx, record).FirstErr()
}

func (p *KafkaProducer) Close() {
	p.client.Close()
}
