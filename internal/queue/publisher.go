package queue

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/segmentio/kafka-go"
)

// PublisherConfig holds the settings required to create a Kafka publisher.
type PublisherConfig struct {
	Brokers []string
	Topic   string
	Logger  *slog.Logger
}

// Publisher wraps a kafka.Writer to publish messages to a Kafka topic.
type Publisher struct {
	writer *kafka.Writer
	logger *slog.Logger
}

// NewPublisher creates a Publisher using the provided configuration.
func NewPublisher(cfg PublisherConfig) *Publisher {
	if cfg.Logger == nil {
		cfg.Logger = slog.Default()
	}
	w := &kafka.Writer{
		Addr:                   kafka.TCP(cfg.Brokers...),
		Topic:                  cfg.Topic,
		Balancer:               &kafka.Hash{},
		AllowAutoTopicCreation: true,
		RequiredAcks:           kafka.RequireAll,
	}
	return &Publisher{writer: w, logger: cfg.Logger}
}

// Publish writes a single message with the given key and value to the topic.
func (p *Publisher) Publish(ctx context.Context, key, value []byte) error {
	if err := p.writer.WriteMessages(ctx, kafka.Message{Key: key, Value: value}); err != nil {
		return fmt.Errorf("publish to %s: %w", p.writer.Topic, err)
	}
	return nil
}

// PublishBatch writes multiple messages in a single call; it is a no-op when
// msgs is empty.
func (p *Publisher) PublishBatch(ctx context.Context, msgs []kafka.Message) error {
	if len(msgs) == 0 {
		return nil
	}
	if err := p.writer.WriteMessages(ctx, msgs...); err != nil {
		return fmt.Errorf("publish batch to %s: %w", p.writer.Topic, err)
	}
	return nil
}

// Close flushes pending messages and shuts down the underlying Kafka writer.
func (p *Publisher) Close() error {
	return p.writer.Close()
}
