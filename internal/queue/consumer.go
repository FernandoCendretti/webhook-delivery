package queue

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/segmentio/kafka-go"
)

// ConsumerConfig holds the settings required to create a Kafka consumer.
type ConsumerConfig struct {
	Brokers           []string
	Topic             string
	GroupID           string
	Logger            *slog.Logger
	SessionTimeout    time.Duration // 0 = kafka-go default (30 s)
	HeartbeatInterval time.Duration // 0 = kafka-go default (3 s)
	// StartOffset controls where a new consumer group starts reading.
	// 0 means kafka.LastOffset (the default — skip messages published before joining).
	// Set to kafka.FirstOffset (-2) to read from the beginning; useful in tests.
	StartOffset int64
}

// Consumer wraps a kafka.Reader to provide manual-commit message consumption.
type Consumer struct {
	reader *kafka.Reader
	logger *slog.Logger
}

// NewConsumer creates a Consumer using the provided configuration.
func NewConsumer(cfg ConsumerConfig) *Consumer {
	if cfg.Logger == nil {
		cfg.Logger = slog.Default()
	}
	startOffset := kafka.LastOffset
	if cfg.StartOffset != 0 {
		startOffset = cfg.StartOffset
	}
	r := kafka.NewReader(kafka.ReaderConfig{
		Brokers:           cfg.Brokers,
		Topic:             cfg.Topic,
		GroupID:           cfg.GroupID,
		MinBytes:          1,
		MaxBytes:          10e6,
		StartOffset:       startOffset,
		CommitInterval:    0,
		SessionTimeout:    cfg.SessionTimeout,
		HeartbeatInterval: cfg.HeartbeatInterval,
	})
	return &Consumer{reader: r, logger: cfg.Logger}
}

// FetchMessage blocks until a message is available or ctx is cancelled.
func (c *Consumer) FetchMessage(ctx context.Context) (kafka.Message, error) {
	msg, err := c.reader.FetchMessage(ctx)
	if err != nil {
		return kafka.Message{}, fmt.Errorf("fetch from %s: %w", c.reader.Config().Topic, err)
	}
	return msg, nil
}

// Commit marks msg's offset as processed in the consumer group.
func (c *Consumer) Commit(ctx context.Context, msg kafka.Message) error {
	if err := c.reader.CommitMessages(ctx, msg); err != nil {
		return fmt.Errorf("commit %s offset %d: %w", c.reader.Config().Topic, msg.Offset, err)
	}
	return nil
}

// Close shuts down the underlying Kafka reader.
func (c *Consumer) Close() error {
	return c.reader.Close()
}
