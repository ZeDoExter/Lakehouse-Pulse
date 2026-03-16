package main

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/segmentio/kafka-go"
)

type kafkaProducer struct {
	writer *kafka.Writer
}

func newKafkaProducer(brokers []string, topic string, writeTimeout time.Duration) *kafkaProducer {
	return &kafkaProducer{
		writer: &kafka.Writer{
			Addr:         kafka.TCP(brokers...),
			Topic:        topic,
			Balancer:     &kafka.LeastBytes{},
			WriteTimeout: writeTimeout,
		},
	}
}

func (p *kafkaProducer) close() {
	_ = p.writer.Close()
}

func (p *kafkaProducer) publishWithRetry(ctx context.Context, metrics *metricState, message map[string]any, maxRetries int, initialBackoff time.Duration) error {
	data, err := json.Marshal(message)
	if err != nil {
		return fmt.Errorf("marshal message: %w", err)
	}

	var lastErr error
	for attempt := 1; attempt <= maxRetries; attempt++ {
		if err := p.writer.WriteMessages(ctx, kafka.Message{Value: data}); err != nil {
			lastErr = fmt.Errorf("attempt %d publish failed: %w", attempt, err)
			metrics.incPublishError()
			if attempt < maxRetries {
				backoff := initialBackoff * time.Duration(1<<(attempt-1))
				if backoff > 30*time.Second {
					backoff = 30 * time.Second
				}
				select {
				case <-ctx.Done():
					return ctx.Err()
				case <-time.After(backoff):
				}
			}
			continue
		}
		metrics.incPublished()
		return nil
	}
	return lastErr
}
