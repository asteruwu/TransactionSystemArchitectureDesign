package repository

import (
	"context"
	"fmt"
	"time"

	redis "github.com/redis/go-redis/v9"
	"github.com/sirupsen/logrus"
)

const (
	DeadStreamKey = "mq:dead:letter"
)

// DeadLetterProducer 死信队列生产者
type DeadLetterProducer struct {
	rdb *redis.Client
	log *logrus.Logger
}

// NewDeadLetterProducer 构造死信队列生产者
func NewDeadLetterProducer(rdb *redis.Client, log *logrus.Logger) *DeadLetterProducer {
	return &DeadLetterProducer{rdb: rdb, log: log}
}

// SendToDeadLetter 将一条毒丸消息写入死信队列
func (d *DeadLetterProducer) SendToDeadLetter(ctx context.Context, originalStream, consumerGroup, msgID, payload, errorReason string) error {
	err := d.rdb.XAdd(ctx, &redis.XAddArgs{
		Stream: DeadStreamKey,
		Values: map[string]interface{}{
			"original_stream": originalStream,
			"consumer_group":  consumerGroup,
			"msg_id":          msgID,
			"payload":         payload,
			"error_reason":    errorReason,
			"created_at":      time.Now().UnixMilli(),
		},
	}).Err()

	if err != nil {
		d.log.Errorf("[DeadLetter] Failed to write dead letter (stream=%s, msgID=%s): %v", originalStream, msgID, err)
		return fmt.Errorf("write dead letter: %w", err)
	}

	d.log.Warnf("[DeadLetter] Poisoned message sent to dead stream (stream=%s, group=%s, msgID=%s, reason=%s)",
		originalStream, consumerGroup, msgID, errorReason)
	return nil
}
