package worker

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/GoogleCloudPlatform/microservices-demo/src/orderservice/pkg/model"
	"github.com/GoogleCloudPlatform/microservices-demo/src/orderservice/pkg/repository"
	"github.com/apache/rocketmq-client-go/v2"
	"github.com/apache/rocketmq-client-go/v2/consumer"
	"github.com/apache/rocketmq-client-go/v2/primitive"
	"github.com/sirupsen/logrus"
)

type DLQConsumer struct {
	client rocketmq.PushConsumer
	repo   repository.OrderRepo
}

func NewDLQConsumer(addr string, repo repository.OrderRepo) (*DLQConsumer, error) {
	c, err := rocketmq.NewPushConsumer(
		// 监听 DLQ 的 Consumer Group，建议单独命名防止冲突，或复用原Group(需小心)
		// 这里使用独立的 Group 来消费死信
		consumer.WithGroupName("DLQMonitorGroup"),
		consumer.WithNsResolver(primitive.NewPassthroughResolver([]string{addr})),
	)
	if err != nil {
		return nil, err
	}

	dc := &DLQConsumer{
		client: c,
		repo:   repo,
	}
	return dc, nil
}

func (dc *DLQConsumer) Start(ctx context.Context, wg *sync.WaitGroup) error {
	wg.Add(1)
	go func() {
		defer wg.Done()
		<-ctx.Done()
		dc.Stop()
	}()

	// 订阅两个 Consumer Group 对应的 DLQ
	// PushConsumer 支持多次 Subscribe()，每个 Topic 共用同一个 consume() 回调
	dlqTopics := []string{"%DLQ%order_db_group", "%DLQ%order_status_group"}
	for _, topic := range dlqTopics {
		if err := dc.client.Subscribe(topic, consumer.MessageSelector{}, dc.consume); err != nil {
			return fmt.Errorf("failed to subscribe DLQ topic %s: %w", topic, err)
		}
	}

	return dc.client.Start()
}

func (dc *DLQConsumer) Stop() {
	if dc.client != nil {
		_ = dc.client.Shutdown()
	}
}

func (dc *DLQConsumer) consume(ctx context.Context, msgs ...*primitive.MessageExt) (consumer.ConsumeResult, error) {
	for _, msg := range msgs {
		body := string(msg.Body)

		// Get DLQ Reason from properties
		reason := msg.GetProperty("dlq_reason")
		if reason == "" {
			reason = "Unknown (Manual Dead Letter)"
		}

		// 1. Alert (In production this would trigger PagerDuty)
		logrus.WithFields(logrus.Fields{
			"msg_id": msg.MsgId,
			"reason": reason,
			"body":   body,
		}).Error("CRITICAL: DEAD LETTER MESSAGE DETECTED")

		// 2. Parse OrderID (Best Effort)
		var partialOrder struct {
			OrderID string `json:"order_id"`
		}
		// Try to parse, ignore error as body might be malformed which is why it's in DLQ
		_ = json.Unmarshal(msg.Body, &partialOrder)

		// 3. Persist to DB using Repo
		failedOrder := &model.FailedOrder{
			OrderID:      partialOrder.OrderID, // Might be empty
			OriginalJSON: body,
			ErrorReason:  reason,
			CreatedAt:    time.Now(),
		}

		if err := dc.repo.InsertFailedOrder(ctx, failedOrder); err != nil {
			logrus.Errorf("Failed to persist failed order: %v", err)
			// If we fail to save to DB, strictly speaking we should RetryLater,
			// but for DLQ monitoring we usually don't want to loop forever.
			// Return RetryLater to be safe.
			return consumer.ConsumeRetryLater, nil
		}
	}

	return consumer.ConsumeSuccess, nil
}
