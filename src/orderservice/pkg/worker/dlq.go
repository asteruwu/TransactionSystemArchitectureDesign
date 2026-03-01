package worker

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/GoogleCloudPlatform/microservices-demo/src/orderservice/pkg/model"
	"github.com/GoogleCloudPlatform/microservices-demo/src/orderservice/pkg/repository"
	rmq_client "github.com/apache/rocketmq-clients/golang/v5"
	"github.com/apache/rocketmq-clients/golang/v5/credentials"
	"github.com/sirupsen/logrus"
)

type DLQConsumer struct {
	client rmq_client.SimpleConsumer
	repo   repository.OrderRepo
}

func NewDLQConsumer(addr string, accessKey string, accessSecret string, repo repository.OrderRepo) (*DLQConsumer, error) {
	c, err := rmq_client.NewSimpleConsumer(
		&rmq_client.Config{
			Endpoint:      addr,
			ConsumerGroup: "DLQMonitorGroup",
			Credentials: &credentials.SessionCredentials{
				AccessKey:    accessKey,
				AccessSecret: accessSecret,
			},
		},
		rmq_client.WithSimpleAwaitDuration(5*time.Second),
		rmq_client.WithSimpleSubscriptionExpressions(map[string]*rmq_client.FilterExpression{
			"%DLQ%order_db_group":     rmq_client.SUB_ALL,
			"%DLQ%order_status_group": rmq_client.SUB_ALL,
		}),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create DLQ simple consumer: %w", err)
	}

	return &DLQConsumer{client: c, repo: repo}, nil
}

func (dc *DLQConsumer) Start(ctx context.Context, wg *sync.WaitGroup) error {
	wg.Add(1)
	go func() {
		defer wg.Done()
		dc.consume(ctx)
	}()

	return dc.client.Start()
}

func (dc *DLQConsumer) Stop() {
	if dc.client != nil {
		_ = dc.client.GracefulStop()
	}
}

func (dc *DLQConsumer) consume(ctx context.Context) {
	const (
		dlqBatchSize        = int32(10)
		dlqInvisibleTimeout = 30 * time.Second
	)

	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		mvs, err := dc.client.Receive(ctx, dlqBatchSize, dlqInvisibleTimeout)
		if err != nil {
			if isDeadlineExceededError(err) {
				continue
			}
			logrus.Warnf("[DLQConsumer] Receive error: %v", err)
			continue
		}

		for _, mv := range mvs {
			body := string(mv.GetBody())
			reason := mv.GetProperties()["dlq_reason"]
			if reason == "" {
				reason = "Unknown (Manual Dead Letter)"
			}

			// 1. Alert
			logrus.WithFields(logrus.Fields{
				"msg_id": mv.GetMessageId(),
				"reason": reason,
				"body":   body,
			}).Error("CRITICAL: DEAD LETTER MESSAGE DETECTED")

			// 2. Parse OrderID (Best Effort)
			var partialOrder struct {
				OrderID string `json:"order_id"`
			}
			_ = json.Unmarshal(mv.GetBody(), &partialOrder)

			// 3. Persist to DB
			failedOrder := &model.FailedOrder{
				OrderID:      partialOrder.OrderID,
				OriginalJSON: body,
				ErrorReason:  reason,
				CreatedAt:    time.Now(),
			}

			if err := dc.repo.InsertFailedOrder(ctx, failedOrder); err != nil {
				logrus.Errorf("[DLQConsumer] Failed to persist dead letter message %s: %v (will retry)", mv.GetMessageId(), err)
				// 不 ACK，等待 invisibleDuration 到期后 Broker 重投
				continue
			}

			// 持久化成功：ACK
			if err := dc.client.Ack(ctx, mv); err != nil {
				logrus.Warnf("[DLQConsumer] Failed to ack message %s: %v", mv.GetMessageId(), err)
			}
		}
	}
}
