package forwarder

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	rocketmq "github.com/apache/rocketmq-client-go/v2"
	"github.com/apache/rocketmq-client-go/v2/primitive"
	redis "github.com/redis/go-redis/v9"
	"github.com/sirupsen/logrus"
	"github.com/sony/gobreaker"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

const (
	streamKeyOrder = "mq:order:create"
	forwarderGroup = "group_order_forwarder"
	targetTopic    = "order_created"
)

// OrderForwarder 从本地 Redis Stream 读取订单消息，转发至 RocketMQ
type OrderForwarder struct {
	rdb          *redis.Client
	producer     rocketmq.Producer
	log          *logrus.Logger
	cb           *gobreaker.CircuitBreaker
	consumer     string
	currentLagMs int64
}

// NewOrderForwarder 构造 Forwarder Worker
func NewOrderForwarder(rdb *redis.Client, producer rocketmq.Producer, log *logrus.Logger) *OrderForwarder {
	hostname, _ := os.Hostname()
	if hostname == "" {
		hostname = "pod-unknown"
	}

	// 初始化熔断器
	st := gobreaker.Settings{
		Name:        "RocketMQ-Forwarder",
		MaxRequests: 3,
		Interval:    10 * time.Second,
		Timeout:     30 * time.Second,
		ReadyToTrip: func(counts gobreaker.Counts) bool {
			return counts.Requests >= 5 && float64(counts.TotalFailures)/float64(counts.Requests) >= 0.6
		},
		OnStateChange: func(name string, from gobreaker.State, to gobreaker.State) {
			log.Warnf("CircuitBreaker[%s] state changed from %s to %s", name, from, to)
		},
	}

	f := &OrderForwarder{
		rdb:      rdb,
		producer: producer,
		log:      log,
		cb:       gobreaker.NewCircuitBreaker(st),
		consumer: fmt.Sprintf("forwarder-%s", hostname),
	}
	f.registerMetrics()

	return f
}

func (f *OrderForwarder) registerMetrics() {
	meter := otel.GetMeterProvider().Meter("productcatalogservice.forwarder")
	// 1. Forwarder 转发延迟 (Redis Stream -> RocketMQ)
	meter.Int64ObservableGauge("app_forwarder_lag_ms",
		metric.WithDescription("Forwarder lag from Redis Stream to RocketMQ"),
		metric.WithInt64Callback(func(_ context.Context, obs metric.Int64Observer) error {
			obs.Observe(atomic.LoadInt64(&f.currentLagMs),
				metric.WithAttributes(attribute.String("group", forwarderGroup)))
			return nil
		}),
	)
}

// Start 启动 Forwarder Worker
func (f *OrderForwarder) Start(ctx context.Context, wg *sync.WaitGroup) {
	// 确保消费者组存在
	f.rdb.XGroupCreateMkStream(ctx, streamKeyOrder, forwarderGroup, "0")

	wg.Add(2)
	go f.startStreamConsumer(ctx, wg)
	go f.startRecovery(ctx, wg)
}

// startStreamConsumer 消费 Redis Stream
func (f *OrderForwarder) startStreamConsumer(ctx context.Context, wg *sync.WaitGroup) {
	defer wg.Done()
	f.log.Infof("[Forwarder] Started consuming stream %s", streamKeyOrder)

	for {
		select {
		case <-ctx.Done():
			f.log.Info("[Forwarder] Shutting down...")
			return
		default:
			entries, err := f.rdb.XReadGroup(ctx, &redis.XReadGroupArgs{
				Group:    forwarderGroup,
				Consumer: f.consumer,
				Streams:  []string{streamKeyOrder, ">"},
				Count:    50, // 批量读取
				Block:    2 * time.Second,
			}).Result()

			if err != nil {
				if err != redis.Nil {
					f.log.Errorf("[Forwarder] XReadGroup error: %v", err)
				}
				continue
			}

			for _, stream := range entries {
				if len(stream.Messages) > 0 {
					f.processMessages(ctx, stream.Messages)
				}
			}
		}
	}
}

// startRecovery 定期恢复 pending 消息
func (f *OrderForwarder) startRecovery(ctx context.Context, wg *sync.WaitGroup) {
	defer wg.Done()

	ticker := time.NewTicker(60 * time.Second)
	defer ticker.Stop()

	f.log.Infof("[Forwarder Recovery] Started recovering pending messages every 60s")

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			pendings, err := f.rdb.XPendingExt(ctx, &redis.XPendingExtArgs{
				Stream: streamKeyOrder,
				Group:  forwarderGroup,
				Idle:   60 * time.Second,
				Start:  "-",
				End:    "+",
				Count:  50,
			}).Result()

			if err != nil || len(pendings) == 0 {
				continue
			}

			var ids []string
			for _, p := range pendings {
				ids = append(ids, p.ID)
			}

			claimedMsgs, err := f.rdb.XClaim(ctx, &redis.XClaimArgs{
				Stream:   streamKeyOrder,
				Group:    forwarderGroup,
				Consumer: f.consumer,
				MinIdle:  60 * time.Second,
				Messages: ids,
			}).Result()

			if len(claimedMsgs) > 0 {
				f.log.Infof("[Forwarder Recovery] Claimed %d pending messages", len(claimedMsgs))
				f.processMessages(ctx, claimedMsgs)
			}
		}
	}
}

// processMessages 批量转发消息到 RocketMQ
func (f *OrderForwarder) processMessages(ctx context.Context, messages []redis.XMessage) {
	var batchMsgs []*primitive.Message
	var validMsgIDs []string

	for _, msg := range messages {
		payloadStr, ok := msg.Values["payload"].(string)
		if !ok {
			f.log.Errorf("[Forwarder] Invalid message format: missing 'payload', id=%s", msg.ID)
			f.rdb.XAck(ctx, streamKeyOrder, forwarderGroup, msg.ID)
			continue
		}

		// 解析验证 JSON
		var orderData map[string]interface{}
		if err := json.Unmarshal([]byte(payloadStr), &orderData); err != nil {
			f.log.Errorf("[Forwarder] Invalid JSON payload, id=%s, err=%v", msg.ID, err)
			f.rdb.XAck(ctx, streamKeyOrder, forwarderGroup, msg.ID)
			continue
		}

		// 提取 OrderID 作为 Key
		orderID, _ := orderData["order_id"].(string)

		pMsg := primitive.NewMessage(targetTopic, []byte(payloadStr))
		pMsg.WithKeys([]string{orderID})
		pMsg.WithTag("order_created")

		// 提取并注入 Trace Context
		if traceData, ok := orderData["trace_ctx"].(map[string]interface{}); ok {
			for k, v := range traceData {
				if vStr, ok := v.(string); ok {
					pMsg.WithProperty(k, vStr)
				}
			}
		}

		batchMsgs = append(batchMsgs, pMsg)
		validMsgIDs = append(validMsgIDs, msg.ID)

		if parts := strings.SplitN(msg.ID, "-", 2); len(parts) == 2 {
			if tsMs, err := strconv.ParseInt(parts[0], 10, 64); err == nil {
				lag := time.Now().UnixMilli() - tsMs
				atomic.StoreInt64(&f.currentLagMs, lag)
			}
		}
	}

	if len(batchMsgs) == 0 {
		return
	}

	// 批量发送 (熔断器保护)
	result, err := f.cb.Execute(func() (interface{}, error) {
		return f.producer.SendSync(ctx, batchMsgs...)
	})

	if err != nil {
		if err == gobreaker.ErrOpenState {
			f.log.Warnf("[Forwarder] RocketMQ send rejected by circuit breaker")
		} else {
			f.log.Errorf("[Forwarder] Batch send failed (will retry via recovery): %v", err)
		}
		return
	}

	res := result.(*primitive.SendResult)
	if res.Status == primitive.SendOK {
		// 批量 ACK
		f.rdb.XAck(ctx, streamKeyOrder, forwarderGroup, validMsgIDs...)
		f.log.Debugf("[Forwarder] Forwarded %d messages to RocketMQ", len(validMsgIDs))
	} else {
		f.log.Warnf("[Forwarder] RocketMQ batch send status not OK: %v", res.Status)
	}
}
