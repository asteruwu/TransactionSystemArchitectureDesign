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

	"github.com/GoogleCloudPlatform/microservices-demo/src/productcatalogservice/repository"
	rmq_client "github.com/apache/rocketmq-clients/golang/v5"
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
	producer     rmq_client.Producer
	log          *logrus.Logger
	cb           *gobreaker.CircuitBreaker
	dlp          *repository.DeadLetterProducer // 死信队列生产者
	consumer     string
	currentLagMs int64

	sendSuccessTotal    uint64
	transientFailTotal  uint64
	permanentFailTotal  uint64
	dlqTotal            uint64
	fallbackCBOpenTotal uint64
	cbState             int32 // 熔断器状态（0=closed, 1=half-open, 2=open）
}

// NewOrderForwarder 构造 Forwarder Worker
func NewOrderForwarder(rdb *redis.Client, producer rmq_client.Producer, log *logrus.Logger, dlp *repository.DeadLetterProducer) *OrderForwarder {
	hostname, _ := os.Hostname()
	if hostname == "" {
		hostname = "pod-unknown"
	}

	f := &OrderForwarder{
		rdb:      rdb,
		producer: producer,
		log:      log,
		dlp:      dlp,
		consumer: fmt.Sprintf("forwarder-%s", hostname),
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
			stateMap := map[gobreaker.State]int32{
				gobreaker.StateClosed:   0,
				gobreaker.StateHalfOpen: 1,
				gobreaker.StateOpen:     2,
			}
			atomic.StoreInt32(&f.cbState, stateMap[to])
		},
	}
	f.cb = gobreaker.NewCircuitBreaker(st)

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

	// 2. 发送到 RocketMQ 的消息总数（按 result 区分）
	meter.Int64ObservableGauge("app_forwarder_send_total",
		metric.WithDescription("Total messages sent to RocketMQ"),
		metric.WithInt64Callback(func(_ context.Context, obs metric.Int64Observer) error {
			obs.Observe(int64(atomic.LoadUint64(&f.sendSuccessTotal)),
				metric.WithAttributes(attribute.String("result", "success")))
			obs.Observe(int64(atomic.LoadUint64(&f.transientFailTotal)),
				metric.WithAttributes(attribute.String("result", "transient_fail")))
			obs.Observe(int64(atomic.LoadUint64(&f.permanentFailTotal)),
				metric.WithAttributes(attribute.String("result", "permanent_fail")))
			return nil
		}),
	)

	// 3. 写入死信队列的消息数
	meter.Int64ObservableGauge("app_forwarder_dlq_total",
		metric.WithDescription("Total messages sent to dead letter queue"),
		metric.WithInt64Callback(func(_ context.Context, obs metric.Int64Observer) error {
			obs.Observe(int64(atomic.LoadUint64(&f.dlqTotal)))
			return nil
		}),
	)

	// 4. 熔断器拒绝发送的触发次数
	meter.Int64ObservableGauge("app_forwarder_cb_reject_total",
		metric.WithDescription("Total times send was rejected by circuit breaker"),
		metric.WithInt64Callback(func(_ context.Context, obs metric.Int64Observer) error {
			obs.Observe(int64(atomic.LoadUint64(&f.fallbackCBOpenTotal)),
				metric.WithAttributes(attribute.String("reason", "cb_open")))
			return nil
		}),
	)

	// 5. 熔断器当前状态（0=closed, 1=half-open, 2=open）
	meter.Int64ObservableGauge("app_forwarder_cb_state",
		metric.WithDescription("Circuit breaker state: 0=closed, 1=half-open, 2=open"),
		metric.WithInt64Callback(func(_ context.Context, obs metric.Int64Observer) error {
			obs.Observe(int64(atomic.LoadInt32(&f.cbState)),
				metric.WithAttributes(attribute.String("name", "RocketMQ-Forwarder")))
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
	var batchMsgs []*rmq_client.Message
	var validMsgIDs []string

	for _, msg := range messages {
		payloadStr, ok := msg.Values["payload"].(string)
		if !ok {
			f.log.Errorf("[Forwarder] Invalid message format: missing 'payload', id=%s", msg.ID)
			// 永久性解析错误 -> 写入死信队列
			if err := f.dlp.SendToDeadLetter(ctx, streamKeyOrder, forwarderGroup, msg.ID, "", "payload field missing or invalid type"); err == nil {
				atomic.AddUint64(&f.dlqTotal, 1)
			}
			f.rdb.XAck(ctx, streamKeyOrder, forwarderGroup, msg.ID)
			continue
		}

		// 解析验证 JSON
		var orderData map[string]interface{}
		if err := json.Unmarshal([]byte(payloadStr), &orderData); err != nil {
			f.log.Errorf("[Forwarder] Invalid JSON payload, id=%s, err=%v", msg.ID, err)
			// 永久性解析错误 -> 写入死信队列（保留原始 payload）
			if dlqErr := f.dlp.SendToDeadLetter(ctx, streamKeyOrder, forwarderGroup, msg.ID, payloadStr, fmt.Sprintf("json unmarshal failed: %v", err)); dlqErr == nil {
				atomic.AddUint64(&f.dlqTotal, 1)
			}
			f.rdb.XAck(ctx, streamKeyOrder, forwarderGroup, msg.ID)
			continue
		}

		// 提取 OrderID 作为 Key
		orderID, _ := orderData["order_id"].(string)

		pMsg := &rmq_client.Message{
			Topic: targetTopic,
			Body:  []byte(payloadStr),
		}
		pMsg.SetKeys(orderID)
		pMsg.SetTag("order_created")

		// 提取并注入 Trace Context
		if traceData, ok := orderData["trace_ctx"].(map[string]interface{}); ok {
			for k, v := range traceData {
				if vStr, ok := v.(string); ok {
					pMsg.AddProperty(k, vStr)
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
	// 5.x SDK 不支持原生批量发送，通过熔断器保护并发逐条发送
	_, err := f.cb.Execute(func() (interface{}, error) {
		return nil, f.sendBatch(ctx, batchMsgs, validMsgIDs)
	})

	if err != nil {
		if err == gobreaker.ErrOpenState {
			f.log.Warnf("[Forwarder] RocketMQ send rejected by circuit breaker, skipping fallback")
			atomic.AddUint64(&f.fallbackCBOpenTotal, 1)
			return
		}
		// sendBatch 内部已处理逐条降级，熔断器感知到错误时记录指标
		f.log.Warnf("[Forwarder] Batch send encountered errors: %v", err)
	}
}

// sendBatch 通过逐条发送模拟批量，统一走熔断器保护
func (f *OrderForwarder) sendBatch(ctx context.Context, msgs []*rmq_client.Message, msgIDs []string) error {
	var firstErr error
	var ackIDs []string

	for i, msg := range msgs {
		_, err := f.producer.Send(ctx, msg)
		if err != nil {
			if isTransientMQError(err) {
				f.log.Warnf("[Forwarder] Transient error for msg (streamID=%s): %v. Will retry via recovery.", msgIDs[i], err)
				atomic.AddUint64(&f.transientFailTotal, 1)
				if firstErr == nil {
					firstErr = err
				}
			} else {
				f.log.Errorf("[Forwarder] Permanent error for msg (streamID=%s): %v. Sending to dead stream.", msgIDs[i], err)
				atomic.AddUint64(&f.permanentFailTotal, 1)
				if dlqErr := f.dlp.SendToDeadLetter(ctx, streamKeyOrder, forwarderGroup, msgIDs[i], string(msg.Body), fmt.Sprintf("permanent MQ error: %v", err)); dlqErr == nil {
					atomic.AddUint64(&f.dlqTotal, 1)
				}
				ackIDs = append(ackIDs, msgIDs[i])
			}
			continue
		}

		atomic.AddUint64(&f.sendSuccessTotal, 1)
		ackIDs = append(ackIDs, msgIDs[i])
	}

	if len(ackIDs) > 0 {
		f.rdb.XAck(ctx, streamKeyOrder, forwarderGroup, ackIDs...)
		f.log.Debugf("[Forwarder] ACKed %d/%d messages", len(ackIDs), len(msgs))
	}

	return firstErr
}

// isTransientMQError 判断 RocketMQ 错误是否为临时性故障（可重试）
func isTransientMQError(err error) bool {
	if err == nil {
		return false
	}

	errMsg := strings.ToLower(err.Error())
	transientKeywords := []string{
		"timeout", "deadline exceeded", "connection refused", "connect",
		"system busy", "service not available", "broken pipe", "reset by peer",
		"unavailable", "internal",
	}
	for _, kw := range transientKeywords {
		if strings.Contains(errMsg, kw) {
			return true
		}
	}

	return false
}
