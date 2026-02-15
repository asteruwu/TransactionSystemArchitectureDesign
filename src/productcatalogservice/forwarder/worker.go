package forwarder

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	rocketmq "github.com/apache/rocketmq-client-go/v2"
	mqerrors "github.com/apache/rocketmq-client-go/v2/errors"
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
			f.log.Warnf("[Forwarder] RocketMQ send rejected by circuit breaker, skipping fallback")
			return
		}
		// 批量发送失败 -> 降级为逐条发送
		f.log.Warnf("[Forwarder] Batch send failed (%v), falling back to sequential send", err)
		f.sequentialSend(ctx, batchMsgs, validMsgIDs)
		return
	}

	res := result.(*primitive.SendResult)
	if res.Status == primitive.SendOK {
		// 批量 ACK
		f.rdb.XAck(ctx, streamKeyOrder, forwarderGroup, validMsgIDs...)
		f.log.Debugf("[Forwarder] Forwarded %d messages to RocketMQ", len(validMsgIDs))
	} else {
		// 状态非 OK -> 降级为逐条发送
		f.log.Warnf("[Forwarder] RocketMQ batch send status not OK (%v), falling back to sequential send", res.Status)
		f.sequentialSend(ctx, batchMsgs, validMsgIDs)
	}
}

// sequentialSend 逐条降级发送：对每条消息单独调用 SendSync
func (f *OrderForwarder) sequentialSend(ctx context.Context, msgs []*primitive.Message, msgIDs []string) {
	var ackIDs []string
	var retryMsgIDs []string

	for i, msg := range msgs {
		res, err := f.producer.SendSync(ctx, msg)
		if err != nil {
			if isTransientMQError(err) {
				// 临时故障（超时、连接断开、Broker 不可用等）：不 ACK，等待 Recovery 重试
				f.log.Warnf("[Forwarder Sequential] Transient error for msg (streamID=%s): %v. Will retry via recovery.", msgIDs[i], err)
				retryMsgIDs = append(retryMsgIDs, msgIDs[i])
			} else {
				// 永久故障（Topic 不存在、消息体非法等）：ACK 防止阻塞
				f.log.Errorf("[Forwarder Sequential] Permanent error for msg (streamID=%s): %v. ACKing to prevent blocking.", msgIDs[i], err)
				ackIDs = append(ackIDs, msgIDs[i])
			}
			continue
		}

		if res.Status == primitive.SendOK {
			ackIDs = append(ackIDs, msgIDs[i])
		} else if isTransientSendStatus(res.Status) {
			// FlushDiskTimeout / FlushSlaveTimeout / SlaveNotAvailable：属于临时故障，不 ACK
			f.log.Warnf("[Forwarder Sequential] Transient send status for msg (streamID=%s): %v. Will retry via recovery.", msgIDs[i], res.Status)
			retryMsgIDs = append(retryMsgIDs, msgIDs[i])
		} else {
			// 未知状态：ACK 防止阻塞
			f.log.Errorf("[Forwarder Sequential] Unknown send status for msg (streamID=%s): %v. ACKing to prevent blocking.", msgIDs[i], res.Status)
			ackIDs = append(ackIDs, msgIDs[i])
		}
	}

	if len(retryMsgIDs) > 0 {
		f.log.Warnf("[Forwarder Sequential] %d messages skipped ACK due to transient errors (will retry via recovery)", len(retryMsgIDs))
	}

	if len(ackIDs) > 0 {
		f.rdb.XAck(ctx, streamKeyOrder, forwarderGroup, ackIDs...)
		f.log.Infof("[Forwarder Sequential] Processed %d/%d messages via sequential fallback", len(ackIDs), len(msgs))
	}
}

// isTransientMQError 判断 RocketMQ 错误是否为临时性故障（可重试）
func isTransientMQError(err error) bool {
	if err == nil {
		return false
	}

	// 1. 检查 RocketMQ Client 预定义的临时性错误
	if errors.Is(err, mqerrors.ErrRequestTimeout) {
		return true
	}
	if errors.Is(err, mqerrors.ErrBrokerNotFound) {
		return true
	}
	if errors.Is(err, mqerrors.ErrService) {
		return true
	}

	// 2. 检查 RocketMQ Remoting 层网络错误
	if primitive.IsRemotingErr(err) {
		return true
	}

	// 3. 检查 Broker 返回的临时性响应码
	var brokerErr primitive.MQBrokerErr
	if errors.As(err, &brokerErr) {
		switch brokerErr.ResponseCode {
		case 10:
			return true
		case 11:
			return true
		case 12:
			return true
		case 14:
			return true
		}
		return false
	}

	// 4. 检查底层网络错误（与 isTransientDBError 一致）
	var netErr net.Error
	if errors.As(err, &netErr) {
		return true
	}

	// 5. 检查 context 超时/取消（graceful shutdown 场景）
	if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) {
		return true
	}

	// 6. 基于错误消息字符串的兜底匹配（应对未导出的错误类型）
	errMsg := strings.ToLower(err.Error())
	transientKeywords := []string{"timeout", "connection refused", "connect", "system busy", "service not available", "broken pipe", "reset by peer"}
	for _, kw := range transientKeywords {
		if strings.Contains(errMsg, kw) {
			return true
		}
	}

	return false // 默认视为永久故障
}

func isTransientSendStatus(status primitive.SendStatus) bool {
	switch status {
	case primitive.SendFlushDiskTimeout,
		primitive.SendFlushSlaveTimeout,
		primitive.SendSlaveNotAvailable:
		return true
	default:
		return false
	}
}
