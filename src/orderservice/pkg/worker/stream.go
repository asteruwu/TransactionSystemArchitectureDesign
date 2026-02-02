package worker

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"sync"
	"time"

	"github.com/GoogleCloudPlatform/microservices-demo/src/orderservice/pkg/model"

	rocketmq "github.com/apache/rocketmq-client-go/v2"
	"github.com/apache/rocketmq-client-go/v2/primitive"
	redis "github.com/redis/go-redis/v9"
	"github.com/sirupsen/logrus"
)

type StreamWorker struct {
	rdb       *redis.Client
	producer  rocketmq.Producer
	logger    *logrus.Logger
	streamKey string
	group     string
	consumer  string
	topic     string
}

// 构造 StreamWorker
func NewStreamWorker(rdb *redis.Client, producer rocketmq.Producer, log *logrus.Logger) *StreamWorker {
	hostname, _ := os.Hostname()
	if hostname == "" {
		hostname = "pod-unknown"
	}
	return &StreamWorker{
		rdb:       rdb,
		producer:  producer,
		logger:    log,
		streamKey: "mq:order:create",
		group:     "group_order_forwarder",
		consumer:  hostname,
		topic:     "orders",
	}
}

func (s *StreamWorker) Start(ctx context.Context, wg *sync.WaitGroup) {
	// 确保消费者组存在
	s.rdb.XGroupCreateMkStream(ctx, s.streamKey, s.group, "0")

	wg.Add(2)
	go s.startRedisStreamConsumer(ctx, wg)
	go s.startRecoveryStream(ctx, wg)
}

// 开始 Redis Stream 消费
func (s *StreamWorker) startRedisStreamConsumer(ctx context.Context, wg *sync.WaitGroup) {
	defer wg.Done()
	for {
		select {
		case <-ctx.Done():
			return
		default:
			// 获得流中的所有数据
			entries, err := s.rdb.XReadGroup(ctx, &redis.XReadGroupArgs{
				Group:    s.group,
				Consumer: s.consumer,
				Streams:  []string{s.streamKey, ">"},
				Count:    10,
				Block:    2 * time.Second,
			}).Result()

			if err != nil {
				continue
			}

			// 处理stream消息
			for _, stream := range entries {
				s.processMessages(ctx, stream.Messages)
			}
		}
	}
}

// 开启 redis stream 补偿
func (s *StreamWorker) startRecoveryStream(ctx context.Context, wg *sync.WaitGroup) {
	defer wg.Done()
	ticker := time.NewTicker(1 * time.Minute)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			// 获取 pending 消息
			pendings, err := s.rdb.XPendingExt(ctx, &redis.XPendingExtArgs{
				Stream: s.streamKey,
				Group:  s.group,
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

			// 认领 pending 消息
			claimedMsgs, err := s.rdb.XClaim(ctx, &redis.XClaimArgs{
				Stream:   s.streamKey,
				Group:    s.group,
				Consumer: s.consumer,
				MinIdle:  60 * time.Second,
				Messages: ids,
			}).Result()

			// 处理 pending 消息
			if len(claimedMsgs) > 0 {
				s.processMessages(ctx, claimedMsgs)
			}
		}
	}
}

func (s *StreamWorker) processMessages(ctx context.Context, messages []redis.XMessage) {
	var batchMsgs []*primitive.Message
	var validMsgIDs []string

	for _, msg := range messages {
		// --- 步骤 1: 解析 Redis 消息 (Payload 模式) ---
		payloadStr, ok := msg.Values["payload"].(string)

		if !ok {
			s.logger.Errorf("Invalid message format: missing 'payload' field, id=%s. Skipping.", msg.ID)
			s.sendToDLQ(ctx, msg, "missing_payload_field")
			s.rdb.XAck(ctx, s.streamKey, s.group, msg.ID)
			continue
		}

		// 反序列化校验 (同时也是为了获取 OrderID 作为 RocketMQ Key)
		var orderData model.OrderMessage
		if err := json.Unmarshal([]byte(payloadStr), &orderData); err != nil {
			s.logger.Errorf("Payload is not valid JSON, id=%s, err=%v. Payload: %s", msg.ID, err, payloadStr)
			s.sendToDLQ(ctx, msg, fmt.Sprintf("invalid_json_payload: %v", err))
			s.rdb.XAck(ctx, s.streamKey, s.group, msg.ID)
			continue
		}

		// --- 步骤 2: 收集到 Batch ---
		pMsg := primitive.NewMessage(s.topic, []byte(payloadStr))
		pMsg.WithKeys([]string{orderData.OrderID})
		pMsg.WithTag("order_created")

		batchMsgs = append(batchMsgs, pMsg)
		validMsgIDs = append(validMsgIDs, msg.ID)
	}

	// --- 步骤 3: 批量发送 ---
	if len(batchMsgs) > 0 {
		res, err := s.producer.SendSync(ctx, batchMsgs...)

		if err == nil {
			if res.Status != primitive.SendOK {
				s.logger.Warnf("RocketMQ batch send status not OK: %v", res.Status)
			} else {
				// 发送成功，批量 ACK 所有 ID
				s.rdb.XAck(ctx, s.streamKey, s.group, validMsgIDs...)
			}
		} else {
			s.logger.Errorf("Failed to batch send rocketmq (will retry via recovery): %v", err)
			// 全部不 ACK，依靠 startRecoveryStream 稍后捞起重试
		}
	}
}

// 发送消息到死信队列
func (s *StreamWorker) sendToDLQ(ctx context.Context, msg redis.XMessage, reason string) {
	bodyBytes, _ := json.Marshal(msg.Values)

	dlqMsg := primitive.NewMessage("orders_dlq", bodyBytes)
	dlqMsg.WithProperty("original_msg_id", msg.ID)
	dlqMsg.WithProperty("dlq_reason", reason)
	dlqMsg.WithProperty("source", "redis_stream_worker")

	_, err := s.producer.SendSync(ctx, dlqMsg)
	if err != nil {
		s.logger.Errorf("CRITICAL: Failed to send corrupted stream msg to DLQ! MsgID: %s, Error: %v", msg.ID, err)
	} else {
		s.logger.Infof("Sent corrupted stream msg to DLQ. MsgID: %s, Reason: %s", msg.ID, reason)
	}
}
