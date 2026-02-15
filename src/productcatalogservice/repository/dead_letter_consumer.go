package repository

import (
	"context"
	"fmt"
	"os"
	"strconv"
	"sync"
	"time"

	redis "github.com/redis/go-redis/v9"
	"github.com/sirupsen/logrus"
	"gorm.io/gorm"
)

const (
	deadLetterGroup    = "group_dead_letter_persister"
	deadLetterConsumer = "dead-letter-consumer"
)

// DeadMessage GORM 模型，对应 MySQL dead_messages 表
type DeadMessage struct {
	ID             int64     `gorm:"primaryKey;autoIncrement"`
	MsgID          string    `gorm:"column:msg_id;type:varchar(64);not null;index:idx_msg_id"`
	OriginalStream string    `gorm:"column:original_stream;type:varchar(128);not null"`
	ConsumerGroup  string    `gorm:"column:consumer_group;type:varchar(128);not null"`
	Payload        string    `gorm:"column:payload;type:json"`
	ErrorReason    string    `gorm:"column:error_reason;type:text"`
	CreatedAt      time.Time `gorm:"column:created_at;autoCreateTime"`
}

func (DeadMessage) TableName() string {
	return "dead_messages"
}

// DeadLetterConsumer 死信队列消费者，从 Redis Dead Stream 拉取消息并持久化到 MySQL
type DeadLetterConsumer struct {
	rdb *redis.Client
	db  *gorm.DB
	log *logrus.Logger
}

// NewDeadLetterConsumer 构造死信消费者
func NewDeadLetterConsumer(rdb *redis.Client, db *gorm.DB, log *logrus.Logger) *DeadLetterConsumer {
	return &DeadLetterConsumer{rdb: rdb, db: db, log: log}
}

// Start 启动死信消费者（确保消费者组 + AutoMigrate + 启动消费协程）
func (c *DeadLetterConsumer) Start(ctx context.Context, wg *sync.WaitGroup) {
	// 自动建表
	if err := c.db.AutoMigrate(&DeadMessage{}); err != nil {
		c.log.Errorf("[DeadLetterConsumer] AutoMigrate failed: %v", err)
		return
	}

	// 创建消费者组（幂等）
	hostname, _ := os.Hostname()
	consumerName := fmt.Sprintf("%s-%s", deadLetterConsumer, hostname)
	c.rdb.XGroupCreateMkStream(ctx, DeadStreamKey, deadLetterGroup, "0")

	wg.Add(2)
	go c.consume(ctx, wg, consumerName)
	go c.startRecovery(ctx, wg, consumerName)
}

// consume 核心消费循环
func (c *DeadLetterConsumer) consume(ctx context.Context, wg *sync.WaitGroup, consumerName string) {
	defer wg.Done()
	c.log.Info("[DeadLetterConsumer] Started consuming dead stream")

	for {
		select {
		case <-ctx.Done():
			c.log.Info("[DeadLetterConsumer] Shutting down...")
			return
		default:
			entries, err := c.rdb.XReadGroup(ctx, &redis.XReadGroupArgs{
				Group:    deadLetterGroup,
				Consumer: consumerName,
				Streams:  []string{DeadStreamKey, ">"},
				Count:    10,
				Block:    5 * time.Second,
			}).Result()

			if err != nil {
				if err != redis.Nil {
					c.log.Errorf("[DeadLetterConsumer] XReadGroup error: %v", err)
				}
				continue
			}

			for _, stream := range entries {
				if len(stream.Messages) > 0 {
					c.persistMessages(ctx, stream.Messages)
				}
			}
		}
	}
}

// startRecovery 定期恢复 pending 消息
func (c *DeadLetterConsumer) startRecovery(ctx context.Context, wg *sync.WaitGroup, consumerName string) {
	defer wg.Done()

	ticker := time.NewTicker(60 * time.Second)
	defer ticker.Stop()

	c.log.Infof("[DeadLetterConsumer Recovery] Started recovering pending messages every 60s")

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			// 1. 查找空闲超过 60 秒的 pending 消息
			pendings, err := c.rdb.XPendingExt(ctx, &redis.XPendingExtArgs{
				Stream: DeadStreamKey,
				Group:  deadLetterGroup,
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

			// 2. 尝试 Claim 这些消息
			claimedMsgs, err := c.rdb.XClaim(ctx, &redis.XClaimArgs{
				Stream:   DeadStreamKey,
				Group:    deadLetterGroup,
				Consumer: consumerName,
				MinIdle:  60 * time.Second,
				Messages: ids,
			}).Result()

			if err != nil {
				c.log.Errorf("[DeadLetterConsumer Recovery] XClaim error: %v", err)
				continue
			}

			if len(claimedMsgs) > 0 {
				c.log.Infof("[DeadLetterConsumer Recovery] Claimed %d pending messages", len(claimedMsgs))
				c.persistMessages(ctx, claimedMsgs)
			}
		}
	}
}

// persistMessages 解析并批量写入 MySQL，成功后 ACK
func (c *DeadLetterConsumer) persistMessages(ctx context.Context, messages []redis.XMessage) {
	records := make([]DeadMessage, 0, len(messages))
	var ackIDs []string

	for _, msg := range messages {
		record := DeadMessage{
			MsgID:          getString(msg.Values, "msg_id"),
			OriginalStream: getString(msg.Values, "original_stream"),
			ConsumerGroup:  getString(msg.Values, "consumer_group"),
			Payload:        getString(msg.Values, "payload"),
			ErrorReason:    getString(msg.Values, "error_reason"),
		}

		// 解析 created_at（Unix Milliseconds -> time.Time）
		if createdAtStr := getString(msg.Values, "created_at"); createdAtStr != "" {
			if ms, err := strconv.ParseInt(createdAtStr, 10, 64); err == nil {
				record.CreatedAt = time.UnixMilli(ms)
			}
		}

		records = append(records, record)
		ackIDs = append(ackIDs, msg.ID)
	}

	if len(records) == 0 {
		return
	}

	// 批量插入 MySQL
	if err := c.db.WithContext(ctx).Create(&records).Error; err != nil {
		c.log.Errorf("[DeadLetterConsumer] Failed to persist %d dead messages: %v", len(records), err)
		// 不 ACK，等待下次重试
		return
	}

	// 持久化成功 -> ACK
	if err := c.rdb.XAck(ctx, DeadStreamKey, deadLetterGroup, ackIDs...).Err(); err != nil {
		c.log.Errorf("[DeadLetterConsumer] Failed to ACK %d messages: %v", len(ackIDs), err)
		return
	}

	c.log.Infof("[DeadLetterConsumer] Persisted and ACKed %d dead messages", len(records))
}

// getString 安全地从 Redis Stream Values 中提取字符串
func getString(values map[string]interface{}, key string) string {
	if v, ok := values[key]; ok {
		if s, ok := v.(string); ok {
			return s
		}
	}
	return ""
}
