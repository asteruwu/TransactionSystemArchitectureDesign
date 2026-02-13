package repository

import (
	"context"
	"encoding/json"
	"fmt"
	"math/rand"
	"os"
	"sync"
	"sync/atomic"
	"time"

	pb "github.com/GoogleCloudPlatform/microservices-demo/src/productcatalogservice/genproto"
	"github.com/GoogleCloudPlatform/microservices-demo/src/productcatalogservice/model"
	redis "github.com/redis/go-redis/v9"
	"github.com/sirupsen/logrus"
	"github.com/sony/gobreaker"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"
	"golang.org/x/sync/singleflight"
	"gorm.io/gorm"
)

const (
	streamKey      = "mq:stock:sync"   // Stock Stream (Short Payload)
	streamKeyOrder = "mq:order:create" // Order Stream (Full Payload)
	streamGroup    = "group_stock_flusher"
)

type cachedRepo struct {
	next          ProductRepository
	rdb           *redis.Client
	db            *gorm.DB
	sf            singleflight.Group
	log           *logrus.Logger
	cb            *gobreaker.CircuitBreaker
	deductScript  *redis.Script
	restockScript *redis.Script
	consumerName  string
	meter         metric.Meter

	lastFlushUnixNano int64
	flushSuccessTotal uint64
	flushFailTotal    uint64

	recoverSuccessTotal uint64
	recoverFailTotal    uint64
	recoverSkipTotal    uint64

	ACKFailTotal uint64

	stockChargeSuccessTotal uint64
}

type StockDedupLog struct {
	MsgID     string `gorm:"primaryKey;column:msg_id"`
	ProductID string `gorm:"column:product_id"`
	CreatedAt time.Time
}

const LuaDeductStock = `
	-- KEYS: [dedupKey, stockKey1, stockKey2, ..., stockStreamKey, orderStreamKey]
	-- ARGV: [amount1, amount2, ..., stockPayload, orderPayload]

	local dedupKey = KEYS[1]
	local keyCount = #KEYS
	local orderStreamKey = KEYS[keyCount]
	local stockStreamKey = KEYS[keyCount-1]
	local itemCount = keyCount - 3

	-- 0. 幂等性检查 (Idempotency Check)
	if redis.call('exists', dedupKey) == 1 then
		return 2 -- 已处理过，直接返回成功
	end

	-- 1. 确认库存是否充足
	for i = 1, itemCount do
		local stockKey = KEYS[i+1]
		local amount = tonumber(ARGV[i])
		local current = tonumber(redis.call('get', stockKey))

		if current == nil then
			return -1 -- 库存未初始化
		end
		if current < amount then
			return 0 -- 库存不足
		end
	end

	-- 2. 扣减库存
	for i = 1, itemCount do
		redis.call('decrby', KEYS[i+1], ARGV[i])
	end

	-- 3. 写入去重记录 (TTL 1小时)
	redis.call('setex', dedupKey, 3600, '1')

	-- 4. 发送消息
	local stockPayload = ARGV[itemCount+1]
	local orderPayload = ARGV[itemCount+2]

	-- 4.1 发送库存流消息
	redis.call('xadd', stockStreamKey, '*', 'payload', stockPayload)

	-- 4.2 发送订单流消息
	redis.call('xadd', orderStreamKey, '*', 'payload', orderPayload)

	return 1 -- 成功
`

const LuaRestock = `
	-- KEYS: [rollbackKey, stockStreamKey, stockKey1, stockKey2, ...]
	-- ARGV: [stockPayload, amount1, amount2, ...]

	local rollbackKey = KEYS[1]
	local stockStreamKey = KEYS[2]
	
	-- 1. 幂等性检查
	if redis.call('setnx', rollbackKey, '1') == 0 then
		return 1 -- 已经回滚过
	end
	redis.call('expire', rollbackKey, 86400) -- TTL 24h

	local itemCount = #KEYS - 2
	
	-- 2. 恢复库存
	for i = 1, itemCount do
		local stockKey = KEYS[2+i]
		local amount = tonumber(ARGV[1+i])
		redis.call('incrby', stockKey, amount)
	end
	
	-- 3. 发送库存流消息
	local payload = ARGV[1]
	redis.call('xadd', stockStreamKey, '*', 'payload', payload)

	return 1
`

func (StockDedupLog) TableName() string {
	return "stock_dedup_log"
}

func NewCachedRepo(next ProductRepository, rdb *redis.Client, db *gorm.DB, log *logrus.Logger, ctx context.Context, wg *sync.WaitGroup) ProductRepository {
	script := redis.NewScript(LuaDeductStock)
	restockScript := redis.NewScript(LuaRestock)

	st := gobreaker.Settings{
		Name:        "RedisCircuitBreaker",
		MaxRequests: 1,
		Interval:    10 * time.Second,
		Timeout:     30 * time.Second,

		// 触发熔断的条件
		ReadyToTrip: func(counts gobreaker.Counts) bool {
			failureRatio := float64(counts.TotalFailures) / float64(counts.Requests)
			return counts.Requests >= 5 && failureRatio >= 0.5
		},

		// 状态变更回调（用于打日志监控）
		OnStateChange: func(name string, from gobreaker.State, to gobreaker.State) {
			log.Warnf("CircuitBreaker state changed from %s to %s", from, to)
		},
	}

	host_name, _ := os.Hostname()
	consumerName := fmt.Sprintf("pod-%s-%d", host_name, rand.Intn(1000))

	s := &cachedRepo{
		next:          next,
		rdb:           rdb,
		db:            db,
		log:           log,
		cb:            gobreaker.NewCircuitBreaker(st),
		deductScript:  script,
		restockScript: restockScript,
		consumerName:  consumerName,
		meter:         otel.GetMeterProvider().Meter("productcatalogservice.repo"),
	}
	s.registerMetrics()

	err := rdb.XGroupCreateMkStream(ctx, streamKey, streamGroup, "0").Err()
	if err != nil {
		log.Warnf("failed to create stream: %v", err)
	}

	wg.Add(1)
	go s.startStreamWorker(ctx, wg)

	wg.Add(1)
	go s.startPendingRecover(ctx, wg)

	return s
}

func (c *cachedRepo) registerMetrics() {
	// 1. MySQL落库成功次数
	_, err := c.meter.Int64ObservableGauge(
		"repo_flush_success_total",
		metric.WithUnit("{items}"),
		metric.WithInt64Callback(func(ctx context.Context, obs metric.Int64Observer) error {
			obs.Observe(int64(atomic.LoadUint64(&c.flushSuccessTotal)))
			return nil
		}),
	)

	// 2. MySQL落库失败次数
	_, err = c.meter.Int64ObservableGauge(
		"repo_flush_fail_total",
		metric.WithUnit("{items}"),
		metric.WithInt64Callback(func(ctx context.Context, obs metric.Int64Observer) error {
			obs.Observe(int64(atomic.LoadUint64(&c.flushFailTotal)))
			return nil
		}),
	)

	// 3. ACK失败次数
	_, err = c.meter.Int64ObservableGauge(
		"repo_ack_fail_total",
		metric.WithUnit("{items}"),
		metric.WithInt64Callback(func(ctx context.Context, obs metric.Int64Observer) error {
			obs.Observe(int64(atomic.LoadUint64(&c.ACKFailTotal)))
			return nil
		}),
	)

	// 4. 恢复失败次数
	_, err = c.meter.Int64ObservableGauge(
		"repo_recover_fail_total",
		metric.WithUnit("{items}"),
		metric.WithInt64Callback(func(ctx context.Context, obs metric.Int64Observer) error {
			obs.Observe(int64(atomic.LoadUint64(&c.recoverFailTotal)))
			return nil
		}),
	)

	// 5. 恢复成功次数
	_, err = c.meter.Int64ObservableGauge(
		"repo_recover_success_total",
		metric.WithUnit("{items}"),
		metric.WithInt64Callback(func(ctx context.Context, obs metric.Int64Observer) error {
			obs.Observe(int64(atomic.LoadUint64(&c.recoverSuccessTotal)))
			return nil
		}),
	)

	if err != nil {
		c.log.Warnf("failed to register metrics: %v", err)
	}

	_, err = c.meter.Int64ObservableGauge(
		"app_stock_charge_success_total",
		metric.WithUnit("{ops}"),
		metric.WithInt64Callback(func(ctx context.Context, obs metric.Int64Observer) error {
			obs.Observe(int64(atomic.LoadUint64(&c.stockChargeSuccessTotal)))
			return nil
		}),
	)
	if err != nil {
		c.log.Warnf("failed to register stock charge metric: %v", err)
	}
}

func (c *cachedRepo) GetProduct(ctx context.Context, id string) (*model.Product, error) {
	key := fmt.Sprintf("product:%s", id)

	// 熔断器
	val, err := c.cb.Execute(func() (interface{}, error) {
		// 执行redis查询
		res, err := c.rdb.Get(ctx, key).Result()

		if err == redis.Nil {
			return nil, nil
		}
		if err != nil {
			return nil, err
		}
		return res, nil
	})

	// 如果熔断器开启，则直接返回错误，否则返回redis异常
	if err != nil {
		c.log.Errorf("[GetProduct] Circuit breaker open or Redis error: %v.", err)
		return nil, fmt.Errorf("system busy: cache unavailable")
	}

	// 如果命中缓存，则直接返回数据
	if val != nil {
		var product model.Product
		if err := json.Unmarshal([]byte(val.(string)), &product); err == nil {
			return &product, nil
		}
		c.log.Errorf("[GetProduct] failed to unmarshal product %s from redis: %v", key, err)
	}

	// 未命中缓存，聚合相同的读请求，回写redis
	result, err, shared := c.sf.Do(key, func() (interface{}, error) {
		product, err := c.next.GetProduct(ctx, id)
		if err != nil {
			c.log.Errorf("[GetProduct] failed to get product %s from mysql: %v", id, err)
			return nil, err
		}

		data, _ := json.Marshal(product)
		ttl := 10*time.Minute + time.Duration(rand.Intn(60))*time.Second
		if err := c.rdb.Set(ctx, key, string(data), ttl).Err(); err != nil {
			c.log.Errorf("[GetProduct]failed to write cache for redis key %s: %v", key, err)
		}

		return product, nil
	})
	if err != nil {
		return nil, err
	}
	if shared {
		c.log.Infof("shared cache for id: %s", id)
	}
	// 返回库存
	return result.(*model.Product), nil
}

func (c *cachedRepo) ListProducts(ctx context.Context) ([]*model.Product, error) {
	return c.next.ListProducts(ctx)
}

func (c *cachedRepo) SearchProducts(ctx context.Context, query string) ([]*model.Product, error) {
	return c.next.SearchProducts(ctx, query)
}

func (c *cachedRepo) ChargeProduct(ctx context.Context, req *pb.ChargeProductRequest) (bool, string) {
	count := len(req.Items)
	keys := make([]string, 0, count+3) // +3 for dedupKey, stockStreamKey, orderStreamKey
	args := make([]interface{}, 0, count+2)

	// 0. 幂等键 (Dedup Key)
	dedupKey := fmt.Sprintf("dedup:charge:%s", req.OrderId)
	keys = append(keys, dedupKey)

	// 1. Stock Item
	type stockItem struct {
		Pid string `json:"pid"`
		Amt int32  `json:"amt"`
	}
	stockItems := make([]stockItem, 0, count)

	// 2. Order Item
	type orderItemFlat struct {
		ProductID string      `json:"product_id"`
		Quantity  int32       `json:"quantity"`
		Cost      interface{} `json:"cost"`
	}
	orderItems := make([]orderItemFlat, 0, count)

	for _, item := range req.Items {
		keys = append(keys, stockKey(item.Item.ProductId))
		args = append(args, item.Item.Quantity)

		stockItems = append(stockItems, stockItem{
			Pid: item.Item.ProductId,
			Amt: item.Item.Quantity,
		})

		orderItems = append(orderItems, orderItemFlat{
			ProductID: item.Item.ProductId,
			Quantity:  item.Item.Quantity,
			Cost:      item.Cost,
		})
	}
	keys = append(keys, streamKey, streamKeyOrder)

	// 构造 Stock Payload JSON(含order id)
	// 注入 Trace Context
	carrier := propagation.MapCarrier{}
	otel.GetTextMapPropagator().Inject(ctx, carrier)

	stockPayloadObj := map[string]interface{}{
		"oid":       req.OrderId,
		"items":     stockItems,
		"trace_ctx": carrier,
	}
	stockPayloadBytes, _ := json.Marshal(stockPayloadObj)
	args = append(args, string(stockPayloadBytes))

	// 构造 Order Payload JSON
	orderMsg := map[string]interface{}{
		"order_id":    req.OrderId,
		"user_id":     req.UserId,
		"items":       orderItems,
		"total_price": req.TotalPrice,
		"address":     req.Address,
		"email":       "",
		"status":      0,
		"created_at":  time.Now().Unix(),
		"trace_ctx":   carrier,
	}
	orderPayloadBytes, _ := json.Marshal(orderMsg)
	args = append(args, string(orderPayloadBytes))

	// 执行Lua脚本，外嵌熔断器
	execDeduct := func() (int64, error) {
		val, err := c.cb.Execute(func() (interface{}, error) {
			res, err := c.deductScript.Run(ctx, c.rdb, keys, args...).Result()

			if err != nil {
				c.log.Errorf("[ChargeBatch] Lua exec failed: %v", err)
				return nil, fmt.Errorf("redis failure: %v", err)
			}

			code, ok := res.(int64)
			if !ok {
				return nil, fmt.Errorf("unexpected lua return type")
			}

			return code, nil
		})

		if err != nil {
			return 0, err
		}
		return val.(int64), nil
	}

	// 重试3次，每次失败后尝试从数据库加载库存
	for i := 0; i < 3; i++ {
		code, err := execDeduct()
		if err != nil {
			return false, err.Error()
		}

		if code == 1 || code == 2 {
			atomic.AddUint64(&c.stockChargeSuccessTotal, 1)
			return true, "Success"
		}
		if code == 0 {
			return false, "Stock not enough"
		}
		if code == -1 {
			// 库存未初始化，尝试从数据库加载
			if i < 2 {
				if err := c.loadBatchStockFromDB(ctx, req.Items); err != nil {
					return false, fmt.Sprintf("Failed to load stock: %v", err)
				}

				// 再次尝试扣减
				code, err = execDeduct()
				if err != nil {
					return false, err.Error()
				}
				if code == 1 || code == 2 {
					return true, "Success"
				}
				if code == 0 {
					return false, "Stock not enough"
				}
				// 如果还是 -1，进入下一次 retry 循环
				continue
			}
			return false, "Stock not initialized (retries exhausted)"
		}
	}

	return false, "System busy"
}

func (c *cachedRepo) RestockProduct(ctx context.Context, req *pb.RestockProductRequest) error {
	count := len(req.Items)
	keys := make([]string, 0, count+2)
	args := make([]interface{}, 0, count+1)

	rollbackKey := fmt.Sprintf("rollback:%s", req.OrderId)
	keys = append(keys, rollbackKey, streamKey)

	type stockItem struct {
		Pid string `json:"pid"`
		Amt int32  `json:"amt"`
	}
	stockItems := make([]stockItem, 0, count)

	for _, item := range req.Items {
		keys = append(keys, stockKey(item.Item.ProductId))
		args = append(args, item.Item.Quantity)

		stockItems = append(stockItems, stockItem{
			Pid: item.Item.ProductId,
			Amt: -item.Item.Quantity,
		})
	}

	// 构造 Stock Payload JSON(含order id)
	stockPayloadObj := map[string]interface{}{
		"oid":   req.OrderId,
		"items": stockItems,
	}
	stockPayloadBytes, _ := json.Marshal(stockPayloadObj)

	finalArgs := make([]interface{}, 0, len(args)+1)
	finalArgs = append(finalArgs, string(stockPayloadBytes))
	finalArgs = append(finalArgs, args...)

	// 执行Lua脚本，外嵌熔断器
	_, err := c.cb.Execute(func() (interface{}, error) {
		return c.restockScript.Run(ctx, c.rdb, keys, finalArgs...).Result()
	})

	return err
}

// 批量检查并加载缺失的库存缓存
func (c *cachedRepo) loadBatchStockFromDB(ctx context.Context, items []*pb.OrderItem) error {
	for _, item := range items {
		pid := item.Item.ProductId
		// singleflight 防止缓存击穿
		_, err, _ := c.sf.Do("load_stock:"+pid, func() (interface{}, error) {
			sKey := stockKey(pid)
			// 如果已经存在则不需要覆盖
			if c.rdb.Exists(ctx, sKey).Val() > 0 {
				return nil, nil
			}

			// 从 MySQL 读取
			product, err := c.next.GetProduct(ctx, pid)
			if err != nil {
				return nil, err
			}

			// 回写 Redis (SetNX 防御并发覆盖)
			if err := c.rdb.SetNX(ctx, sKey, product.Stock, time.Hour*24).Err(); err != nil {
				c.log.Warnf("[LoadStock] SetNX failed (maybe race): %v", err)
			}
			return nil, nil
		})

		if err != nil {
			return err
		}
	}
	return nil
}

// 启动消息队列消费者
func (c *cachedRepo) startStreamWorker(ctx context.Context, wg *sync.WaitGroup) {
	defer wg.Done()
	c.log.Infof("[Stream Worker - %s] Start consuming stream %s", c.consumerName, streamKey)

	for {
		select {
		case <-ctx.Done():
			c.log.Infof("[Stream Worker] Shutting down")
			return
		default:
			// 获得流中的所有数据
			entries, err := c.rdb.XReadGroup(ctx, &redis.XReadGroupArgs{
				Group:    streamGroup,
				Consumer: c.consumerName,
				Streams:  []string{streamKey, ">"},
				Count:    200,
				Block:    2 * time.Second,
			}).Result()

			if err != nil {
				if err != redis.Nil {
					c.log.Errorf("[Stream Worker] Failed to read stream: %v", err)
				}
				continue
			}

			if len(entries) == 0 || len(entries[0].Messages) == 0 {
				continue
			}

			// 取出消息，批量刷入 MySQL
			messages := entries[0].Messages
			c.flushBufferToMySQL(ctx, messages)
		}
	}
}

func (c *cachedRepo) flushBufferToMySQL(ctx context.Context, messages []redis.XMessage) {
	atomic.StoreInt64(&c.lastFlushUnixNano, time.Now().UnixNano())

	// 提取消息ID列表
	msgIDList := make([]string, 0, len(messages))
	for _, msg := range messages {
		msgIDList = append(msgIDList, msg.ID)
	}

	// 开启事务
	tx := c.db.Begin()
	if tx.Error != nil {
		c.log.Errorf("[Flush] Failed to begin transaction: %v", tx.Error)
		return
	}

	defer tx.Rollback()

	// 检查重复消息
	var existedIDs []string
	if err := tx.Table("stock_dedup_log").
		Where("msg_id IN ?", msgIDList).
		Pluck("msg_id", &existedIDs).Error; err != nil {

		c.log.Errorf("[Flush] Failed to check dedup log: %v", err)
		atomic.AddUint64(&c.flushFailTotal, 1)
		return
	}

	existedMap := make(map[string]bool)
	for _, id := range existedIDs {
		existedMap[id] = true
	}

	// 解析并聚合有效消息
	validBuffer := make(map[string]int32)
	validNewMsgIDs := make([]string, 0, len(messages))
	newLogEntries := make([]StockDedupLog, 0, len(messages))
	traceLinks := make([]trace.Link, 0, len(messages))

	for _, msg := range messages {
		if existedMap[msg.ID] {
			continue
		}

		// 解析 Stock Payload JSON
		payloadStr, ok := msg.Values["payload"].(string)
		if !ok {
			c.log.Errorf("[Flush] Msg %s has no payload or invalid type", msg.ID)
			continue
		}

		type stockItem struct {
			Pid string `json:"pid"`
			Amt int32  `json:"amt"`
		}

		var payloadObj struct {
			Oid      string                 `json:"oid"`
			Items    []stockItem            `json:"items"`
			TraceCtx propagation.MapCarrier `json:"trace_ctx"`
		}

		if err := json.Unmarshal([]byte(payloadStr), &payloadObj); err != nil {
			c.log.Errorf("[Flush] Failed to unmarshal payload for msg %s: %v", msg.ID, err)
			continue
		}

		// 聚合有效扣减
		for _, item := range payloadObj.Items {
			validBuffer[item.Pid] += item.Amt
		}

		if len(payloadObj.TraceCtx) > 0 {
			sc := otel.GetTextMapPropagator().Extract(ctx, payloadObj.TraceCtx)
			traceLinks = append(traceLinks, trace.Link{SpanContext: trace.SpanContextFromContext(sc)})
		}

		// 记录新消息
		validNewMsgIDs = append(validNewMsgIDs, msg.ID)
		newLogEntries = append(newLogEntries, StockDedupLog{
			MsgID:     msg.ID,
			ProductID: payloadObj.Oid,
		})
	}

	tracer := otel.Tracer("productcatalogservice-repo")
	ctx, flushSpan := tracer.Start(ctx, "stock_flush_batch",
		trace.WithLinks(traceLinks...),
		trace.WithSpanKind(trace.SpanKindConsumer),
	)
	defer flushSpan.End()

	// 更新库存，记录去重
	if len(validBuffer) > 0 {
		// 构建 CASE WHEN 语句
		caseStmt := "CASE id"
		ids := make([]interface{}, 0, len(validBuffer))
		params := make([]interface{}, 0, len(validBuffer)*2)

		for pid, amount := range validBuffer {
			caseStmt += " WHEN ? THEN ?"
			params = append(params, pid, amount)
			ids = append(ids, pid)
		}
		caseStmt += " ELSE 0 END"

		// UPDATE products SET stock = stock - CASE ... WHERE id IN (...)
		query := fmt.Sprintf("UPDATE products SET stock = stock - (%s) WHERE id IN ?", caseStmt)
		params = append(params, ids)

		if err := tx.Exec(query, params...).Error; err != nil {
			c.log.Errorf("[Flush] Failed to batch update stock: %v", err)
			atomic.AddUint64(&c.flushFailTotal, 1)
			return
		}

		if err := tx.Create(&newLogEntries).Error; err != nil {
			c.log.Errorf("[Flush] Failed to insert dedup logs: %v", err)
			atomic.AddUint64(&c.flushFailTotal, 1)
			return
		}
	}

	// 提交事务
	if err := tx.Commit().Error; err != nil {
		c.log.Errorf("[Flush] Failed to commit transaction: %v", err)
		atomic.AddUint64(&c.flushFailTotal, 1)
		return
	}

	atomic.AddUint64(&c.flushSuccessTotal, 1)

	// 确认消息已处理
	if len(msgIDList) > 0 {
		err := c.rdb.XAck(ctx, streamKey, streamGroup, msgIDList...).Err()
		if err != nil {
			c.log.Errorf("[Stream Worker] Failed to ack messages: %v", err)
			atomic.AddUint64(&c.ACKFailTotal, 1)
		}
	}
}

// 启动 pending 消息恢复
func (c *cachedRepo) startPendingRecover(ctx context.Context, wg *sync.WaitGroup) {
	defer wg.Done()
	c.log.Infof("[Recovery - %s] Start recovering pending messages", c.consumerName)

	ticker := time.NewTicker(1 * time.Minute)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			c.log.Infof("[Recovery] Shutting down")
			return
		case <-ticker.C:
			// 获取 pending 消息
			pendings, err := c.rdb.XPendingExt(ctx, &redis.XPendingExtArgs{
				Stream: streamKey,
				Group:  streamGroup,
				Idle:   3 * time.Minute,
				Start:  "-",
				End:    "+",
				Count:  200,
			}).Result()

			if err != nil {
				c.log.Errorf("[Recovery] Failed to get pending messages: %v", err)
				atomic.AddUint64(&c.recoverFailTotal, 1)
				continue
			}

			if len(pendings) == 0 {
				atomic.AddUint64(&c.recoverSkipTotal, 1)
				continue
			}

			ids := make([]string, 0, len(pendings))
			for _, pending := range pendings {
				ids = append(ids, pending.ID)
			}

			// 认领 pending 消息
			messages, err := c.rdb.XClaim(ctx, &redis.XClaimArgs{
				Stream:   streamKey,
				Group:    streamGroup,
				Consumer: c.consumerName,
				MinIdle:  3 * time.Minute,
				Messages: ids,
			}).Result()

			if err != nil {
				c.log.Errorf("[Recovery] Failed to claim pending messages: %v", err)
				atomic.AddUint64(&c.recoverFailTotal, 1)
				continue
			}

			// 处理 pending 消息
			if len(messages) > 0 {
				c.log.Infof("[Recovery - %s] Recovered %d pending messages", c.consumerName, len(messages))
				c.flushBufferToMySQL(ctx, messages)
				atomic.AddUint64(&c.recoverSuccessTotal, 1)
			}
		}
	}
}

func stockKey(productId string) string {
	return fmt.Sprintf("product:stock:%s", productId)
}
