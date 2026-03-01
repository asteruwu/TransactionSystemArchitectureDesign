package worker

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/GoogleCloudPlatform/microservices-demo/src/orderservice/pkg/model"
	"github.com/GoogleCloudPlatform/microservices-demo/src/orderservice/pkg/repository"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"

	pb "github.com/GoogleCloudPlatform/microservices-demo/src/orderservice/genproto"
	rmq_client "github.com/apache/rocketmq-clients/golang/v5"
	"github.com/apache/rocketmq-clients/golang/v5/credentials"
	"github.com/go-sql-driver/mysql"
	"github.com/sirupsen/logrus"
)

const (
	maxDeliveryAttempts = int32(3)
	maxBatchSize        = int32(32)
	invisibleDuration   = 30 * time.Second
)

type ConsumerWorker struct {
	repo           repository.OrderRepo
	logger         *logrus.Logger
	client         rmq_client.SimpleConsumer
	producer       rmq_client.Producer
	shippingClient pb.ShippingServiceClient
	groupID        string

	shippingSuccessTotal uint64
	e2eLatency           metric.Int64Histogram
	consumerLag          metric.Int64Histogram
}

// 构造 consumer
func NewConsumerWorker(nameServer string, groupID string, accessKey string, accessSecret string, producer rmq_client.Producer, repo repository.OrderRepo, shippingClient pb.ShippingServiceClient, log *logrus.Logger) (*ConsumerWorker, error) {
	c, err := rmq_client.NewSimpleConsumer(
		&rmq_client.Config{
			Endpoint:      nameServer,
			ConsumerGroup: groupID,
			Credentials: &credentials.SessionCredentials{
				AccessKey:    accessKey,
				AccessSecret: accessSecret,
			},
		},
		rmq_client.WithSimpleAwaitDuration(2*time.Second),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create rocketmq simple consumer: %w", err)
	}

	w := &ConsumerWorker{
		repo:           repo,
		logger:         log,
		client:         c,
		producer:       producer,
		shippingClient: shippingClient,
		groupID:        groupID,
	}
	w.registerMetrics()

	return w, nil
}

func (w *ConsumerWorker) registerMetrics() {
	meter := otel.GetMeterProvider().Meter("orderservice.consumer")
	// 1. 订单端到端延迟 (Create -> DB Insert)
	w.e2eLatency, _ = meter.Int64Histogram("app_order_e2e_latency_ms",
		metric.WithDescription("Order end-to-end latency from creation to DB insert"))

	// 2. RocketMQ 消费延迟
	w.consumerLag, _ = meter.Int64Histogram("rocketmq_consumer_lag_ms",
		metric.WithDescription("RocketMQ consumer lag"))

	// 3. 发货成功次数 (Consumer 侧)
	meter.Int64ObservableGauge("app_shipping_success_total",
		metric.WithDescription("Shipping success count"),
		metric.WithInt64Callback(func(_ context.Context, obs metric.Int64Observer) error {
			obs.Observe(int64(atomic.LoadUint64(&w.shippingSuccessTotal)),
				metric.WithAttributes(attribute.String("source", "mq_consumer")))
			return nil
		}),
	)
}

// 启动 mq 下游 consumer
func (w *ConsumerWorker) Start(ctx context.Context, wg *sync.WaitGroup, topic string) {
	wg.Add(1)
	defer wg.Done()

	if err := w.client.Subscribe(topic, rmq_client.SUB_ALL); err != nil {
		w.logger.Errorf("Failed to subscribe topic %s: %v", topic, err)
		return
	}

	if err := w.client.Start(); err != nil {
		w.logger.Errorf("Failed to start rocketmq simple consumer: %v", err)
		return
	}
	defer w.client.GracefulStop()

	w.logger.Infof("RocketMQ SimpleConsumer started on topic: %s", topic)

	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		mvs, err := w.client.Receive(ctx, maxBatchSize, invisibleDuration)
		if err != nil {
			if isDeadlineExceededError(err) {
				// 正常长轮询超时（无消息），继续
				continue
			}
			w.logger.Warnf("[Consumer] Receive error: %v", err)
			continue
		}

		if len(mvs) == 0 {
			continue
		}

		// 记录消费延迟
		for _, mv := range mvs {
			if mv.GetBornTimestamp() != nil {
				lag := time.Now().UnixMilli() - mv.GetBornTimestamp().UnixMilli()
				w.consumerLag.Record(ctx, lag,
					metric.WithAttributes(
						attribute.String("group", w.groupID),
						attribute.String("topic", mv.GetTopic())))
			}
		}

		// 根据 topic 路由处理
		if topic == "order_status_events" {
			w.handleOrderStatusUpdate(ctx, mvs)
		} else {
			w.handleOrderCreate(ctx, mvs)
		}
	}
}

func (w *ConsumerWorker) handleOrderStatusUpdate(ctx context.Context, mvs []*rmq_client.MessageView) {
	type parsedEvent struct {
		mv        *rmq_client.MessageView
		event     model.OrderStatusEvent
		statusInt int32
	}

	var paidEvents []parsedEvent
	var cancelledMvs []*rmq_client.MessageView
	statusMap := make(map[string]int32)
	traceLinks := make([]trace.Link, 0, len(mvs))

	// 1. 解析并按状态分类
	for _, mv := range mvs {
		var event model.OrderStatusEvent
		if err := json.Unmarshal(mv.GetBody(), &event); err != nil {
			w.logger.Errorf("Invalid JSON for status event: %v", err)
			w.sendToDLQ(ctx, mv, fmt.Sprintf("json_unmarshal_error: %v", err))
			w.ack(ctx, mv)
			continue
		}

		// 1.1 提取 Trace Context 并创建 Span
		carrier := propagation.MapCarrier{}
		for k, v := range mv.GetProperties() {
			carrier[k] = v
		}
		upstreamCtx := otel.GetTextMapPropagator().Extract(ctx, carrier)
		traceLinks = append(traceLinks, trace.Link{SpanContext: trace.SpanContextFromContext(upstreamCtx)})

		switch event.Status {
		case "PAID":
			statusMap[event.OrderID] = 1
			paidEvents = append(paidEvents, parsedEvent{mv: mv, event: event, statusInt: 1})
		case "CANCELLED":
			statusMap[event.OrderID] = 2
			cancelledMvs = append(cancelledMvs, mv)
		default:
			w.logger.Warnf("Unknown status: %s", event.Status)
			w.ack(ctx, mv)
		}
	}

	if len(statusMap) == 0 {
		return
	}

	tracer := otel.Tracer("orderservice-worker")
	ctx, batchSpan := tracer.Start(ctx, "batch_status_update",
		trace.WithLinks(traceLinks...),
		trace.WithSpanKind(trace.SpanKindConsumer),
	)
	defer batchSpan.End()

	// 2. 默认：批量更新状态
	allValidMvs := make([]*rmq_client.MessageView, 0, len(paidEvents)+len(cancelledMvs))
	for _, pe := range paidEvents {
		allValidMvs = append(allValidMvs, pe.mv)
	}
	allValidMvs = append(allValidMvs, cancelledMvs...)

	err := w.repo.UpdateOrderStatusBatch(ctx, statusMap)
	if err != nil {
		w.logger.Warnf("[OrderStatus] Batch update failed (%v), falling back to sequential update", err)
		w.handleOrderStatusUpdateSequential(ctx, allValidMvs)
		return
	}

	// 3. 对 PAID 订单触发发货 (批量 + 并发)
	if len(paidEvents) == 0 || w.shippingClient == nil {
		// 批量更新成功，ACK 所有有效消息
		for _, mv := range allValidMvs {
			w.ack(ctx, mv)
		}
		return
	}

	// 3.1 批量获取订单
	orderIDs := make([]string, 0, len(paidEvents))
	for _, pe := range paidEvents {
		orderIDs = append(orderIDs, pe.event.OrderID)
	}

	orders, err := w.repo.GetOrdersByIDs(ctx, orderIDs)
	if err != nil {
		w.logger.Warnf("[Shipping] Failed to batch get orders: %v (will retry via MQ)", err)
		// GetOrdersByIDs 失败：PAID 消息不 ACK 等待重投，CANCELLED 消息直接 ACK
		for _, mv := range cancelledMvs {
			w.ack(ctx, mv)
		}
		return
	}

	// 过滤非 PAID 状态的订单；建立 order_id 到 order 对象的映射
	orderMap := make(map[string]*model.Order)
	for _, order := range orders {
		if order.Status == model.OrderStatusPaid {
			orderMap[order.OrderID] = order
		}
	}

	// 3.2 并发调用发货 RPC，按消息粒度绑定结果
	type shipResult struct {
		mv       *rmq_client.MessageView
		shipment *model.Shipment
	}

	shipResults := make([]shipResult, 0, len(paidEvents))
	var mu sync.Mutex
	var shipWg sync.WaitGroup
	sem := make(chan struct{}, 10)

	for _, pe := range paidEvents {
		order, ok := orderMap[pe.event.OrderID]
		if !ok {
			// 订单不在 PAID 状态（已发货或已取消），视为已处理，ACK 掉
			w.ack(ctx, pe.mv)
			continue
		}
		shipWg.Add(1)
		go func(o *model.Order, mv *rmq_client.MessageView) {
			defer shipWg.Done()
			sem <- struct{}{}
			defer func() { <-sem }()

			shipment := w.shipOrderAndCollect(ctx, o)
			mu.Lock()
			shipResults = append(shipResults, shipResult{mv: mv, shipment: shipment})
			mu.Unlock()
		}(order, pe.mv)
	}
	shipWg.Wait()

	// 3.3 批量更新发货状态（成功的订单）
	var shipments []*model.Shipment
	for _, r := range shipResults {
		if r.shipment != nil {
			shipments = append(shipments, r.shipment)
		}
	}
	if len(shipments) > 0 {
		if err := w.repo.UpdateOrdersAndInsertShipmentsBatch(ctx, shipments); err != nil {
			w.logger.Warnf("[Shipping] Batch update failed (%v), falling back to sequential", err)
			for _, s := range shipments {
				if err := w.repo.UpdateOrderAndInsertShipment(ctx, s.OrderID, s.Status, s); err != nil {
					w.logger.Warnf("[Shipping] Failed to update shipment for %s: %v", s.OrderID, err)
				}
			}
		} else {
			w.logger.Infof("[Shipping] Batch updated %d shipments", len(shipments))
		}
	}

	// 3.4 逐条 ACK：RPC 成功的消息立即 ACK，失败的不 ACK 等待重投
	for _, r := range shipResults {
		if r.shipment != nil {
			w.ack(ctx, r.mv)
		}
	}

	// CANCELLED 消息在批量更新成功后直接 ACK
	for _, mv := range cancelledMvs {
		w.ack(ctx, mv)
	}
}

// shipOrderAndCollect 发货并收集结果
func (w *ConsumerWorker) shipOrderAndCollect(ctx context.Context, order *model.Order) *model.Shipment {
	cartItems := w.convertToCartItems(order.Items)
	shipReq := &pb.ShipOrderRequest{
		Address: &pb.Address{
			StreetAddress: order.ShippingAddress,
		},
		Items:   cartItems,
		OrderId: order.OrderID,
	}

	shipResp, err := w.shippingClient.ShipOrder(ctx, shipReq)
	if err != nil {
		w.logger.Warnf("[Shipping] RPC failed for order %s: %v (will retry via MQ)", order.OrderID, err)
		return nil
	}

	w.logger.Infof("[Shipping] Order %s shipped successfully, tracking: %s", order.OrderID, shipResp.TrackingId)
	atomic.AddUint64(&w.shippingSuccessTotal, 1)
	return &model.Shipment{
		OrderID:    order.OrderID,
		TrackingID: shipResp.TrackingId,
		Status:     model.OrderStatusShipped,
	}
}

// handleOrderStatusUpdateSequential 降级：逐条更新
func (w *ConsumerWorker) handleOrderStatusUpdateSequential(ctx context.Context, mvs []*rmq_client.MessageView) {
	for _, mv := range mvs {
		var event model.OrderStatusEvent
		json.Unmarshal(mv.GetBody(), &event)
		statusInt := int32(0)
		if event.Status == "PAID" {
			statusInt = 1
		} else {
			statusInt = 2
		}

		err := w.repo.UpdateOrderStatus(ctx, event.OrderID, statusInt)
		if err != nil {
			w.logger.Warnf("[Fallback] Update status failed. OrderID: %s, Error: %v", event.OrderID, err)

			if mv.GetDeliveryAttempt() >= maxDeliveryAttempts {
				w.sendToDLQ(ctx, mv, fmt.Sprintf("db_update_error_max_retry: %v", err))
				w.ack(ctx, mv)
				continue
			}
			// 需要重试：不 ACK，等待 Broker 重投
			continue
		}
		w.ack(ctx, mv)
	}
}

// convertToCartItems 辅助函数
func (w *ConsumerWorker) convertToCartItems(items []model.OrderItem) []*pb.CartItem {
	var cartItems []*pb.CartItem
	for _, item := range items {
		cartItems = append(cartItems, &pb.CartItem{
			ProductId: item.ProductID,
			Quantity:  item.Quantity,
		})
	}
	return cartItems
}

func (w *ConsumerWorker) handleOrderCreate(ctx context.Context, mvs []*rmq_client.MessageView) {
	var validOrders []*model.Order
	var validMvs []*rmq_client.MessageView
	traceLinks := make([]trace.Link, 0, len(mvs))

	// 1. 解析并聚合有效消息
	for _, mv := range mvs {
		var rMsg model.OrderMessage
		if err := json.Unmarshal(mv.GetBody(), &rMsg); err != nil {
			w.logger.Errorf("Invalid JSON: %v. Body: %s", err, string(mv.GetBody()))
			w.sendToDLQ(ctx, mv, fmt.Sprintf("json_unmarshal_error: %v", err))
			w.ack(ctx, mv)
			continue
		}

		// 提取 Trace Context
		carrier := propagation.MapCarrier{}
		for k, v := range mv.GetProperties() {
			carrier[k] = v
		}
		upstreamCtx := otel.GetTextMapPropagator().Extract(ctx, carrier)
		traceLinks = append(traceLinks, trace.Link{SpanContext: trace.SpanContextFromContext(upstreamCtx)})

		order := &model.Order{
			OrderID:    rMsg.OrderID,
			UserID:     rMsg.UserID,
			Status:     rMsg.Status,
			Items:      rMsg.Items,
			TotalPrice: rMsg.TotalPrice,
		}

		validOrders = append(validOrders, order)
		validMvs = append(validMvs, mv)
	}

	if len(validOrders) == 0 {
		return
	}

	tracer := otel.Tracer("orderservice-worker")
	ctx, batchSpan := tracer.Start(ctx, "batch_insert_orders",
		trace.WithLinks(traceLinks...),
		trace.WithSpanKind(trace.SpanKindConsumer),
	)
	defer batchSpan.End()

	// 2. 默认：批量插入
	err := w.repo.InsertOrdersBatch(ctx, validOrders)
	if err == nil {
		for i, mv := range validMvs {
			lag := time.Now().UnixMilli() - validMvs[i].GetBornTimestamp().UnixMilli()
			w.e2eLatency.Record(ctx, lag, metric.WithAttributes(attribute.String("flow", "create_order")))
			w.ack(ctx, mv)
		}
		return
	}

	// 3. 降级：逐条插入
	batchSpan.RecordError(err)
	w.logger.Warnf("Batch insert failed (%v), falling back to sequential insert for %d messages", err, len(validOrders))
	w.fallbackSequentialInsert(ctx, validMvs, validOrders)
}

func (w *ConsumerWorker) fallbackSequentialInsert(ctx context.Context, mvs []*rmq_client.MessageView, orders []*model.Order) {
	for i, order := range orders {
		mv := mvs[i]
		err := w.repo.InsertOrder(ctx, order)
		if err != nil {
			if isDuplicateError(err) {
				w.logger.Warnf("[Fallback] Duplicate order detected (Idempotent success), OrderID: %s", order.OrderID)
				w.ack(ctx, mv)
				continue
			}

			w.logger.Warnf("[Fallback] DB Insert failed. OrderID: %s, Error: %v, DeliveryAttempt: %d", order.OrderID, err, mv.GetDeliveryAttempt())

			if mv.GetDeliveryAttempt() >= maxDeliveryAttempts {
				w.logger.Errorf("[Fallback] Max delivery attempts reached for OrderID: %s. Sending to DLQ.", order.OrderID)
				w.sendToDLQ(ctx, mv, fmt.Sprintf("db_insert_error_max_retry: %v", err))
				w.ack(ctx, mv)
				continue
			}

			// 需要重试：不 ACK，等待 invisibleDuration 到期后 Broker 重投
			continue
		}
		w.ack(ctx, mv)
	}
}

// 幂等性检查
func isDuplicateError(err error) bool {
	var mysqlErr *mysql.MySQLError
	if errors.As(err, &mysqlErr) {
		if mysqlErr.Number == 1062 {
			return true
		}
	}
	return false
}

// 封装单消息 ACK
func (w *ConsumerWorker) ack(ctx context.Context, mv *rmq_client.MessageView) {
	if err := w.client.Ack(ctx, mv); err != nil {
		w.logger.Warnf("[ACK] Failed to ack message %s: %v (will be redelivered)", mv.GetMessageId(), err)
	}
}

func (w *ConsumerWorker) sendToDLQ(ctx context.Context, mv *rmq_client.MessageView, reason string) {
	dlqTopic := "%DLQ%" + w.groupID
	dlqMsg := &rmq_client.Message{
		Topic: dlqTopic,
		Body:  mv.GetBody(),
	}
	dlqMsg.AddProperty("original_msg_id", mv.GetMessageId())
	dlqMsg.AddProperty("dlq_reason", reason)
	dlqMsg.AddProperty("source", "rocketMQ_consumer")

	_, err := w.producer.Send(ctx, dlqMsg)
	if err != nil {
		// DLQ 发不出去，打印日志且直接返回，防止卡死消费者
		w.logger.Errorf("CRITICAL: Failed to send message to DLQ! MsgID: %s, Error: %v", mv.GetMessageId(), err)
	} else {
		w.logger.Infof("Sent message to DLQ. MsgID: %s, Reason: %s", mv.GetMessageId(), reason)
	}
}

// isDeadlineExceededError 判断是否为长轮询正常超时
func isDeadlineExceededError(err error) bool {
	if err == nil {
		return false
	}
	// v5 SDK 在无消息时返回 DEADLINE_EXCEEDED 错误
	errStr := err.Error()
	return strings.Contains(errStr, "DEADLINE_EXCEEDED") || strings.Contains(errStr, "deadline exceeded")
}
