package worker

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"

	"github.com/GoogleCloudPlatform/microservices-demo/src/orderservice/pkg/model"
	"github.com/GoogleCloudPlatform/microservices-demo/src/orderservice/pkg/repository"

	pb "github.com/GoogleCloudPlatform/microservices-demo/src/orderservice/genproto"
	rocketmq "github.com/apache/rocketmq-client-go/v2"
	"github.com/apache/rocketmq-client-go/v2/consumer"
	"github.com/apache/rocketmq-client-go/v2/primitive"
	"github.com/go-sql-driver/mysql"
	"github.com/sirupsen/logrus"
)

type ConsumerWorker struct {
	repo           repository.OrderRepo
	logger         *logrus.Logger
	client         rocketmq.PushConsumer
	producer       rocketmq.Producer
	shippingClient pb.ShippingServiceClient
	groupID        string
}

// 构造 consumer
func NewConsumerWorker(nameServers []string, groupID string, producer rocketmq.Producer, repo repository.OrderRepo, shippingClient pb.ShippingServiceClient, log *logrus.Logger) (*ConsumerWorker, error) {
	c, err := rocketmq.NewPushConsumer(
		consumer.WithGroupName(groupID),
		consumer.WithNameServer(nameServers),
		consumer.WithMaxReconsumeTimes(3),
		consumer.WithConsumerModel(consumer.Clustering),
		consumer.WithConsumeMessageBatchMaxSize(32), // 批量消费，每次回调最多32条
	)

	if err != nil {
		return nil, fmt.Errorf("failed to create rocketmq consumer: %w", err)
	}

	return &ConsumerWorker{
		repo:           repo,
		logger:         log,
		client:         c,
		producer:       producer,
		shippingClient: shippingClient,
		groupID:        groupID,
	}, nil
}

// 启动 mq 下游 consumer
func (w *ConsumerWorker) Start(ctx context.Context, wg *sync.WaitGroup, topic string) {
	wg.Add(1)
	defer wg.Done()

	err := w.client.Subscribe(topic, consumer.MessageSelector{}, w.handleMessage)
	if err != nil {
		w.logger.Errorf("Failed to subscribe topic %s: %v", topic, err)
		return
	}

	if err := w.client.Start(); err != nil {
		w.logger.Errorf("Failed to start rocketmq consumer: %v", err)
		return
	}
	w.logger.Infof("RocketMQ Consumer started on topic: %s", topic)

	<-ctx.Done()

	w.logger.Info("Stopping RocketMQ consumer...")
	if err := w.client.Shutdown(); err != nil {
		w.logger.Errorf("Failed to shutdown consumer: %v", err)
	}
}

// 基于topic选择消费方式
func (w *ConsumerWorker) handleMessage(ctx context.Context, msgs ...*primitive.MessageExt) (consumer.ConsumeResult, error) {
	if len(msgs) == 0 {
		return consumer.ConsumeSuccess, nil
	}
	topic := msgs[0].Topic

	if topic == "order_status_events" {
		return w.handleOrderStatusUpdate(ctx, msgs)
	}
	return w.handleOrderCreate(ctx, msgs)
}

func (w *ConsumerWorker) handleOrderStatusUpdate(ctx context.Context, msgs []*primitive.MessageExt) (consumer.ConsumeResult, error) {
	type parsedEvent struct {
		msg       *primitive.MessageExt
		event     model.OrderStatusEvent
		statusInt int32
	}

	var paidEvents []parsedEvent
	statusMap := make(map[string]int32)
	validMsgs := make([]*primitive.MessageExt, 0, len(msgs))

	// 1. 解析并聚合有效消息
	for _, msg := range msgs {
		var event model.OrderStatusEvent
		if err := json.Unmarshal(msg.Body, &event); err != nil {
			w.logger.Errorf("Invalid JSON for status event: %v", err)
			w.sendToDLQ(ctx, msg, fmt.Sprintf("json_unmarshal_error: %v", err))
			continue
		}

		statusInt := int32(0)
		switch event.Status {
		case "PAID":
			statusInt = 1
			paidEvents = append(paidEvents, parsedEvent{msg: msg, event: event, statusInt: statusInt})
		case "CANCELLED":
			statusInt = 2
		default:
			w.logger.Warnf("Unknown status: %s", event.Status)
			continue
		}

		statusMap[event.OrderID] = statusInt
		validMsgs = append(validMsgs, msg)
	}

	if len(statusMap) == 0 {
		return consumer.ConsumeSuccess, nil
	}

	// 2. 批量更新状态
	err := w.repo.UpdateOrderStatusBatch(ctx, statusMap)
	if err != nil {
		w.logger.Warnf("[OrderStatus] Batch update failed (%v), falling back to sequential update", err)
		return w.handleOrderStatusUpdateSequential(ctx, validMsgs)
	}

	// 3. 对 PAID 订单触发发货 (批量 + 并发)
	if len(paidEvents) == 0 || w.shippingClient == nil {
		return consumer.ConsumeSuccess, nil
	}

	// 3.1 批量获取订单
	orderIDs := make([]string, 0, len(paidEvents))
	for _, pe := range paidEvents {
		orderIDs = append(orderIDs, pe.event.OrderID)
	}

	orders, err := w.repo.GetOrdersByIDs(ctx, orderIDs)
	if err != nil {
		w.logger.Warnf("[Shipping] Failed to batch get orders: %v (will retry via MQ)", err)
		return consumer.ConsumeRetryLater, nil
	}

	// 过滤非 PAID 状态的订单
	orderMap := make(map[string]*model.Order)
	for _, order := range orders {
		if order.Status == model.OrderStatusPaid {
			orderMap[order.OrderID] = order
		}
	}

	if len(orderMap) == 0 {
		return consumer.ConsumeSuccess, nil
	}

	// 3.2 并发调用发货 RPC
	var shipments []*model.Shipment
	var mu sync.Mutex
	var wg sync.WaitGroup
	var hasRPCFailure bool

	sem := make(chan struct{}, 10)

	for _, order := range orderMap {
		wg.Add(1)
		go func(o *model.Order) {
			defer wg.Done()
			sem <- struct{}{}
			defer func() { <-sem }()

			shipment := w.shipOrderAndCollect(ctx, o, &hasRPCFailure)
			if shipment != nil {
				mu.Lock()
				shipments = append(shipments, shipment)
				mu.Unlock()
			}
		}(order)
	}
	wg.Wait()

	// 3.3 批量更新发货状态
	if len(shipments) > 0 {
		if err := w.repo.UpdateOrdersAndInsertShipmentsBatch(ctx, shipments); err != nil {
			w.logger.Warnf("[Shipping] Batch update failed (%v), falling back to sequential", err)
			// 降级处理
			for _, s := range shipments {
				if err := w.repo.UpdateOrderAndInsertShipment(ctx, s.OrderID, s.Status, s); err != nil {
					w.logger.Warnf("[Shipping] Failed to update shipment for %s: %v", s.OrderID, err)
				}
			}
		} else {
			w.logger.Infof("[Shipping] Batch updated %d shipments", len(shipments))
		}
	}

	if hasRPCFailure {
		return consumer.ConsumeRetryLater, nil
	}
	return consumer.ConsumeSuccess, nil
}

// shipOrderAndCollect 发货并收集结果
func (w *ConsumerWorker) shipOrderAndCollect(ctx context.Context, order *model.Order, hasFailure *bool) *model.Shipment {
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
		*hasFailure = true
		return nil
	}

	w.logger.Infof("[Shipping] Order %s shipped successfully, tracking: %s", order.OrderID, shipResp.TrackingId)
	return &model.Shipment{
		OrderID:    order.OrderID,
		TrackingID: shipResp.TrackingId,
		Status:     model.OrderStatusShipped,
	}
}

// handleOrderStatusUpdateSequential 降级：逐条更新
func (w *ConsumerWorker) handleOrderStatusUpdateSequential(ctx context.Context, msgs []*primitive.MessageExt) (consumer.ConsumeResult, error) {
	shouldRetryBatch := false
	for _, msg := range msgs {
		var event model.OrderStatusEvent
		json.Unmarshal(msg.Body, &event)
		statusInt := int32(0)
		if event.Status == "PAID" {
			statusInt = 1
		} else {
			statusInt = 2
		}

		err := w.repo.UpdateOrderStatus(ctx, event.OrderID, statusInt)
		if err != nil {
			w.logger.Warnf("[Fallback] Update status failed. OrderID: %s, Error: %v", event.OrderID, err)

			if msg.ReconsumeTimes >= 3 {
				w.sendToDLQ(ctx, msg, fmt.Sprintf("db_update_error_max_retry: %v", err))
				continue
			}
			shouldRetryBatch = true
		}
	}

	if shouldRetryBatch {
		return consumer.ConsumeRetryLater, nil
	}
	return consumer.ConsumeSuccess, nil
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

func (w *ConsumerWorker) handleOrderCreate(ctx context.Context, msgs []*primitive.MessageExt) (consumer.ConsumeResult, error) {
	var validOrders []*model.Order
	var validMsgs []*primitive.MessageExt

	// 1. 解析并聚合有效消息
	for _, msg := range msgs {
		var rMsg model.OrderMessage
		if err := json.Unmarshal(msg.Body, &rMsg); err != nil {
			w.logger.Errorf("Invalid JSON: %v. Body: %s", err, string(msg.Body))
			w.sendToDLQ(ctx, msg, fmt.Sprintf("json_unmarshal_error: %v", err))
			continue
		}

		order := &model.Order{
			OrderID:    rMsg.OrderID,
			UserID:     rMsg.UserID,
			Status:     rMsg.Status,
			Items:      rMsg.Items,
			TotalPrice: rMsg.TotalPrice,
		}

		validOrders = append(validOrders, order)
		validMsgs = append(validMsgs, msg)
	}

	if len(validOrders) == 0 {
		return consumer.ConsumeSuccess, nil
	}

	// 2. 默认：批量插入订单记录
	err := w.repo.InsertOrdersBatch(ctx, validOrders)
	if err == nil {
		return consumer.ConsumeSuccess, nil
	}

	// 3. 降级：批量插入失败
	w.logger.Warnf("Batch insert failed (%v), falling back to sequential insert for %d messages", err, len(validOrders))
	return w.fallbackSequentialInsert(ctx, validMsgs, validOrders)
}

func (w *ConsumerWorker) fallbackSequentialInsert(ctx context.Context, msgs []*primitive.MessageExt, orders []*model.Order) (consumer.ConsumeResult, error) {
	shouldRetryBatch := false

	// 遍历时逐消息插入
	for i, order := range orders {
		msg := msgs[i]
		err := w.repo.InsertOrder(ctx, order)
		if err != nil {
			if isDuplicateError(err) {
				w.logger.Warnf("[Fallback] Duplicate order detected (Idempotent success), OrderID: %s", order.OrderID)
				continue
			}

			w.logger.Warnf("[Fallback] DB Insert failed. OrderID: %s, Error: %v, ReconsumeTimes: %d", order.OrderID, err, msg.ReconsumeTimes)

			if msg.ReconsumeTimes >= 3 {
				w.logger.Errorf("[Fallback] Max reconsume times reached for OrderID: %s. Sending to DLQ.", order.OrderID)
				w.sendToDLQ(ctx, msg, fmt.Sprintf("db_insert_error_max_retry: %v", err))
				continue
			}

			shouldRetryBatch = true
		}
	}

	if shouldRetryBatch {
		return consumer.ConsumeRetryLater, nil
	}
	return consumer.ConsumeSuccess, nil
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

func (w *ConsumerWorker) sendToDLQ(ctx context.Context, originalMsg *primitive.MessageExt, reason string) {
	// 构造死信消息，发送到当前 Consumer Group 对应的 DLQ
	dlqTopic := "%DLQ%" + w.groupID
	dlqMsg := primitive.NewMessage(dlqTopic, originalMsg.Body)
	// 保留原始的 msgID 以便追踪
	dlqMsg.WithProperty("original_msg_id", originalMsg.MsgId)
	dlqMsg.WithProperty("dlq_reason", reason)
	dlqMsg.WithProperty("source", "rocketMQ_consumer")
	dlqMsg.WithKeys([]string{originalMsg.GetKeys()})

	res, err := w.producer.SendSync(ctx, dlqMsg)
	if err != nil {
		// DLQ发不出去，打印日志且直接返回success，防止卡死之后的消费者
		w.logger.Errorf("CRITICAL: Failed to send message to DLQ! MsgID: %s, Error: %v", originalMsg.MsgId, err)
	} else {
		w.logger.Infof("Sent message to DLQ. MsgID: %s, Reason: %s, DLQ_MsgID: %s", originalMsg.MsgId, reason, res.MsgID)
	}
}
