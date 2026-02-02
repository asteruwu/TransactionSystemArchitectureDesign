package service

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	pb "github.com/GoogleCloudPlatform/microservices-demo/src/orderservice/genproto"
	"github.com/GoogleCloudPlatform/microservices-demo/src/orderservice/pkg/model"
	"github.com/GoogleCloudPlatform/microservices-demo/src/orderservice/pkg/repository"
	"github.com/apache/rocketmq-client-go/v2/primitive"
	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
	"github.com/sirupsen/logrus"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type ShipmentPusher interface {
	Push(s *model.Shipment)
}

type MQProducer interface {
	SendSync(ctx context.Context, msgs ...*primitive.Message) (*primitive.SendResult, error)
}

type OrderService struct {
	pb.UnimplementedOrderServiceServer
	catalogClient  pb.ProductCatalogServiceClient
	paymentClient  pb.PaymentServiceClient
	shippingClient pb.ShippingServiceClient
	producer       MQProducer
	repo           repository.OrderRepo
	shipmentPusher ShipmentPusher
	rdb            *redis.Client
}

// 订单服务初始化
func NewOrderService(catalogClient pb.ProductCatalogServiceClient, paymentClient pb.PaymentServiceClient, shippingClient pb.ShippingServiceClient, producer MQProducer, repo repository.OrderRepo, shipmentPusher ShipmentPusher) *OrderService {
	return &OrderService{
		catalogClient:  catalogClient,
		paymentClient:  paymentClient,
		shippingClient: shippingClient,
		producer:       producer,
		repo:           repo,
		shipmentPusher: shipmentPusher,
	}
}

// 扣减库存，生成订单，支付扣款，异步发货
func (s *OrderService) CreateOrder(ctx context.Context, req *pb.CreateOrderRequest) (*pb.CreateOrderResponse, error) {
	// 1. 生成order id
	orderID := uuid.New().String()

	// 2. 校验价格
	if req.TotalPrice == nil {
		return nil, fmt.Errorf("total_price is required")
	}
	totalPrice := req.TotalPrice

	// 3. rpc调用：库存扣减
	chargeReq := &pb.ChargeProductRequest{
		OrderId:    orderID,
		UserId:     req.UserId,
		Items:      req.Items,
		TotalPrice: totalPrice,
		Address:    req.Address,
	}

	resp, err := s.catalogClient.ChargeProduct(ctx, chargeReq)
	if err != nil {
		return nil, fmt.Errorf("failed to charge product: %v", err)
	}
	if !resp.Success {
		return &pb.CreateOrderResponse{
			Success: false,
			Message: fmt.Sprintf("Inventory deduction failed: %s", resp.Message),
		}, nil
	}

	// 4. rpc调用：支付
	paymentReq := &pb.ChargeRequest{
		Amount:     totalPrice,
		CreditCard: req.CreditCard,
		OrderId:    orderID,
	}

	_, err = s.paymentClient.Charge(ctx, paymentReq)
	if err != nil {
		logrus.Warnf("[CreateOrder] Payment failed for Order %s: %v. Order remains PENDING for retry or timeout cancellation.", orderID, err)
		return &pb.CreateOrderResponse{
			Success: false,
			Message: fmt.Sprintf("Payment failed: %v", err),
			OrderId: orderID, // Return OrderID so client knows what to retry
		}, nil
	}

	// 5. 支付成功 -> 更新订单状态
	s.sendOrderStatusEvent(ctx, orderID, "PAID")

	// 6. 异步发货
	go func() {
		bgCtx := context.Background()
		cartItems := extractCartItemsFromProto(req.Items)
		// 常规路径
		s.shipOrderHappyPath(bgCtx, orderID, req.Address, cartItems)
	}()

	// 7. 返回成功
	return &pb.CreateOrderResponse{
		OrderId: orderID,
		Success: true,
		Message: "Order accepted",
	}, nil
}

func (s *OrderService) shipOrderHappyPath(ctx context.Context, orderID string, address *pb.Address, items []*pb.CartItem) {
	// rpc调用：发货
	shipResp, err := s.shippingClient.ShipOrder(ctx, &pb.ShipOrderRequest{
		Address: address,
		Items:   items,
		OrderId: orderID,
	})

	if err != nil {
		logrus.Errorf("[HappyPath] ShipOrder failed for %s: %v. Will be handled by Recover Worker.", orderID, err)
		return
	}

	// 准备状态更新数据
	shipment := &model.Shipment{
		OrderID:    orderID,
		TrackingID: shipResp.TrackingId,
		Status:     model.OrderStatusShipped,
	}

	if s.shipmentPusher != nil {
		// 生产：发送到channel
		s.shipmentPusher.Push(shipment)
	} else {
		// 降级：逐请求处理
		err = s.repo.UpdateOrderAndInsertShipment(ctx, orderID, model.OrderStatusShipped, shipment)
		if err != nil {
			logrus.Errorf("[HappyPath] Failed to update DB for %s: %v. Recover Worker will fix idempotency.", orderID, err)
		}
	}
}

func (s *OrderService) sendOrderStatusEvent(ctx context.Context, orderID, status string) {
	event := model.OrderStatusEvent{
		OrderID: orderID,
		Status:  status,
	}
	data, _ := json.Marshal(event)

	// 发送消息到mq
	msg := primitive.NewMessage("order_status_events", data)
	msg.WithKeys([]string{orderID})

	res, err := s.producer.SendSync(ctx, msg)
	if err != nil {
		logrus.Errorf("[OrderService] Failed to send status event (%s) for order %s: %v", status, orderID, err)
	} else {
		logrus.Infof("[OrderService] Sent status event (%s) for order %s. MsgID: %s", status, orderID, res.MsgID)
	}
}

// 未付款订单取消处理
func (s *OrderService) CancelOrderWithoutPayment(ctx context.Context, req *pb.CancelOrderRequest) (*pb.CancelOrderResponse, error) {
	orderID := req.OrderId

	// 1. 校验订单状态
	order, err := s.repo.GetOrder(ctx, orderID)
	if err != nil {
		return &pb.CancelOrderResponse{Success: false, Message: fmt.Sprintf("Order not found: %v", err)}, nil
	}

	// 0 = PENDING, 1 = PAID, 2 = CANCELLED
	if order.Status == model.OrderStatusCancelled {
		return &pb.CancelOrderResponse{Success: true, Message: "Order already cancelled"}, nil
	}
	if order.Status == model.OrderStatusPaid || order.Status == model.OrderStatusShipped {
		return &pb.CancelOrderResponse{Success: false, Message: "Order is already PAID/SHIPPED. Please request refund."}, nil
	}
	if order.Status != model.OrderStatusPending {
		return &pb.CancelOrderResponse{Success: false, Message: fmt.Sprintf("Order status %d cannot be cancelled", order.Status)}, nil
	}

	// 2. 检查支付情况
	var chargeResp *pb.ChargeResponse
	var paymentErr error

	// 获得交易流水单号
	for i := 0; i < 3; i++ {
		if i > 0 {
			time.Sleep(time.Duration(100*(1<<i)) * time.Millisecond)
		}
		chargeResp, paymentErr = s.paymentClient.GetCharge(ctx, &pb.GetChargeRequest{OrderId: orderID})
		if paymentErr == nil {
			break
		}
		logrus.Warnf("[CancelOrder] Payment check failed (attempt %d/3) for %s: %v", i+1, orderID, paymentErr)
	}

	if paymentErr != nil {
		st, ok := status.FromError(paymentErr)
		// 确认未支付
		if ok && st.Code() == codes.NotFound {
			// 支付记录为空
		} else {
			// 否则拒绝取消订单
			logrus.Errorf("[CancelOrder] Payment verification failed for %s after retries. Aborting cancellation.", orderID)
			return &pb.CancelOrderResponse{
				Success: false,
				Message: "System busy: Unable to verify payment status. Please try again later.",
			}, nil
		}
	} else if chargeResp.GetTransactionId() != "" {
		// 已支付：不可直接取消
		return &pb.CancelOrderResponse{
			Success: false,
			Message: "Payment record found. Order is paid. Please request refund.",
		}, nil
	}

	// 3. prc调用：库存回滚
	restockReq := &pb.RestockProductRequest{
		OrderId: orderID,
		Items:   convertItemsToProto(order.Items),
	}
	_, err = s.catalogClient.RestockProduct(ctx, restockReq)
	if err != nil {
		return &pb.CancelOrderResponse{Success: false, Message: fmt.Sprintf("Restock failed: %v", err)}, nil
	}

	// 4. 发送状态更新消息到mq
	s.sendOrderStatusEvent(ctx, orderID, "CANCELLED")

	return &pb.CancelOrderResponse{Success: true, Message: "Order cancelled successfully"}, nil
}

// 辅助函数
func convertItemsToProto(items []model.OrderItem) []*pb.OrderItem {
	var protoItems []*pb.OrderItem
	for _, item := range items {
		protoItems = append(protoItems, &pb.OrderItem{
			Item: &pb.CartItem{
				ProductId: item.ProductID,
				Quantity:  item.Quantity,
			},
			Cost: &pb.Money{
				CurrencyCode: item.Cost.CurrencyCode,
				Units:        item.Cost.Units,
				Nanos:        item.Cost.Nanos,
			},
		})
	}
	return protoItems
}

func extractCartItemsFromProto(items []*pb.OrderItem) []*pb.CartItem {
	var cartItems []*pb.CartItem
	for _, item := range items {
		cartItems = append(cartItems, item.Item)
	}
	return cartItems
}

// 订单过期时间（5分钟）
const orderExpirationDuration = 5 * time.Minute

// GetOrder 获取单个订单详情
func (s *OrderService) GetOrder(ctx context.Context, req *pb.GetOrderRequest) (*pb.GetOrderResponse, error) {
	order, err := s.repo.GetOrder(ctx, req.OrderId)
	if err != nil {
		return nil, status.Errorf(codes.NotFound, "order not found: %v", err)
	}

	resp := &pb.GetOrderResponse{
		OrderId:   order.OrderID,
		UserId:    order.UserID,
		Status:    order.Status,
		CreatedAt: order.CreatedAt.Unix(),
		TotalPrice: &pb.Money{
			CurrencyCode: order.TotalPrice.CurrencyCode,
			Units:        order.TotalPrice.Units,
			Nanos:        order.TotalPrice.Nanos,
		},
		Items: convertItemsToProto(order.Items),
	}

	// PENDING 订单添加过期时间
	if order.Status == model.OrderStatusPending {
		resp.ExpiresAt = order.CreatedAt.Add(orderExpirationDuration).Unix()
	}

	// 已发货订单获取 tracking_id
	if order.Status == model.OrderStatusShipped {
		if shipment, err := s.repo.GetShipment(ctx, req.OrderId); err == nil && shipment != nil {
			resp.TrackingId = shipment.TrackingID
		}
	}

	return resp, nil
}

// ListOrders 获取用户所有订单
func (s *OrderService) ListOrders(ctx context.Context, req *pb.ListOrdersRequest) (*pb.ListOrdersResponse, error) {
	orders, err := s.repo.ListOrdersByUser(ctx, req.UserId)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to list orders: %v", err)
	}

	var respOrders []*pb.GetOrderResponse
	for _, order := range orders {
		respOrder := &pb.GetOrderResponse{
			OrderId:   order.OrderID,
			UserId:    order.UserID,
			Status:    order.Status,
			CreatedAt: order.CreatedAt.Unix(),
			TotalPrice: &pb.Money{
				CurrencyCode: order.TotalPrice.CurrencyCode,
				Units:        order.TotalPrice.Units,
				Nanos:        order.TotalPrice.Nanos,
			},
			Items: convertItemsToProto(order.Items),
		}

		// PENDING 订单添加过期时间
		if order.Status == model.OrderStatusPending {
			respOrder.ExpiresAt = order.CreatedAt.Add(orderExpirationDuration).Unix()
		}

		// 已发货订单获取 tracking_id
		if order.Status == model.OrderStatusShipped {
			if shipment, err := s.repo.GetShipment(ctx, order.OrderID); err == nil && shipment != nil {
				respOrder.TrackingId = shipment.TrackingID
			}
		}

		respOrders = append(respOrders, respOrder)
	}

	return &pb.ListOrdersResponse{Orders: respOrders}, nil
}
