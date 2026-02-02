package worker

import (
	"context"
	"sync"
	"time"

	pb "github.com/GoogleCloudPlatform/microservices-demo/src/orderservice/genproto"
	"github.com/GoogleCloudPlatform/microservices-demo/src/orderservice/pkg/model"
	"github.com/GoogleCloudPlatform/microservices-demo/src/orderservice/pkg/repository"
	"github.com/sirupsen/logrus"
)

type ShippingRecoverWorker struct {
	repo           repository.OrderRepo
	shippingClient pb.ShippingServiceClient
	log            *logrus.Entry
	interval       time.Duration
}

// 构造 shipping recover worker
func NewShippingRecoverWorker(repo repository.OrderRepo, shippingClient pb.ShippingServiceClient, log *logrus.Logger) *ShippingRecoverWorker {
	return &ShippingRecoverWorker{
		repo:           repo,
		shippingClient: shippingClient,
		log:            log.WithField("worker", "ShippingRecoverWorker"),
		interval:       1 * time.Minute,
	}
}

// 启动 shipping recover worker
func (w *ShippingRecoverWorker) Start(ctx context.Context, wg *sync.WaitGroup) {
	wg.Add(1)
	defer wg.Done()

	w.log.Info("ShippingRecoverWorker started")
	ticker := time.NewTicker(w.interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			w.log.Info("ShippingRecoverWorker stopping...")
			return
		case <-ticker.C:
			// 处理已支付超时订单
			w.processPaidOrders(ctx)
		}
	}
}

func (w *ShippingRecoverWorker) processPaidOrders(ctx context.Context) {
	// 1. 获得已支付超时订单
	orders, err := w.repo.GetPaidOrders(ctx, 1*time.Minute, 50)
	if err != nil {
		w.log.Errorf("Failed to fetch paid orders: %v", err)
		return
	}

	if len(orders) == 0 {
		return
	}

	w.log.Infof("Found %d paid orders to check for shipping recovery", len(orders))

	var shipments []*model.Shipment
	var mu sync.Mutex
	var wg sync.WaitGroup

	sem := make(chan struct{}, 10)

	// 聚合
	for _, order := range orders {
		wg.Add(1)
		go func(o *model.Order) {
			defer wg.Done()
			sem <- struct{}{}
			defer func() { <-sem }()

			shipment := w.recoverOrderAndCollect(ctx, o)
			if shipment != nil {
				mu.Lock()
				shipments = append(shipments, shipment)
				mu.Unlock()
			}
		}(order)
	}
	wg.Wait()

	// 批量更新、插入记录
	if len(shipments) > 0 {
		w.log.Infof("Batch updating %d recovered orders", len(shipments))
		if err := w.repo.UpdateOrdersAndInsertShipmentsBatch(ctx, shipments); err != nil {
			// 降级处理
			w.log.Warnf("Batch update failed (%v), falling back to sequential", err)
			for _, s := range shipments {
				if err := w.repo.UpdateOrderAndInsertShipment(ctx, s.OrderID, s.Status, s); err != nil {
					w.log.Errorf("Failed to update shipment for %s: %v", s.OrderID, err)
				}
			}
		} else {
			w.log.Infof("Successfully recovered %d orders in batch", len(shipments))
		}
	}
}

func (w *ShippingRecoverWorker) recoverOrderAndCollect(ctx context.Context, order *model.Order) *model.Shipment {
	log := w.log.WithField("order_id", order.OrderID)
	log.Debug("Attempting to recover shipping for order")

	reqCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	// 检查是否已成功发货
	trackResp, err := w.shippingClient.GetTrackingId(reqCtx, &pb.GetTrackingIdRequest{OrderId: order.OrderID})
	if err == nil && trackResp.TrackingId != "" {
		log.Infof("Found existing tracking ID %s. Repairing local state.", trackResp.TrackingId)
		return &model.Shipment{
			OrderID:    order.OrderID,
			TrackingID: trackResp.TrackingId,
			Status:     model.OrderStatusShipped,
		}
	}

	cartItems := convertToCartItems(order.Items)
	address := &pb.Address{
		StreetAddress: order.ShippingAddress,
	}

	var shipResp *pb.ShipOrderResponse
	var shipErr error

	// 有限次重试发货
	for i := 0; i < 3; i++ {
		if i > 0 {
			time.Sleep(time.Duration(100*(1<<i)) * time.Millisecond)
		}
		shipResp, shipErr = w.shippingClient.ShipOrder(reqCtx, &pb.ShipOrderRequest{
			Address: address,
			Items:   cartItems,
			OrderId: order.OrderID,
		})
		if shipErr == nil {
			break
		}
	}

	if shipErr != nil {
		log.Errorf("Failed to ship order after retries: %v", shipErr)
		return &model.Shipment{
			OrderID:    order.OrderID,
			TrackingID: "",
			Status:     model.OrderStatusShipFailed,
			ErrorMsg:   shipErr.Error(),
		}
	}

	// Success
	return &model.Shipment{
		OrderID:    order.OrderID,
		TrackingID: shipResp.TrackingId,
		Status:     model.OrderStatusShipped,
	}
}

func convertToCartItems(items []model.OrderItem) []*pb.CartItem {
	var cartItems []*pb.CartItem
	for _, item := range items {
		cartItems = append(cartItems, &pb.CartItem{
			ProductId: item.ProductID,
			Quantity:  item.Quantity,
		})
	}
	return cartItems
}
