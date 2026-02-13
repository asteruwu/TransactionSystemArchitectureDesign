package worker

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	pb "github.com/GoogleCloudPlatform/microservices-demo/src/orderservice/genproto"
	"github.com/GoogleCloudPlatform/microservices-demo/src/orderservice/pkg/model"
	"github.com/GoogleCloudPlatform/microservices-demo/src/orderservice/pkg/repository"
	"github.com/sirupsen/logrus"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type OrderCleanupWorker struct {
	repo          repository.OrderRepo
	logger        *logrus.Logger
	paymentClient pb.PaymentServiceClient
	catalogClient pb.ProductCatalogServiceClient

	expiredFoundTotal    uint64
	rollbackSuccessTotal uint64
	rollbackFailTotal    uint64
	fixPaidStatusTotal   uint64
}

func NewOrderCleanupWorker(
	repo repository.OrderRepo,
	paymentClient pb.PaymentServiceClient,
	catalogClient pb.ProductCatalogServiceClient,
	log *logrus.Logger,
) *OrderCleanupWorker {
	w := &OrderCleanupWorker{
		repo:          repo,
		logger:        log,
		paymentClient: paymentClient,
		catalogClient: catalogClient,
	}
	w.registerMetrics()

	return w
}

func (w *OrderCleanupWorker) registerMetrics() {
	meter := otel.GetMeterProvider().Meter("orderservice.cleanup")
	// 1. 清理任务执行统计
	meter.Int64ObservableGauge("app_cleanup_job_total",
		metric.WithInt64Callback(func(_ context.Context, obs metric.Int64Observer) error {
			obs.Observe(int64(atomic.LoadUint64(&w.expiredFoundTotal)),
				metric.WithAttributes(attribute.String("action", "scan_expired")))
			obs.Observe(int64(atomic.LoadUint64(&w.rollbackSuccessTotal)),
				metric.WithAttributes(attribute.String("action", "rollback_stock"), attribute.String("result", "success")))
			obs.Observe(int64(atomic.LoadUint64(&w.rollbackFailTotal)),
				metric.WithAttributes(attribute.String("action", "rollback_stock"), attribute.String("result", "failed")))
			obs.Observe(int64(atomic.LoadUint64(&w.fixPaidStatusTotal)),
				metric.WithAttributes(attribute.String("action", "fix_paid_status")))
			return nil
		}),
	)
}

func (w *OrderCleanupWorker) Start(ctx context.Context, wg *sync.WaitGroup) {
	wg.Add(1)
	defer wg.Done()

	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()

	w.logger.Info("[CleanupWorker] Started polling for expired pending orders (every 30s)")

	for {
		select {
		case <-ctx.Done():
			w.logger.Info("[CleanupWorker] Stopping...")
			return
		case <-ticker.C:
			w.processExpiredOrders(ctx)
		}
	}
}

func (w *OrderCleanupWorker) processExpiredOrders(ctx context.Context) {
	// 1. 获得超时 pending 订单
	orders, err := w.repo.GetExpiredPendingOrders(ctx, 5*time.Minute, 50)
	if err != nil {
		w.logger.Errorf("[CleanupWorker] Failed to fetch expired orders: %v", err)
		return
	}

	if len(orders) == 0 {
		return
	}

	w.logger.Infof("[CleanupWorker] Found %d expired pending orders. Starting reconciliation...", len(orders))
	atomic.AddUint64(&w.expiredFoundTotal, uint64(len(orders)))

	// 2. 并发查询支付状态，分类
	var mu sync.Mutex
	var wg sync.WaitGroup
	paidOrders := make([]string, 0)         // 已支付，需修复状态
	unpaidOrders := make([]*model.Order, 0) // 未支付，需回滚

	sem := make(chan struct{}, 10)

	for _, order := range orders {
		wg.Add(1)
		go func(o *model.Order) {
			defer wg.Done()
			sem <- struct{}{}
			defer func() { <-sem }()

			chargeResp, err := w.paymentClient.GetCharge(ctx, &pb.GetChargeRequest{OrderId: o.OrderID})

			mu.Lock()
			defer mu.Unlock()

			if err != nil {
				if status.Code(err) == codes.NotFound {
					w.logger.Infof("[CleanupWorker] Order %s confirmed NOT PAID (NotFound)", o.OrderID)
					unpaidOrders = append(unpaidOrders, o)
				} else {
					w.logger.Warnf("[CleanupWorker] GetCharge for %s returned error: %v. Skipping to avoid false rollback.", o.OrderID, err)
				}
				return
			}

			// 有响应，检查交易流水号
			if chargeResp.GetTransactionId() != "" {
				paidOrders = append(paidOrders, o.OrderID)
			} else {
				// 响应成功但无流水号，视为"确定未支付"
				w.logger.Infof("[CleanupWorker] Order %s confirmed NOT PAID (empty transactionId)", o.OrderID)
				unpaidOrders = append(unpaidOrders, o)
			}
		}(order)
	}
	wg.Wait()

	// 3. 批量更新已支付订单状态为 PAID
	if len(paidOrders) > 0 {
		w.logger.Infof("[CleanupWorker] Fixing %d orders to PAID status", len(paidOrders))
		atomic.AddUint64(&w.fixPaidStatusTotal, uint64(len(paidOrders)))
		statusMap := make(map[string]int32)
		for _, oid := range paidOrders {
			statusMap[oid] = 1 // PAID
		}
		if err := w.repo.UpdateOrderStatusBatch(ctx, statusMap); err != nil {
			// 降级处理
			w.logger.Warnf("[CleanupWorker] Batch update PAID failed (%v), falling back to sequential", err)
			for _, oid := range paidOrders {
				if err := w.repo.UpdateOrderStatus(ctx, oid, 1); err != nil {
					w.logger.Errorf("[CleanupWorker] Failed to update PAID for %s: %v", oid, err)
				}
			}
		}
	}

	// 4. 逐个回滚未支付订单库存，收集成功的
	cancelledOrders := make([]string, 0, len(unpaidOrders))
	for _, order := range unpaidOrders {
		restockReq := &pb.RestockProductRequest{
			OrderId: order.OrderID,
			Items:   convertItemsToProto(order.Items),
		}

		_, err := w.catalogClient.RestockProduct(ctx, restockReq)
		if err != nil {
			w.logger.Errorf("[CleanupWorker] Restock failed for %s: %v. Will retry next tick.", order.OrderID, err)
			atomic.AddUint64(&w.rollbackFailTotal, 1)
			continue
		}
		atomic.AddUint64(&w.rollbackSuccessTotal, 1)
		cancelledOrders = append(cancelledOrders, order.OrderID)
	}

	// 5. 批量更新回滚成功的订单状态为 CANCELLED
	if len(cancelledOrders) > 0 {
		w.logger.Infof("[CleanupWorker] Cancelling %d orders after restock", len(cancelledOrders))
		statusMap := make(map[string]int32)
		for _, oid := range cancelledOrders {
			statusMap[oid] = 2 // CANCELLED
		}
		if err := w.repo.UpdateOrderStatusBatch(ctx, statusMap); err != nil {
			// 降级处理
			w.logger.Warnf("[CleanupWorker] Batch update CANCELLED failed (%v), falling back to sequential", err)
			for _, oid := range cancelledOrders {
				if err := w.repo.UpdateOrderStatus(ctx, oid, 2); err != nil {
					w.logger.Errorf("[CleanupWorker] Failed to update CANCELLED for %s: %v", oid, err)
				}
			}
		}
	}
}

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
