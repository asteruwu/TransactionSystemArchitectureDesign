package repository

import (
	"context"
	"fmt"
	"time"

	"github.com/GoogleCloudPlatform/microservices-demo/src/orderservice/pkg/model"
	"gorm.io/gorm"
)

type OrderRepo interface {
	InsertOrder(ctx context.Context, order *model.Order) error
	InsertOrdersBatch(ctx context.Context, orders []*model.Order) error
	GetOrder(ctx context.Context, orderID string) (*model.Order, error)
	GetOrdersByIDs(ctx context.Context, orderIDs []string) ([]*model.Order, error)
	UpdateOrderStatus(ctx context.Context, orderID string, status int32) error
	GetExpiredPendingOrders(ctx context.Context, delay time.Duration, limit int) ([]*model.Order, error)
	UpdateOrderStatusBatch(ctx context.Context, statuses map[string]int32) error
	InsertFailedOrder(ctx context.Context, failedOrder *model.FailedOrder) error
	UpdateOrderAndInsertShipment(ctx context.Context, orderID string, status int32, shipment *model.Shipment) error
	GetPaidOrders(ctx context.Context, delay time.Duration, limit int) ([]*model.Order, error)
	UpdateOrdersAndInsertShipmentsBatch(ctx context.Context, shipments []*model.Shipment) error
	ListOrdersByUser(ctx context.Context, userID string) ([]*model.Order, error)
	GetShipment(ctx context.Context, orderID string) (*model.Shipment, error)
}

type mysqlRepo struct {
	db *gorm.DB
}

func NewOrderRepo(db *gorm.DB) OrderRepo {
	return &mysqlRepo{db: db}
}

// [order_created Consumer] Batch插入失败，整批回滚，逐消息插入
func (r *mysqlRepo) InsertOrder(ctx context.Context, order *model.Order) error {
	return r.db.WithContext(ctx).Create(order).Error
}

// [order_created Consumer] 常规路径
func (r *mysqlRepo) InsertOrdersBatch(ctx context.Context, orders []*model.Order) error {
	return r.db.WithContext(ctx).Session(&gorm.Session{FullSaveAssociations: true}).CreateInBatches(orders, 100).Error
}

// 用于取消订单
func (r *mysqlRepo) GetOrder(ctx context.Context, orderID string) (*model.Order, error) {
	var order model.Order
	if err := r.db.WithContext(ctx).Preload("Items").Where("order_id = ?", orderID).First(&order).Error; err != nil {
		return nil, err
	}
	return &order, nil
}

// [Shipping Consumer] 批量获取订单
func (r *mysqlRepo) GetOrdersByIDs(ctx context.Context, orderIDs []string) ([]*model.Order, error) {
	if len(orderIDs) == 0 {
		return nil, nil
	}
	var orders []*model.Order
	err := r.db.WithContext(ctx).
		Preload("Items").
		Where("order_id IN ?", orderIDs).
		Find(&orders).Error
	return orders, err
}

// [order_status Consumer] Batch更新失败，整批回滚，逐消息更新
func (r *mysqlRepo) UpdateOrderStatus(ctx context.Context, orderID string, status int32) error {
	// 防回滚：只有新状态大于当前状态时才更新
	res := r.db.WithContext(ctx).Model(&model.Order{}).
		Where("order_id = ? AND status < ?", orderID, status).
		Update("status", status)

	if res.Error != nil {
		return res.Error
	}
	// 如果 RowsAffected == 0，说明状态已经是最新的（或者订单不存在），视为成功
	return nil
}

// [CleanupWorker] Cancelled or Paid?
func (r *mysqlRepo) GetExpiredPendingOrders(ctx context.Context, delay time.Duration, limit int) ([]*model.Order, error) {
	var orders []*model.Order
	threshold := time.Now().Add(-delay)
	// Status 0 = Pending
	err := r.db.WithContext(ctx).
		Preload("Items"). // Need Items for Restock
		Where("status = ? AND created_at < ?", 0, threshold).
		Limit(limit).
		Find(&orders).Error
	return orders, err
}

// [order_status Consumer] 常规路径
// SET status {CASE... THEN...}
func (r *mysqlRepo) UpdateOrderStatusBatch(ctx context.Context, statuses map[string]int32) error {
	if len(statuses) == 0 {
		return nil
	}

	caseStmt := "CASE order_id"
	ids := make([]interface{}, 0, len(statuses))
	params := make([]interface{}, 0, len(statuses)*2)

	for oid, status := range statuses {
		caseStmt += " WHEN ? THEN IF(status < ?, ?, status)"
		params = append(params, oid, status, status)
		ids = append(ids, oid)
	}
	caseStmt += " ELSE status END"

	query := fmt.Sprintf("UPDATE orders SET status = %s WHERE order_id IN ?", caseStmt)
	params = append(params, ids)

	return r.db.WithContext(ctx).Exec(query, params...).Error
}

// [DLQ Consumer] 逐消息插入
func (r *mysqlRepo) InsertFailedOrder(ctx context.Context, failedOrder *model.FailedOrder) error {
	return r.db.WithContext(ctx).Create(failedOrder).Error
}

// [Shippment Flusher / Shipping Recover] Batch处理失败，整批回滚，逐消息处理
func (r *mysqlRepo) UpdateOrderAndInsertShipment(ctx context.Context, orderID string, status int32, shipment *model.Shipment) error {
	return r.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		// 1. Update Order Status (带防回滚条件：order 必须存在且状态小于目标状态)
		res := tx.Model(&model.Order{}).Where("order_id = ? AND status < ?", orderID, status).Update("status", status)
		if res.Error != nil {
			return res.Error
		}
		if res.RowsAffected == 0 {
			// 检查订单是否存在
			var count int64
			tx.Model(&model.Order{}).Where("order_id = ?", orderID).Count(&count)
			if count == 0 {
				return fmt.Errorf("order not found: %s", orderID)
			}
			// 订单存在但状态已经是目标状态或更高，视为成功（幂等）
		}

		// 2. Insert Shipment
		if err := tx.Create(shipment).Error; err != nil {
			return err
		}

		return nil
	})
}

// [Shipping Recover] Shipped or Failed?
func (r *mysqlRepo) GetPaidOrders(ctx context.Context, delay time.Duration, limit int) ([]*model.Order, error) {
	var orders []*model.Order
	threshold := time.Now().Add(-delay)
	// Status 1 = PAID
	err := r.db.WithContext(ctx).
		Preload("Items").
		Where("status = ? AND created_at < ?", 1, threshold).
		Limit(limit).
		Find(&orders).Error
	return orders, err
}

// [Shippment Flusher / Shipping Recover] 常规路径
func (r *mysqlRepo) UpdateOrdersAndInsertShipmentsBatch(ctx context.Context, shipments []*model.Shipment) error {
	if len(shipments) == 0 {
		return nil
	}

	return r.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		// 1. 批量插入 Shipments
		if err := tx.CreateInBatches(shipments, 100).Error; err != nil {
			return err
		}

		// 2. 批量更新 Orders 状态
		caseStmt := "CASE order_id"
		ids := make([]interface{}, 0, len(shipments))
		params := make([]interface{}, 0, len(shipments)*2)

		for _, s := range shipments {
			caseStmt += " WHEN ? THEN ?"
			params = append(params, s.OrderID, s.Status)
			ids = append(ids, s.OrderID)
		}
		caseStmt += " ELSE status END"

		query := fmt.Sprintf("UPDATE orders SET status = %s WHERE order_id IN ?", caseStmt)
		params = append(params, ids)

		result := tx.Exec(query, params...)
		if result.Error != nil {
			return result.Error
		}
		// 检查是否所有订单都被更新（订单必须存在）
		if result.RowsAffected != int64(len(shipments)) {
			return fmt.Errorf("not all orders updated, expected %d, got %d (some orders may not exist yet)", len(shipments), result.RowsAffected)
		}
		return nil
	})
}

// [Frontend] 获取用户所有订单
func (r *mysqlRepo) ListOrdersByUser(ctx context.Context, userID string) ([]*model.Order, error) {
	var orders []*model.Order
	err := r.db.WithContext(ctx).
		Preload("Items").
		Where("user_id = ?", userID).
		Order("created_at DESC").
		Limit(50).
		Find(&orders).Error
	return orders, err
}

// [Frontend] 获取发货信息
func (r *mysqlRepo) GetShipment(ctx context.Context, orderID string) (*model.Shipment, error) {
	var shipment model.Shipment
	err := r.db.WithContext(ctx).Where("order_id = ?", orderID).First(&shipment).Error
	if err != nil {
		return nil, err
	}
	return &shipment, nil
}
