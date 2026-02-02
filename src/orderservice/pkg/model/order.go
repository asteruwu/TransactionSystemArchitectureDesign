package model

import (
	"time"
)

// Order Status Constants
const (
	OrderStatusPending    = 0
	OrderStatusPaid       = 1
	OrderStatusCancelled  = 2
	OrderStatusShipped    = 3
	OrderStatusShipFailed = 4
)

type Money struct {
	CurrencyCode string `gorm:"type:char(3);comment:ISO 4217 currency code" json:"currency_code"`
	Units        int64  `gorm:"type:bigint;comment:Whole units" json:"units"`
	Nanos        int32  `gorm:"type:int;comment:Nano units (10^-9)" json:"nanos"`
}

type Order struct {
	OrderID         string    `gorm:"primaryKey;type:varchar(64)" json:"order_id"`
	UserID          string    `gorm:"type:varchar(64);index" json:"user_id"`
	ShippingAddress string    `gorm:"type:text" json:"shipping_address"`
	TotalPrice      Money     `gorm:"embedded;embeddedPrefix:total_price_" json:"total_price"`
	Status          int32     `gorm:"type:int;index:idx_status_created_at,priority:1" json:"status"`
	CreatedAt       time.Time `gorm:"index:idx_status_created_at,priority:2" json:"created_at"`
	UpdatedAt       time.Time `json:"updated_at"`

	Items []OrderItem `gorm:"foreignKey:OrderID;references:OrderID" json:"items"`
}

type OrderItem struct {
	ID        uint   `gorm:"primaryKey"`
	OrderID   string `gorm:"type:varchar(64);index"`
	ProductID string `gorm:"type:varchar(64)" json:"product_id"`
	Quantity  int32  `gorm:"type:int" json:"quantity"`
	Cost      Money  `gorm:"embedded;embeddedPrefix:cost_" json:"cost"`
}

// Shipment Model
type Shipment struct {
	OrderID    string    `gorm:"primaryKey;type:varchar(64)" json:"order_id"`
	TrackingID string    `gorm:"type:varchar(128);not null" json:"tracking_id"`
	Status     int32     `gorm:"type:tinyint;not null" json:"status"` // 3=SHIPPED, 4=FAILED
	ErrorMsg   string    `gorm:"type:text" json:"error_msg"`
	CreatedAt  time.Time `json:"created_at"`
}

func (Shipment) TableName() string {
	return "shipments"
}

// ... existing message structs ...
type OrderMessage struct {
	OrderID    string      `json:"order_id"`
	UserID     string      `json:"user_id"`
	Address    interface{} `json:"address"`
	Items      []OrderItem `json:"items"`
	TotalPrice Money       `json:"total_price"`
	Status     int32       `json:"status"`
}

type OrderStatusEvent struct {
	OrderID string `json:"order_id"`
	Status  string `json:"status"`
}

func (Order) TableName() string {
	return "orders"
}
