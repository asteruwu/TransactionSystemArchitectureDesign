package model

import "time"

type FailedOrder struct {
	ID           int64     `gorm:"primaryKey;autoIncrement" json:"id"`
	OrderID      string    `gorm:"index" json:"order_id"`
	OriginalJSON string    `gorm:"type:text" json:"original_json"`
	ErrorReason  string    `gorm:"type:varchar(255)" json:"error_reason"`
	CreatedAt    time.Time `gorm:"autoCreateTime" json:"created_at"`
}

func (FailedOrder) TableName() string {
	return "failed_orders"
}
