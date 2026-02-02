package model

type User struct {
	UserID   string `gorm:"primaryKey;type:varchar(64)"` // UUID
	Username string `gorm:"uniqueIndex;type:varchar(32);not null"`
	Password string `gorm:"type:varchar(128);not null"` // 存储 BCrypt 哈希值
}

func (User) TableName() string {
	return "users"
}
