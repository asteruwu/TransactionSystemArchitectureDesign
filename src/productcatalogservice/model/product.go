package model

type Product struct {
	Id          string  `json:"id"`
	Name        string  `json:"name"`
	Description string  `json:"description"`
	Picture     string  `json:"picture"`
	PriceUsd    float64 `json:"price_usd"`
	Categories  string  `json:"categories"`
	Stock       int32   `json:"stock"`
}

func (Product) TableName() string {
	return "products"
}
