package model

type Product struct {
	Id                   string `json:"id"`
	Name                 string `json:"name"`
	Description          string `json:"description"`
	Picture              string `json:"picture"`
	PriceUsdCurrencyCode string `json:"price_usd_currency_code"`
	PriceUsdUnits        int64  `json:"price_usd_units"`
	PriceUsdNanos        int32  `json:"price_usd_nanos"`
	Categories           string `json:"categories"`
	Stock                int32  `json:"stock"`
}

func (Product) TableName() string {
	return "products"
}
