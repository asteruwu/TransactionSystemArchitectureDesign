package repository

import (
	"context"
	"fmt"

	"github.com/sirupsen/logrus"

	pb "github.com/GoogleCloudPlatform/microservices-demo/src/productcatalogservice/genproto"
	"github.com/GoogleCloudPlatform/microservices-demo/src/productcatalogservice/model"

	"gorm.io/gorm"
)

type ProductRepository interface {
	ListProducts(ctx context.Context) ([]*model.Product, error)
	GetProduct(ctx context.Context, id string) (*model.Product, error)
	SearchProducts(ctx context.Context, query string) ([]*model.Product, error)
	ChargeProduct(ctx context.Context, req *pb.ChargeProductRequest) (bool, string)
	RestockProduct(ctx context.Context, req *pb.RestockProductRequest) error
}

type mysqlRepo struct {
	db *gorm.DB
}

func NewMysqlRepo(db *gorm.DB) ProductRepository {
	return &mysqlRepo{db: db}
}

func (r *mysqlRepo) ListProducts(ctx context.Context) ([]*model.Product, error) {
	var products []*model.Product
	if err := r.db.WithContext(ctx).Find(&products).Error; err != nil {
		return nil, fmt.Errorf("failed to list products: %w", err)
	}
	return products, nil
}

func (r *mysqlRepo) GetProduct(ctx context.Context, id string) (*model.Product, error) {
	var products []*model.Product
	if err := r.db.WithContext(ctx).Where("id = ?", id).First(&products).Error; err != nil {
		return nil, fmt.Errorf("failed to get product: %w", err)
	}
	return products[0], nil
}

func (r *mysqlRepo) SearchProducts(ctx context.Context, query string) ([]*model.Product, error) {
	var products []*model.Product

	likeQuery := fmt.Sprintf("%%%s%%", query)
	if err := r.db.WithContext(ctx).Where("name LIKE ? OR description LIKE ?", likeQuery, likeQuery).Find(&products).Error; err != nil {
		return nil, fmt.Errorf("failed to search products: %w", err)
	}
	return products, nil
}

func (r *mysqlRepo) ChargeProduct(ctx context.Context, req *pb.ChargeProductRequest) (bool, string) {
	logrus.Warnf("[ChargeProduct] DEPRECATED: Direct MySQL charging called. This should be handled by Redis Stream. Ignoring.")
	return false, "Deprecated: direct usage not allowed"
}

func (r *mysqlRepo) RestockProduct(ctx context.Context, req *pb.RestockProductRequest) error {
	logrus.Warnf("[RestockProduct] DEPRECATED: Direct MySQL restock called. This should be handled by Redis Stream. Ignoring.")
	return nil
}
