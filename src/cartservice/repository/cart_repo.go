package repository

import (
	"context"

	pb "github.com/GoogleCloudPlatform/microservices-demo/src/cartservice/genproto"
)

// ICartRepository 定义了操作数据的标准行为
type ICartRepository interface {
	AddItem(ctx context.Context, userID string, item *pb.CartItem) error
	GetCart(ctx context.Context, userID string) ([]*pb.CartItem, error)
	EmptyCart(ctx context.Context, userID string) error
}
