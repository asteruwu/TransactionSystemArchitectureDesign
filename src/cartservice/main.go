package main

import (
	"context"
	"fmt"
	"log"
	"net"
	"os"
	"time"

	pb "github.com/GoogleCloudPlatform/microservices-demo/src/cartservice/genproto"
	"github.com/GoogleCloudPlatform/microservices-demo/src/cartservice/repository"

	"github.com/pkg/errors"
	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/status"
)

const (
	defaultPort = "7070"
	cartTimeout = 500 * time.Millisecond
)

// 定义我们的服务结构体
type cartService struct {
	pb.UnimplementedCartServiceServer                            // 必备：向前兼容
	repo                              repository.ICartRepository // 数据库客户端
	// 其他服务的地址
	productCatalogSvcAddr string
	productCatalogSvcConn *grpc.ClientConn
	productClient         pb.ProductCatalogServiceClient
}

type CartItemWithDetails struct {
	ProductID   string
	Quantity    int32
	Name        string
	Description string
}

// TODO: 1. 实现 AddItem (添加商品)
func (s *cartService) AddItem(ctx context.Context, req *pb.AddItemRequest) (*pb.Empty, error) {
	childCtx, cancel := context.WithTimeout(ctx, cartTimeout)
	defer cancel()
	if err := s.repo.AddItem(childCtx, req.UserId, req.Item); err != nil {
		if err == context.DeadlineExceeded {
			return nil, status.Error(codes.DeadlineExceeded, "[timeout]Failed to add item for user")
		}
		return nil, status.Error(codes.Internal, "[internal]Failed to add item for user")
	}
	// log.Printf("Adding item for user %s: %s", req.UserId, req.Item.ProductId)
	return &pb.Empty{}, nil
}

// TODO: 2. 实现 GetCart (获取购物车)
func (s *cartService) GetCart(ctx context.Context, req *pb.GetCartRequest) (*pb.Cart, error) {
	childCtx, cancel := context.WithTimeout(ctx, cartTimeout)
	defer cancel()
	cartItems, err := s.repo.GetCart(childCtx, req.UserId)
	if err != nil {
		if err == context.DeadlineExceeded {
			return nil, status.Error(codes.DeadlineExceeded, "[timeout]Failed to get cart for user")
		}
		return nil, status.Error(codes.Internal, "[internal]Failed to get cart for user")
	}
	cart := &pb.Cart{UserId: req.UserId, Items: cartItems}
	return cart, nil
}

// TODO: 3. 实现 EmptyCart (清空购物车)
func (s *cartService) EmptyCart(ctx context.Context, req *pb.EmptyCartRequest) (*pb.Empty, error) {
	childCtx, cancel := context.WithTimeout(ctx, cartTimeout)
	defer cancel()
	if err := s.repo.EmptyCart(childCtx, req.UserId); err != nil {
		if err == context.DeadlineExceeded {
			return nil, status.Error(codes.DeadlineExceeded, "[timeout]Failed to empty cart for user")
		}
		return nil, status.Error(codes.Internal, "[internal]Failed to empty cart for user")
	}
	return &pb.Empty{}, nil
}

// GetCartWithDetails 获取购物车商品列表，并在内部进行计算
func (s *cartService) GetCartWithDetails(ctx context.Context, req *pb.GetCartRequest) ([]*CartItemWithDetails, error) {
	items, err := s.repo.GetCart(ctx, req.UserId)
	if err != nil {
		return nil, status.Error(codes.Internal, "[internal]Failed to get cart for user")
	}

	g, groupCtx := errgroup.WithContext(ctx)
	results := make([]*CartItemWithDetails, len(items))
	for i, item := range items {
		index, value := i, item
		g.Go(func() error {
			product, err := s.productClient.GetProduct(groupCtx, &pb.GetProductRequest{Id: value.ProductId})
			if err != nil {
				return fmt.Errorf("failed to get product details: %+v", err)
			}
			results[index] = &CartItemWithDetails{
				ProductID:   value.ProductId,
				Quantity:    value.Quantity,
				Name:        product.Name,
				Description: product.Description,
			}
			return nil
		})
	}

	if err := g.Wait(); err != nil {
		return nil, fmt.Errorf("failed to get product details: %+v", err)
	}
	return results, nil
}

func main() {
	ctx := context.Background()
	// 创建 CartService 实例
	cr, err := repository.NewCartRedis()
	if err != nil {
		log.Fatalf("failed to create cart repository: %v", err)
		return
	}
	srv := NewCartService(cr)

	//  启动 gRPC 服务
	port := os.Getenv("PORT")
	if port == "" {
		port = defaultPort
	}
	lis, err := net.Listen("tcp", fmt.Sprintf(":%s", port))
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}

	// 创建远程调用productCatalogService的客户端
	mustMapEnv(&srv.productCatalogSvcAddr, "PRODUCT_CATALOG_SERVICE_ADDR")
	mustConnGRPC(ctx, &srv.productCatalogSvcConn, srv.productCatalogSvcAddr)

	srv.productClient = pb.NewProductCatalogServiceClient(srv.productCatalogSvcConn)

	// 创建gRPC服务器（cartService）
	grpcServer := grpc.NewServer(
		grpc.StatsHandler(otelgrpc.NewServerHandler()),
	)

	// 注册服务
	pb.RegisterCartServiceServer(grpcServer, srv)
	healthpb.RegisterHealthServer(grpcServer, health.NewServer())

	// log.Printf("Go CartService listening on port %s...", port)
	if err := grpcServer.Serve(lis); err != nil {
		log.Fatalf("failed to serve: %v", err)
	}
}

func NewCartService(repo repository.ICartRepository) *cartService {
	return &cartService{repo: repo}
}

func mustMapEnv(target *string, envKey string) {
	v := os.Getenv(envKey)
	if v == "" {
		panic(fmt.Sprintf("environment variable %q not set", envKey))
	}
	*target = v
}

func mustConnGRPC(ctx context.Context, conn **grpc.ClientConn, addr string) {
	var err error
	ctx, cancel := context.WithTimeout(ctx, time.Second*3)
	defer cancel()
	*conn, err = grpc.DialContext(ctx, addr,
		grpc.WithInsecure(),
		grpc.WithStatsHandler(otelgrpc.NewClientHandler()))
	if err != nil {
		panic(errors.Wrapf(err, "grpc: failed to connect %s", addr))
	}
}
