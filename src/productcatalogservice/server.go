// Copyright 2018 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package main

import (
	"context"
	"flag"
	"fmt"
	"net"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/GoogleCloudPlatform/microservices-demo/src/productcatalogservice/model"
	"github.com/GoogleCloudPlatform/microservices-demo/src/productcatalogservice/repository"
	"github.com/redis/go-redis/v9"

	pb "github.com/GoogleCloudPlatform/microservices-demo/src/productcatalogservice/genproto"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"

	"cloud.google.com/go/profiler"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetricgrpc"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"go.opentelemetry.io/otel/propagation"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/reflection"
	"google.golang.org/grpc/status"

	"gorm.io/driver/mysql"
	"gorm.io/gorm"

	redisotel "github.com/redis/go-redis/extra/redisotel/v9"
	"github.com/uptrace/opentelemetry-go-extra/otelgorm"
	"go.opentelemetry.io/otel/sdk/resource"
	semconv "go.opentelemetry.io/otel/semconv/v1.17.0"
)

var (
	log          *logrus.Logger
	extraLatency time.Duration

	port = "3550"
)

type productCatalogService struct {
	pb.UnimplementedProductCatalogServiceServer
	repo repository.ProductRepository
}

func init() {
	log = logrus.New()
	log.Formatter = &logrus.JSONFormatter{
		FieldMap: logrus.FieldMap{
			logrus.FieldKeyTime:  "timestamp",
			logrus.FieldKeyLevel: "severity",
			logrus.FieldKeyMsg:   "message",
		},
		TimestampFormat: time.RFC3339Nano,
	}
	log.Out = os.Stdout
}

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var wg sync.WaitGroup

	if os.Getenv("ENABLE_TRACING") == "1" {
		tp, err := initTracing(ctx)
		if err != nil {
			log.Warnf("warn: failed to start tracer: %+v", err)
		} else {
			defer func() {
				if err := tp.Shutdown(context.Background()); err != nil {
					log.Errorf("Error shutting down tracer provider: %v", err)
				}
			}()
		}

		mp, err := initMetrics(ctx)
		if err != nil {
			log.Warnf("warn: failed to start metric provider: %+v", err)
		} else {
			defer func() {
				if err := mp.Shutdown(context.Background()); err != nil {
					log.Errorf("Error shutting down metric provider: %v", err)
				}
			}()
		}
	}

	if os.Getenv("DISABLE_PROFILER") == "" {
		log.Info("Profiling enabled.")
		go initProfiling("productcatalogservice", "1.0.0")
	} else {
		log.Info("Profiling disabled.")
	}

	flag.Parse()

	// set injected latency
	if s := os.Getenv("EXTRA_LATENCY"); s != "" {
		v, err := time.ParseDuration(s)
		if err != nil {
			log.Fatalf("failed to parse EXTRA_LATENCY (%s) as time.Duration: %+v", v, err)
		}
		extraLatency = v
		log.Infof("extra latency enabled (duration: %v)", extraLatency)
	} else {
		extraLatency = time.Duration(0)
	}

	if os.Getenv("PORT") != "" {
		port = os.Getenv("PORT")
	}

	log.Infof("starting grpc server at :%s", port)

	_, srv := run(port, ctx, &wg)

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM)

	<-sigCh
	log.Info("Gracefully shutting down...")

	srv.GracefulStop()
	cancel()
	wg.Wait()

}

func run(port string, ctx context.Context, wg *sync.WaitGroup) (string, *grpc.Server) {
	listener, err := net.Listen("tcp", fmt.Sprintf(":%s", port))
	if err != nil {
		log.Fatal(err)
	}

	// Propagate trace context
	otel.SetTextMapPropagator(
		propagation.NewCompositeTextMapPropagator(
			propagation.TraceContext{}, propagation.Baggage{}))

	srv := grpc.NewServer(
		grpc.StatsHandler(otelgrpc.NewServerHandler()))

	svc := &productCatalogService{repo: initDB(ctx, wg)}

	pb.RegisterProductCatalogServiceServer(srv, svc)
	hsrv := health.NewServer()
	hsrv.SetServingStatus("", healthpb.HealthCheckResponse_SERVING)
	healthpb.RegisterHealthServer(srv, hsrv)
	reflection.Register(srv)

	go srv.Serve(listener)

	return listener.Addr().String(), srv
}

func initTracing(ctx context.Context) (*sdktrace.TracerProvider, error) {
	var (
		collectorAddr string
		collectorConn *grpc.ClientConn
	)

	mustMapEnv(&collectorAddr, "COLLECTOR_SERVICE_ADDR")
	mustConnGRPC(ctx, &collectorConn, collectorAddr)

	exporter, err := otlptracegrpc.New(
		ctx,
		otlptracegrpc.WithGRPCConn(collectorConn))
	if err != nil {
		log.Warnf("warn: Failed to create trace exporter: %v", err)
	}

	res, err := resource.New(ctx,
		resource.WithAttributes(
			// 核心：在 Jaeger 里显示的服务名
			semconv.ServiceNameKey.String("productcatalogservice"),
			semconv.ServiceVersionKey.String("1.0.0"),
			semconv.DeploymentEnvironmentKey.String("production"),
		),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create resource: %w", err)
	}

	tp := sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(exporter),
		sdktrace.WithSampler(sdktrace.ParentBased(sdktrace.TraceIDRatioBased(0.1))),
		sdktrace.WithResource(res),
	)
	otel.SetTracerProvider(tp)
	return tp, err
}

func initMetrics(ctx context.Context) (*sdkmetric.MeterProvider, error) {
	var (
		collectorAddr string
		collectorConn *grpc.ClientConn
	)

	mustMapEnv(&collectorAddr, "COLLECTOR_SERVICE_ADDR")
	mustConnGRPC(ctx, &collectorConn, collectorAddr)

	exporter, err := otlpmetricgrpc.New(
		ctx,
		otlpmetricgrpc.WithInsecure(),
		otlpmetricgrpc.WithEndpoint(collectorAddr),
	)
	if err != nil {
		log.Warnf("warn: Failed to create metric exporter: %v", err)
	}

	reader := sdkmetric.NewPeriodicReader(exporter, sdkmetric.WithInterval(15*time.Second))
	res, err := resource.New(ctx,
		resource.WithAttributes(semconv.ServiceNameKey.String("productcatalogservice")),
	)
	if err != nil {
		log.Warnf("warn: Failed to create resource: %v", err)
	}
	mp := sdkmetric.NewMeterProvider(
		sdkmetric.WithReader(reader),
		sdkmetric.WithResource(res),
	)
	otel.SetMeterProvider(mp)
	return mp, nil
}

func initDB(ctx context.Context, wg *sync.WaitGroup) repository.ProductRepository {
	mysqlAddr := os.Getenv("MYSQL_ADDR")
	if mysqlAddr == "" {
		mysqlAddr = "root:root_password@tcp(127.0.0.1:3307)/product_db"
		log.Info("Tried to connect to MySQL, but MYSQL_ADDR is not set. Using default address.")
	}

	dsn := mysqlAddr
	db, err := gorm.Open(mysql.Open(dsn), &gorm.Config{})
	if err != nil {
		log.Fatalf("failed to connect to mysql: %v", err)
	}
	log.Info("connected to mysql")
	baserepo := repository.NewMysqlRepo(db)

	if err := db.Use(otelgorm.NewPlugin()); err != nil {
		log.Fatalf("failed to initialize otelgorm plugin: %v", err)
	}

	var rdb *redis.Client

	sentinelAddrs := os.Getenv("REDIS_SENTINEL_ADDRS")

	if sentinelAddrs != "" {
		// [模式 A] 哨兵模式 (生产环境/K8s)
		log.Infof("Initializing Redis in Sentinel Mode. Sentinels: %s", sentinelAddrs)

		rdb = redis.NewFailoverClient(&redis.FailoverOptions{

			MasterName:    "mymaster",
			SentinelAddrs: strings.Split(sentinelAddrs, ","),
			DB:            0,
		})
	} else {
		// [模式 B] 单机模式 (本地开发/旧环境)
		redisAddr := os.Getenv("REDIS_ADDR")
		if redisAddr == "" {
			redisAddr = "localhost:6380"
		}
		log.Infof("Initializing Redis in Single Node Mode. Addr: %s", redisAddr)

		rdb = redis.NewClient(&redis.Options{
			Addr: redisAddr,
		})
	}

	if err := redisotel.InstrumentTracing(rdb); err != nil {
		panic(err)
	}

	pingCtx, cancel := context.WithTimeout(context.Background(), time.Second*3)
	defer cancel()
	if err := rdb.Ping(pingCtx).Err(); err != nil {
		log.Warnf("failed to connect to redis: %v", err)
		return baserepo
	}
	log.Info("connected to redis")

	repo := repository.NewCachedRepo(baserepo, rdb, db, log, ctx, wg)

	return repo
}

func initProfiling(service, version string) {
	for i := 1; i <= 3; i++ {
		if err := profiler.Start(profiler.Config{
			Service:        service,
			ServiceVersion: version,
			// ProjectID must be set if not running on GCP.
			// ProjectID: "my-project",
		}); err != nil {
			log.Warnf("failed to start profiler: %+v", err)
		} else {
			log.Info("started Stackdriver profiler")
			return
		}
		d := time.Second * 10 * time.Duration(i)
		log.Infof("sleeping %v to retry initializing Stackdriver profiler", d)
		time.Sleep(d)
	}
	log.Warn("could not initialize Stackdriver profiler after retrying, giving up")
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
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithUnaryInterceptor(otelgrpc.UnaryClientInterceptor()),
		grpc.WithStreamInterceptor(otelgrpc.StreamClientInterceptor()))
	if err != nil {
		panic(errors.Wrapf(err, "grpc: failed to connect %s", addr))
	}
}

func (s *productCatalogService) ListProducts(ctx context.Context, req *pb.Empty) (*pb.ListProductsResponse, error) {
	products, err := s.repo.ListProducts(ctx)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to list products: %v", err)
	}

	var productList []*pb.Product
	for _, product := range products {
		productList = append(productList, modelToProto(product))
	}

	return &pb.ListProductsResponse{Products: productList}, nil
}

func (s *productCatalogService) GetProduct(ctx context.Context, req *pb.GetProductRequest) (*pb.Product, error) {
	product, err := s.repo.GetProduct(ctx, req.Id)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to get product: %v", err)
	}

	return modelToProto(product), nil
}

func (s *productCatalogService) SearchProducts(ctx context.Context, req *pb.SearchProductsRequest) (*pb.SearchProductsResponse, error) {
	products, err := s.repo.SearchProducts(ctx, req.Query)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to search products: %v", err)
	}
	var productList []*pb.Product
	for _, product := range products {
		productList = append(productList, modelToProto(product))
	}

	return &pb.SearchProductsResponse{Results: productList}, nil
}

func (s *productCatalogService) ChargeProduct(ctx context.Context, req *pb.ChargeProductRequest) (*pb.ChargeProductResponse, error) {
	result, message := s.repo.ChargeProduct(ctx, req)
	if !result {
		if message == "stock is not enough" {
			return &pb.ChargeProductResponse{Success: false, Message: message}, status.Errorf(codes.OutOfRange, "ResourceExhausted: stock is not enough")
		}
		return &pb.ChargeProductResponse{Success: false, Message: message}, status.Errorf(codes.Internal, "failed to charge product: %v", message)
	}
	return &pb.ChargeProductResponse{Success: result, Message: message}, nil
}

func (s *productCatalogService) RestockProduct(ctx context.Context, req *pb.RestockProductRequest) (*pb.Empty, error) {
	err := s.repo.RestockProduct(ctx, req)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to restock product: %v", err)
	}
	return &pb.Empty{}, nil
}

func modelToProto(p *model.Product) *pb.Product {
	units := int64(p.PriceUsd)
	nanos := int32((p.PriceUsd - float64(units)) * 1e9)

	categories := strings.Split(p.Categories, ",")

	return &pb.Product{
		Id:          p.Id,
		Name:        p.Name,
		Description: p.Description,
		Picture:     p.Picture,
		PriceUsd: &pb.Money{
			CurrencyCode: "USD",
			Units:        units,
			Nanos:        nanos,
		},
		Categories: categories,
		Stock:      p.Stock,
	}
}
