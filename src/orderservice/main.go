package main

import (
	"context"
	"fmt"
	"net"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/GoogleCloudPlatform/microservices-demo/src/orderservice/pkg/client"
	"github.com/GoogleCloudPlatform/microservices-demo/src/orderservice/pkg/model"
	"github.com/GoogleCloudPlatform/microservices-demo/src/orderservice/pkg/repository"
	"github.com/GoogleCloudPlatform/microservices-demo/src/orderservice/pkg/service"
	"github.com/GoogleCloudPlatform/microservices-demo/src/orderservice/pkg/worker"

	pb "github.com/GoogleCloudPlatform/microservices-demo/src/orderservice/genproto"
	rocketmq "github.com/apache/rocketmq-client-go/v2"
	"github.com/apache/rocketmq-client-go/v2/producer"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"gorm.io/driver/mysql"
	"gorm.io/gorm"

	"github.com/uptrace/opentelemetry-go-extra/otelgorm"

	// Tracing
	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"go.opentelemetry.io/otel/propagation"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.17.0"

	// Metrics
	"go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetricgrpc"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/resource"
)

var log *logrus.Logger

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
	wg := &sync.WaitGroup{}

	// 1. Init Tracing & Metrics (Optional)
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

	repo := initDB()

	// Init RocketMQ Producer
	rocketmqAddr := os.Getenv("ROCKETMQ_NAMESERVER")
	if rocketmqAddr == "" {
		rocketmqAddr = "localhost:9876"
	}

	// RocketMQ Go 客户端不支持主机名，需要解析为 IP 地址
	resolvedAddr := resolveToIP(rocketmqAddr)
	log.Infof("RocketMQ NameServer: %s -> %s", rocketmqAddr, resolvedAddr)

	p, err := rocketmq.NewProducer(
		producer.WithNameServer([]string{resolvedAddr}),
		producer.WithGroupName("order_stream_producer_group"),
		producer.WithRetry(2),
	)
	if err != nil {
		log.Fatalf("Failed to create producer: %v", err)
	}

	// [新增] 必须显式启动 Producer
	err = p.Start()
	if err != nil {
		log.Fatalf("Failed to start producer: %v", err)
	}
	defer p.Shutdown()
	// 4. Init gRPC Clients (Needed for TimeoutWorker and Service)
	catalogAddr := os.Getenv("PRODUCT_CATALOG_SERVICE_ADDR")
	if catalogAddr == "" {
		catalogAddr = "productcatalogservice:3550"
	}
	catalogConn, _ := grpc.Dial(catalogAddr, grpc.WithInsecure())
	catalogClient := pb.NewProductCatalogServiceClient(catalogConn)

	paymentAddr := os.Getenv("PAYMENT_SERVICE_ADDR")
	if paymentAddr == "" {
		paymentAddr = "paymentservice:50051"
	}
	paymentConn, err := grpc.Dial(paymentAddr, grpc.WithInsecure())
	if err != nil {
		log.Warnf("failed to connect to paymentservice: %v", err)
	}
	// Wrap with Circuit Breaker and Timeout
	rawPaymentClient := pb.NewPaymentServiceClient(paymentConn)
	paymentClient := client.NewPaymentClientWrapper(rawPaymentClient, log)

	shippingAddr := os.Getenv("SHIPPING_SERVICE_ADDR")
	if shippingAddr == "" {
		shippingAddr = "shippingservice:50051"
	}
	shippingConn, err := grpc.Dial(shippingAddr, grpc.WithInsecure())
	if err != nil {
		log.Warnf("failed to connect to shippingservice: %v", err)
	}
	shippingClient := pb.NewShippingServiceClient(shippingConn)

	// 5. Start RocketMQ Consumer (订单创建 - 由 ProductCatalog Forwarder 转发)
	consumerWorker, err := worker.NewConsumerWorker([]string{resolvedAddr}, "order_db_group", p, repo, shippingClient, log)
	if err != nil {
		log.Fatalf("Failed to init consumer: %v", err)
	}

	go consumerWorker.Start(ctx, wg, "orders")

	// 7. Start RocketMQ Status Consumer
	statusConsumerWorker, err := worker.NewConsumerWorker([]string{resolvedAddr}, "order_status_group", p, repo, shippingClient, log)
	if err != nil {
		log.Fatalf("Failed to init status consumer: %v", err)
	}

	go statusConsumerWorker.Start(ctx, wg, "order_status_events")

	// 8. Start Order Cleanup Worker (SQL Polling / Reconciliation)
	cleanupWorker := worker.NewOrderCleanupWorker(repo, paymentClient, catalogClient, log)
	go cleanupWorker.Start(ctx, wg)

	// 9. Start DLQ Consumer (Dead Letter Queue Monitoring)
	// Monitor the default DLQ topic for our consumer group
	dlqConsumer, err := worker.NewDLQConsumer(resolvedAddr, repo)
	if err != nil {
		log.Errorf("Failed to init DLQ consumer: %v", err)
	} else {
		err = dlqConsumer.Start(ctx, wg)
		if err != nil {
			log.Errorf("Failed to start DLQ consumer: %v", err)
		} else {
			log.Info("DLQ Consumer started (monitoring dead letters)")
		}
	}

	// 10. Start Shipping Recover Worker (Compensating Transaction)
	shippingRecoverWorker := worker.NewShippingRecoverWorker(repo, shippingClient, log)
	go shippingRecoverWorker.Start(ctx, wg)

	// 11. Start gRPC Server
	orderSvc := service.NewOrderService(catalogClient, paymentClient, shippingClient, p, repo)

	port := os.Getenv("PORT")
	if port == "" {
		port = "50051"
	}
	lis, _ := net.Listen("tcp", ":"+port)

	otel.SetTextMapPropagator(
		propagation.NewCompositeTextMapPropagator(
			propagation.TraceContext{}, propagation.Baggage{}))

	srv := grpc.NewServer(
		grpc.StatsHandler(otelgrpc.NewServerHandler()))

	pb.RegisterOrderServiceServer(srv, orderSvc)

	// 注册 gRPC Health Service (K8s 健康检查需要)
	healthServer := health.NewServer()
	healthpb.RegisterHealthServer(srv, healthServer)
	healthServer.SetServingStatus("", healthpb.HealthCheckResponse_SERVING)

	log.Infof("OrderService started on :%s", port)
	go srv.Serve(lis)

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM)

	<-sigCh
	log.Info("Gracefully shutting down...")

	srv.GracefulStop()
	// Notify workers to stop
	cancel()
	// Wait for workers to cleanup
	wg.Wait()
}

func initDB() repository.OrderRepo {
	mysqlAddr := os.Getenv("MYSQL_ADDR")
	if mysqlAddr == "" {
		mysqlAddr = "root:root_password@tcp(127.0.0.1:3307)/product_db"
		log.Info("Tried to connect to MySQL, but MYSQL_ADDR is not set. Using default address.")
	}

	dsn := mysqlAddr
	if !strings.Contains(dsn, "parseTime=true") {
		if strings.Contains(dsn, "?") {
			dsn += "&parseTime=true"
		} else {
			dsn += "?parseTime=true"
		}
	}

	db, err := gorm.Open(mysql.Open(dsn), &gorm.Config{})
	if err != nil {
		log.Fatalf("failed to connect to mysql: %v", err)
	}

	// 监控 sql 语句执行时间
	if err := db.Use(otelgorm.NewPlugin()); err != nil {
		log.Fatalf("failed to initialize otelgorm plugin: %v", err)
	}

	// Migrate both Order tables and FailedOrder table, and Shipment (New)
	db.AutoMigrate(&model.Order{}, &model.OrderItem{}, &model.FailedOrder{}, &model.Shipment{})
	repo := repository.NewOrderRepo(db)
	log.Info("connected to mysql")

	return repo

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
			semconv.ServiceNameKey.String("orderservice"),
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
		resource.WithAttributes(semconv.ServiceNameKey.String("orderservice")),
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
	*conn, err = grpc.NewClient(addr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithStatsHandler(otelgrpc.NewClientHandler()))
	if err != nil {
		panic(errors.Wrapf(err, "grpc: failed to connect %s", addr))
	}
}

// resolveToIP 将 hostname:port 格式解析为 ip:port 格式
// RocketMQ Go 客户端不支持主机名，需要先进行 DNS 解析
func resolveToIP(addr string) string {
	host, port, err := net.SplitHostPort(addr)
	if err != nil {
		return addr // 无法解析则原样返回
	}

	// 检查是否已经是 IP 地址
	if ip := net.ParseIP(host); ip != nil {
		return addr // 已经是 IP，直接返回
	}

	// DNS 解析主机名
	ips, err := net.LookupIP(host)
	if err != nil || len(ips) == 0 {
		return addr // 解析失败则原样返回
	}

	// 优先使用 IPv4 地址
	for _, ip := range ips {
		if ip4 := ip.To4(); ip4 != nil {
			return net.JoinHostPort(ip4.String(), port)
		}
	}

	// 没有 IPv4 则使用第一个 IP
	return net.JoinHostPort(ips[0].String(), port)
}
