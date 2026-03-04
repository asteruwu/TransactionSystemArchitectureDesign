package main

import (
	"context"
	"fmt"
	"math"
	"math/rand"
	"net"
	"os"
	"os/signal"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/GoogleCloudPlatform/microservices-demo/src/orderservice/pkg/client"
	"github.com/GoogleCloudPlatform/microservices-demo/src/orderservice/pkg/model"
	"github.com/GoogleCloudPlatform/microservices-demo/src/orderservice/pkg/repository"
	"github.com/GoogleCloudPlatform/microservices-demo/src/orderservice/pkg/service"
	"github.com/GoogleCloudPlatform/microservices-demo/src/orderservice/pkg/worker"

	pb "github.com/GoogleCloudPlatform/microservices-demo/src/orderservice/genproto"
	rmq_client "github.com/apache/rocketmq-clients/golang/v5"
	"github.com/apache/rocketmq-clients/golang/v5/credentials"
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
		rocketmqAddr = "localhost:8081"
	}
	rocketmqAccessKey := os.Getenv("ROCKETMQ_ACCESS_KEY")
	rocketmqAccessSecret := os.Getenv("ROCKETMQ_ACCESS_SECRET")

	// RocketMQ v5 SDK 使用 endpoint（proxy 地址），支持主机名
	resolvedAddr := rocketmqAddr
	log.Infof("RocketMQ Endpoint: %s", resolvedAddr)

	p, err := rmq_client.NewProducer(
		&rmq_client.Config{
			Endpoint:      resolvedAddr,
			ConsumerGroup: "order_stream_producer_group",
			Credentials: &credentials.SessionCredentials{
				AccessKey:    rocketmqAccessKey,
				AccessSecret: rocketmqAccessSecret,
			},
		},
		rmq_client.WithTopics("order_status_events", "%DLQ%order_db_group", "%DLQ%order_status_group"),
	)
	if err != nil {
		log.Fatalf("Failed to create producer: %v", err)
	}

	if err = p.Start(); err != nil {
		log.Fatalf("Failed to start producer: %v", err)
	}
	defer p.GracefulStop()
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
	consumerWorker, err := worker.NewConsumerWorker(resolvedAddr, "order_db_group", rocketmqAccessKey, rocketmqAccessSecret, p, repo, shippingClient, log)
	if err != nil {
		log.Fatalf("Failed to init consumer: %v", err)
	}

	go consumerWorker.Start(ctx, wg, "orders")

	// 7. Start RocketMQ Status Consumer
	statusConsumerWorker, err := worker.NewConsumerWorker(resolvedAddr, "order_status_group", rocketmqAccessKey, rocketmqAccessSecret, p, repo, shippingClient, log)
	if err != nil {
		log.Fatalf("Failed to init status consumer: %v", err)
	}

	go statusConsumerWorker.Start(ctx, wg, "order_status_events")

	// 8. Start Order Cleanup Worker (SQL Polling / Reconciliation)
	cleanupWorker := worker.NewOrderCleanupWorker(repo, paymentClient, catalogClient, log)
	go cleanupWorker.Start(ctx, wg)

	// 9. Start DLQ Consumer (Dead Letter Queue Monitoring)
	dlqConsumer, err := worker.NewDLQConsumer(resolvedAddr, rocketmqAccessKey, rocketmqAccessSecret, repo)
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
	db.AutoMigrate(&model.Order{}, &model.OrderItem{}, &model.FailedOrder{}, &model.Shipment{}, &model.FailedCleanupLog{})
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
		sdktrace.WithSampler(sdktrace.ParentBased(newLoadAdaptiveSampler(ctx))),
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

type loadAdaptiveSampler struct {
	baseRate   float64
	goHigh     int
	minRate    float64
	cachedRate uint64
}

func newLoadAdaptiveSampler(ctx context.Context) *loadAdaptiveSampler {
	base := 0.1
	if v := os.Getenv("TRACE_SAMPLE_RATE"); v != "" {
		if r, err := strconv.ParseFloat(v, 64); err == nil && r >= 0 && r <= 1 {
			base = r
		} else {
			log.Warnf("invalid TRACE_SAMPLE_RATE=%q, using default 0.1", v)
		}
	}
	high := 5000
	if v := os.Getenv("TRACE_GOROUTINE_HIGH"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			high = n
		} else {
			log.Warnf("invalid TRACE_GOROUTINE_HIGH=%q, using default 5000", v)
		}
	}
	minR := 0.01
	if v := os.Getenv("TRACE_SAMPLE_MIN_RATE"); v != "" {
		if r, err := strconv.ParseFloat(v, 64); err == nil && r >= 0 && r <= 1 {
			minR = r
		} else {
			log.Warnf("invalid TRACE_SAMPLE_MIN_RATE=%q, using default 0.01", v)
		}
	}

	s := &loadAdaptiveSampler{baseRate: base, goHigh: high, minRate: minR}
	atomic.StoreUint64(&s.cachedRate, math.Float64bits(base))

	go func() {
		ticker := time.NewTicker(5 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				current := runtime.NumGoroutine()
				rate := s.baseRate
				if current >= s.goHigh {
					ratio := float64(current-s.goHigh) / float64(s.goHigh)
					if ratio > 1 {
						ratio = 1
					}
					rate = s.baseRate - (s.baseRate-s.minRate)*ratio
				}
				atomic.StoreUint64(&s.cachedRate, math.Float64bits(rate))
			}
		}
	}()

	log.Infof("LoadAdaptiveSampler initialized: base=%.2f, goHigh=%d, minRate=%.2f", base, high, minR)
	return s
}

func (s *loadAdaptiveSampler) ShouldSample(p sdktrace.SamplingParameters) sdktrace.SamplingResult {
	rate := math.Float64frombits(atomic.LoadUint64(&s.cachedRate))
	if rand.Float64() < rate {
		return sdktrace.SamplingResult{Decision: sdktrace.RecordAndSample}
	}
	return sdktrace.SamplingResult{Decision: sdktrace.Drop}
}

func (s *loadAdaptiveSampler) Description() string {
	return fmt.Sprintf("LoadAdaptiveSampler{base=%.2f,goHigh=%d}", s.baseRate, s.goHigh)
}
