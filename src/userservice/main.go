package main

import (
	"fmt"
	"net"
	"os"
	"os/signal"
	"syscall"
	"time"

	pb "github.com/GoogleCloudPlatform/microservices-demo/src/userservice/genproto"
	"github.com/GoogleCloudPlatform/microservices-demo/src/userservice/pkg/model"
	"github.com/GoogleCloudPlatform/microservices-demo/src/userservice/pkg/repo"
	"github.com/GoogleCloudPlatform/microservices-demo/src/userservice/pkg/rpc"
	"github.com/GoogleCloudPlatform/microservices-demo/src/userservice/pkg/service"

	"github.com/sirupsen/logrus"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"

	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
	"gorm.io/driver/mysql"
	"gorm.io/gorm"
)

const defaultPort = "50051"

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
	port := os.Getenv("PORT")
	if port == "" {
		port = defaultPort
	}

	mysqlAddr := os.Getenv("MYSQL_ADDR")
	if mysqlAddr == "" {
		mysqlAddr = "root:root_password@tcp(127.0.0.1:3307)/user_db"
		log.Info("Tried to connect to MySQL, but MYSQL_ADDR is not set. Using default address.")
	}

	dsn := mysqlAddr
	db, err := gorm.Open(mysql.Open(dsn), &gorm.Config{})
	if err != nil {
		log.Fatalf("failed to connect to mysql: %v", err)
	}
	log.Info("connected to mysql")

	// 自动迁移表结构
	db.AutoMigrate(&model.User{})

	// 2. 依赖注入
	userRepo := repo.NewUserRepository(db)
	userLogic := service.NewUserServiceLogic(userRepo)
	userHandler := &rpc.UserService{Logic: userLogic}

	// 3. 启动 gRPC Server
	lis, err := net.Listen("tcp", fmt.Sprintf(":%s", port))
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}

	srv := grpc.NewServer()
	pb.RegisterUserServiceServer(srv, userHandler)

	hsrv := health.NewServer()
	hsrv.SetServingStatus("", healthpb.HealthCheckResponse_SERVING)
	healthpb.RegisterHealthServer(srv, hsrv)
	// 启用反射，方便 grpcurl 调试
	reflection.Register(srv)

	log.Printf("UserService listening on port %s", port)
	if err := srv.Serve(lis); err != nil {
		log.Fatalf("failed to serve: %v", err)
	}

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM)

	<-sigCh
	log.Info("Gracefully shutting down...")

	srv.GracefulStop()
}
