package client

import (
	"context"
	"time"

	pb "github.com/GoogleCloudPlatform/microservices-demo/src/orderservice/genproto"
	"github.com/sirupsen/logrus"
	"github.com/sony/gobreaker"
	"google.golang.org/grpc"
)

type PaymentClientWrapper struct {
	client  pb.PaymentServiceClient
	cb      *gobreaker.CircuitBreaker
	timeout time.Duration
}

// 外嵌熔断器
func NewPaymentClientWrapper(client pb.PaymentServiceClient, log *logrus.Logger) *PaymentClientWrapper {
	st := gobreaker.Settings{
		Name:        "PaymentService",
		MaxRequests: 5,
		Interval:    10 * time.Second,
		Timeout:     30 * time.Second,
		ReadyToTrip: func(counts gobreaker.Counts) bool {
			return counts.Requests >= 5 && float64(counts.TotalFailures)/float64(counts.Requests) >= 0.5
		},
		OnStateChange: func(name string, from gobreaker.State, to gobreaker.State) {
			log.Warnf("CircuitBreaker[%s] state changed from %s to %s", name, from, to)
		},
	}

	return &PaymentClientWrapper{
		client:  client,
		cb:      gobreaker.NewCircuitBreaker(st),
		timeout: 3 * time.Second,
	}
}

func (w *PaymentClientWrapper) Charge(ctx context.Context, in *pb.ChargeRequest, opts ...grpc.CallOption) (*pb.ChargeResponse, error) {
	// 1. 设置超时
	ctx, cancel := context.WithTimeout(ctx, w.timeout)
	defer cancel()

	// 2. 熔断执行
	res, err := w.cb.Execute(func() (interface{}, error) {
		return w.client.Charge(ctx, in, opts...)
	})

	if err != nil {
		return nil, err
	}
	return res.(*pb.ChargeResponse), nil
}

func (w *PaymentClientWrapper) GetCharge(ctx context.Context, in *pb.GetChargeRequest, opts ...grpc.CallOption) (*pb.ChargeResponse, error) {
	ctx, cancel := context.WithTimeout(ctx, w.timeout)
	defer cancel()

	// 熔断执行
	res, err := w.cb.Execute(func() (interface{}, error) {
		return w.client.GetCharge(ctx, in, opts...)
	})

	if err != nil {
		return nil, err
	}
	return res.(*pb.ChargeResponse), nil
}
