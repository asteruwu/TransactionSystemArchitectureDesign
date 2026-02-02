package worker

import (
	"context"
	"sync"
	"time"

	"github.com/GoogleCloudPlatform/microservices-demo/src/orderservice/pkg/model"
	"github.com/GoogleCloudPlatform/microservices-demo/src/orderservice/pkg/repository"
	"github.com/sirupsen/logrus"
)

type ShipmentFlusher struct {
	repo          repository.OrderRepo
	buffer        chan *model.Shipment
	bufferSize    int
	flushInterval time.Duration
	log           *logrus.Entry
	wg            *sync.WaitGroup
}

// 构造 shipment flusher
func NewShipmentFlusher(repo repository.OrderRepo, log *logrus.Logger) *ShipmentFlusher {
	return &ShipmentFlusher{
		repo:          repo,
		buffer:        make(chan *model.Shipment, 1000),
		bufferSize:    50,
		flushInterval: 100 * time.Millisecond,
		log:           log.WithField("worker", "ShipmentFlusher"),
	}
}

// 启动 shipment flusher
func (f *ShipmentFlusher) Start(ctx context.Context, wg *sync.WaitGroup) {
	f.wg = wg
	wg.Add(1)
	go func() {
		defer wg.Done()
		f.run(ctx)
	}()
}

func (f *ShipmentFlusher) run(ctx context.Context) {
	f.log.Info("ShipmentFlusher started")
	ticker := time.NewTicker(f.flushInterval)
	defer ticker.Stop()

	batch := make([]*model.Shipment, 0, f.bufferSize)

	flush := func() {
		if len(batch) > 0 {
			f.flushBatch(batch)
			batch = make([]*model.Shipment, 0, f.bufferSize)
		}
	}

	// 消费
	for {
		select {
		// 内存中聚合，溢出刷盘
		case s := <-f.buffer:
			batch = append(batch, s)
			if len(batch) >= f.bufferSize {
				flush()
			}
		// 定时刷盘
		case <-ticker.C:
			flush()
		case <-ctx.Done():
			f.log.Info("ShipmentFlusher stopping, flushing remaining items...")
			close(f.buffer)
			for s := range f.buffer {
				batch = append(batch, s)
			}
			flush()
			return
		}
	}
}

// 生产者
func (f *ShipmentFlusher) Push(s *model.Shipment) {
	select {
	case f.buffer <- s:
		// success
	default:
		f.log.Warn("Shipment buffer full, dropping shipment update (will be recovered later)")
	}
}

// 带超时机制批量更新插入
func (f *ShipmentFlusher) flushBatch(batch []*model.Shipment) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	f.log.Debugf("Flushing batch of %d shipments", len(batch))
	err := f.repo.UpdateOrdersAndInsertShipmentsBatch(ctx, batch)
	if err != nil {
		f.log.Errorf("Failed to flush shipments batch: %v", err)
	}
}
