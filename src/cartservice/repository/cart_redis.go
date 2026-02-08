package repository

import (
	"context"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	pb "github.com/GoogleCloudPlatform/microservices-demo/src/cartservice/genproto"

	redis "github.com/redis/go-redis/v9"
)

type CartRedis struct {
	rdb *redis.Client
}

func NewCartRedis() (*CartRedis, error) {
	var rdb *redis.Client

	// 1. 获取通用的配置
	// 数据库索引 (购物车通常用 DB 1，与商品隔离)
	dbIndex := 0
	if dbStr := os.Getenv("REDIS_DB"); dbStr != "" {
		fmt.Sscanf(dbStr, "%d", &dbIndex)
	}

	sentinelAddrs := os.Getenv("REDIS_SENTINEL_ADDRS")

	if sentinelAddrs != "" {
		// [模式 A] 哨兵高可用模式 (K8s 生产环境)
		// 格式: "redis:26379,redis-sentinel:26379"
		masterName := os.Getenv("REDIS_MASTER_NAME")
		if masterName == "" {
			masterName = "mymaster" // Bitnami Helm Chart 默认值
		}

		fmt.Printf("Initializing Redis in Sentinel Mode. Master: %s, DB: %d\n", masterName, dbIndex)

		rdb = redis.NewFailoverClient(&redis.FailoverOptions{
			MasterName:    masterName,
			SentinelAddrs: strings.Split(sentinelAddrs, ","),
			DB:            dbIndex,
		})

	} else {
		// [模式 B] 单机模式 (本地开发/旧环境回退)
		redisAddr := os.Getenv("REDIS_ADDR")
		if redisAddr == "" {
			redisAddr = "redis-cart:6379" // 旧的默认值
		}

		fmt.Printf("Initializing Redis in Single Mode. Addr: %s, DB: %d\n", redisAddr, dbIndex)

		rdb = redis.NewClient(&redis.Options{
			Addr: redisAddr,
			DB:   dbIndex,
		})
	}

	// 带重试的 Redis 连接
	maxRetries := 10
	for i := 0; i < maxRetries; i++ {
		pingCtx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		err := rdb.Ping(pingCtx).Err()
		cancel()

		if err == nil {
			fmt.Println("connected to redis")
			break
		}

		if i == maxRetries-1 {
			return nil, fmt.Errorf("failed to connect to redis after %d retries: %w", maxRetries, err)
		}

		backoff := time.Duration(1<<i) * time.Second
		if backoff > 30*time.Second {
			backoff = 30 * time.Second
		}
		fmt.Printf("redis not ready, retry in %v... (%d/%d)\n", backoff, i+1, maxRetries)
		time.Sleep(backoff)
	}

	return &CartRedis{rdb: rdb}, nil
}

func (r *CartRedis) AddItem(ctx context.Context, userID string, item *pb.CartItem) error {
	key := fmt.Sprintf("cart:%s", userID)
	return r.rdb.HIncrBy(ctx, key, item.ProductId, int64(item.Quantity)).Err()
}

func (r *CartRedis) GetCart(ctx context.Context, userID string) ([]*pb.CartItem, error) {
	key := fmt.Sprintf("cart:%s", userID)
	data, err := r.rdb.HGetAll(ctx, key).Result()
	if err != nil {
		return nil, err
	}

	cartItems := make([]*pb.CartItem, 0, len(data))
	for k, v := range data {
		q, _ := strconv.Atoi(v)
		item := &pb.CartItem{
			ProductId: k,
			Quantity:  int32(q),
		}
		cartItems = append(cartItems, item)
	}
	return cartItems, nil
}

func (r *CartRedis) EmptyCart(ctx context.Context, userID string) error {
	key := fmt.Sprintf("cart:%s", userID)
	return r.rdb.Del(ctx, key).Err()
}
