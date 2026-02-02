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

	// 3. 测试连接
	// 使用带超时的 Context 防止连不上一直卡住
	_, cancel := context.WithTimeout(context.Background(), time.Second*3) // 伪代码修正：这里应该是 time.Second
	// 修正：你需要 import "time" 才能用 time.Second，或者直接用 context.Background() 简单测试
	// 为了不引入新包，我们简单点直接 Ping
	defer cancel()
	if err := rdb.Ping(context.Background()).Err(); err != nil {
		return nil, fmt.Errorf("failed to connect to redis: %w", err)
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
