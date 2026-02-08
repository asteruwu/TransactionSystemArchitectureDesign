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
	"fmt"
	"net"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"

	pb "github.com/GoogleCloudPlatform/microservices-demo/src/frontend/genproto"
	"github.com/google/uuid"
	redis "github.com/redis/go-redis/v9"
	"github.com/sirupsen/logrus"
)

var tokenBucketScript = redis.NewScript(`
	local key = KEYS[1]
	local capacity = tonumber(ARGV[1])
	local rate = tonumber(ARGV[2])
	local now = tonumber(ARGV[3])
	local requested = tonumber(ARGV[4])

	local info = redis.call("HMGET", key, "tokens", "last_refill")
	local tokens = tonumber(info[1])
	local last_refill = tonumber(info[2])

	if tokens == nil then
		tokens = capacity
		last_refill = now
	end

	local delta = math.max(0, now - last_refill)
	local filled_tokens = math.min(capacity, tokens + (delta / 1000 * rate))

	local allowed = 0
	if filled_tokens >= requested then
		filled_tokens = filled_tokens - requested
		allowed = 1
		redis.call("HMSET", key, "tokens", filled_tokens, "last_refill", now)
		redis.call("EXPIRE", key, math.ceil(capacity / rate) * 2)
	end

	return allowed
`)

type ctxKeyLog struct{}
type ctxKeyRequestID struct{}
type ctxKeyUserID struct{}

type logHandler struct {
	log  *logrus.Logger
	next http.Handler
}

type responseRecorder struct {
	b      int
	status int
	w      http.ResponseWriter
}

type Limiter struct {
	client *redis.Client
	log    *logrus.Logger
}

func (r *responseRecorder) Header() http.Header { return r.w.Header() }

func (r *responseRecorder) Write(p []byte) (int, error) {
	if r.status == 0 {
		r.status = http.StatusOK
	}
	n, err := r.w.Write(p)
	r.b += n
	return n, err
}

func (r *responseRecorder) WriteHeader(statusCode int) {
	r.status = statusCode
	r.w.WriteHeader(statusCode)
}

func (lh *logHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	requestID, _ := uuid.NewRandom()
	ctx = context.WithValue(ctx, ctxKeyRequestID{}, requestID.String())

	start := time.Now()
	rr := &responseRecorder{w: w}
	log := lh.log.WithFields(logrus.Fields{
		"http.req.path":   r.URL.Path,
		"http.req.method": r.Method,
		"http.req.id":     requestID.String(),
	})
	if v, ok := r.Context().Value(ctxKeySessionID{}).(string); ok {
		log = log.WithField("session", v)
	}
	log.Debug("request started")
	defer func() {
		log.WithFields(logrus.Fields{
			"http.resp.took_ms": int64(time.Since(start) / time.Millisecond),
			"http.resp.status":  rr.status,
			"http.resp.bytes":   rr.b}).Debugf("request complete")
	}()

	ctx = context.WithValue(ctx, ctxKeyLog{}, log)
	r = r.WithContext(ctx)
	lh.next.ServeHTTP(rr, r)
}

func ensureSessionID(next http.Handler) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var sessionID string
		c, err := r.Cookie(cookieSessionID)
		if err == http.ErrNoCookie {
			if os.Getenv("ENABLE_SINGLE_SHARED_SESSION") == "true" {
				// Hard coded user id, shared across sessions
				sessionID = "12345678-1234-1234-1234-123456789123"
			} else {
				u, _ := uuid.NewRandom()
				sessionID = u.String()
			}
			http.SetCookie(w, &http.Cookie{
				Name:   cookieSessionID,
				Value:  sessionID,
				MaxAge: cookieMaxAge,
			})
		} else if err != nil {
			return
		} else {
			sessionID = c.Value
		}
		ctx := context.WithValue(r.Context(), ctxKeySessionID{}, sessionID)
		r = r.WithContext(ctx)
		next.ServeHTTP(w, r)
	}
}

func NewRedisLimiter(log *logrus.Logger) *Limiter {
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
			redisAddr = "localhost:6380" // 本地默认
			log.Info("Tried to connect to Redis, but REDIS_ADDR is not set. Using default address.")
		}
		log.Infof("Initializing Redis in Single Node Mode. Addr: %s", redisAddr)

		rdb = redis.NewClient(&redis.Options{
			Addr: redisAddr,
		})
	}

	// 带重试的 Redis 连接
	maxRetries := 10
	for i := 0; i < maxRetries; i++ {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		err := rdb.Ping(ctx).Err()
		cancel()

		if err == nil {
			log.Info("connected to redis")
			break
		}

		if i == maxRetries-1 {
			log.Warnf("failed to connect to redis after %d retries: %v, rate limiter disabled", maxRetries, err)
			return nil
		}

		backoff := time.Duration(1<<i) * time.Second
		if backoff > 30*time.Second {
			backoff = 30 * time.Second
		}
		log.Warnf("redis not ready, retry in %v... (%d/%d)", backoff, i+1, maxRetries)
		time.Sleep(backoff)
	}

	return &Limiter{
		client: rdb,
		log:    log,
	}
}

func (l *Limiter) Allow(ctx context.Context, key string, capacity int, rate float64) (bool, error) {
	now := time.Now().UnixMilli()

	keys := []string{fmt.Sprintf("rate_limit:%s", key)}
	args := []interface{}{capacity, rate, now, 1}

	result, err := tokenBucketScript.Run(ctx, l.client, keys, args...).Result()
	if err != nil {
		return false, err
	}
	return result.(int64) == 1, nil
}

func (l *Limiter) GlobalAndIPLimiter(next http.Handler) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ctx, cancel := context.WithTimeout(r.Context(), 200*time.Millisecond)
		defer cancel()

		ip := getRealIP(r)

		// Env vars with defaults: RATE_LIMIT_{TYPE}_{RPS|BURST}
		globalRate := getEnvFloat("RATELIMIT_GLOBAL_RPS", 1000.0)
		globalBurst := getEnvInt("RATELIMIT_GLOBAL_BURST", 1000)

		ipRate := getEnvFloat("RATELIMIT_IP_RPS", 5.0)
		ipBurst := getEnvInt("RATELIMIT_IP_BURST", 10)

		globalAllowed, err := l.Allow(ctx, "global_frontend", globalBurst, globalRate)
		if err != nil {
			l.log.Warnf("global limiter redis error: %v", err)
		} else if !globalAllowed {
			w.WriteHeader(http.StatusServiceUnavailable)
			w.Write([]byte("System busy"))
			return
		}

		ipAllowed, err := l.Allow(ctx, "ip:"+ip, ipBurst, ipRate)
		if err != nil {
			l.log.Warnf("ip limiter redis error: %v", err)
		} else if !ipAllowed {
			w.WriteHeader(http.StatusTooManyRequests)
			w.Write([]byte("Too many requests"))
			return
		}

		next.ServeHTTP(w, r)

	}
}

func getRealIP(r *http.Request) string {
	ip := r.Header.Get("X-Forwarded-For")
	if ip == "" {
		ip = r.Header.Get("X-Real-IP")
	}
	if ip == "" {
		ip, _, _ = net.SplitHostPort(r.RemoteAddr)
	}
	return ip
}

const cookieToken = cookiePrefix + "token"

// requireAuth 认证中间件
func (fe *frontendServer) requireAuth(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		tokenCookie, err := r.Cookie(cookieToken)
		if err != nil || tokenCookie.Value == "" {
			http.Redirect(w, r, baseUrl+"/login", http.StatusSeeOther)
			return
		}

		// 验证 token
		resp, err := pb.NewUserServiceClient(fe.userSvcConn).VerifyToken(
			r.Context(),
			&pb.VerifyTokenRequest{Token: tokenCookie.Value},
		)
		if err != nil || !resp.IsValid {
			// 清除失效 cookie
			http.SetCookie(w, &http.Cookie{Name: cookieToken, MaxAge: -1})
			http.Redirect(w, r, baseUrl+"/login", http.StatusSeeOther)
			return
		}

		// 注入 user_id
		ctx := context.WithValue(r.Context(), ctxKeyUserID{}, resp.UserId)
		next.ServeHTTP(w, r.WithContext(ctx))
	})
}

// getUserID 从 context 获取用户 ID
func getUserID(r *http.Request) string {
	if uid, ok := r.Context().Value(ctxKeyUserID{}).(string); ok {
		return uid
	}
	return ""
}

func getEnvFloat(key string, defaultVal float64) float64 {
	if val, ok := os.LookupEnv(key); ok {
		if f, err := strconv.ParseFloat(val, 64); err == nil {
			return f
		}
	}
	return defaultVal
}

func getEnvInt(key string, defaultVal int) int {
	if val, ok := os.LookupEnv(key); ok {
		if i, err := strconv.Atoi(val); err == nil {
			return i
		}
	}
	return defaultVal
}
