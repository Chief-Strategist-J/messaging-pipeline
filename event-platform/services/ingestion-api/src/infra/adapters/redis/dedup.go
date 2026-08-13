package redis

import (
	"context"
	"log/slog"
	"sync"
	"time"

	"event-platform/ingestion-api/src/shared/constants"
	"github.com/redis/go-redis/v9"
)

type Deduper interface {
	SeenBefore(ctx context.Context, eventID string) (bool, error)
	Forget(ctx context.Context, eventID string) error
}

type redisDeduper struct {
	client   *redis.Client
	memCache sync.Map
}

func NewRedisDeduper(addr string) Deduper {
	return &redisDeduper{
		client: redis.NewClient(&redis.Options{
			Addr:         addr,
			PoolSize:     500,
			MinIdleConns: 50,
			DialTimeout:  2 * time.Second,
			ReadTimeout:  1 * time.Second,
			WriteTimeout: 1 * time.Second,
			PoolTimeout:  3 * time.Second,
		}),
	}
}

func (d *redisDeduper) SeenBefore(ctx context.Context, eventID string) (bool, error) {
	if eventID == "" {
		return false, nil
	}

	if _, ok := d.memCache.Load(eventID); ok {
		return true, nil
	}

	setByUs, err := d.client.SetNX(ctx, constants.DedupKeyPrefix+eventID, 1, constants.DedupTTL).Result()
	if err != nil {
		slog.Error("redis dedup check failed, failing open", "event_id", eventID, "error", err)
		return false, nil
	}

	if setByUs {
		d.memCache.Store(eventID, struct{}{})
		return false, nil
	}

	d.memCache.Store(eventID, struct{}{})
	return true, nil
}

func (d *redisDeduper) Forget(ctx context.Context, eventID string) error {
	if eventID == "" {
		return nil
	}
	d.memCache.Delete(eventID)
	return d.client.Del(ctx, constants.DedupKeyPrefix+eventID).Err()
}
