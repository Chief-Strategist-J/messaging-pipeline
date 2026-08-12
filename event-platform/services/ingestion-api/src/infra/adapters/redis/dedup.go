package redis

import (
	"context"

	"event-platform/ingestion-api/src/shared/constants"
	"github.com/redis/go-redis/v9"
)

type Deduper interface {
	SeenBefore(ctx context.Context, eventID string) (bool, error)
	Forget(ctx context.Context, eventID string) error
}

type redisDeduper struct{ client *redis.Client }

func NewRedisDeduper(addr string) Deduper {
	return &redisDeduper{client: redis.NewClient(&redis.Options{Addr: addr})}
}

func (d *redisDeduper) SeenBefore(ctx context.Context, eventID string) (bool, error) {
	setByUs, err := d.client.SetNX(ctx, constants.DedupKeyPrefix+eventID, 1, constants.DedupTTL).Result()
	if err != nil {
		return false, err
	}
	return !setByUs, nil
}

func (d *redisDeduper) Forget(ctx context.Context, eventID string) error {
	return d.client.Del(ctx, constants.DedupKeyPrefix+eventID).Err()
}
