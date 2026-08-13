package redis

import (
	"context"
	"sync"
)

type Deduper interface {
	SeenBefore(ctx context.Context, eventID string) (bool, error)
	Forget(ctx context.Context, eventID string) error
}

type inMemDeduper struct {
	memCache sync.Map
}

func NewInMemDeduper() Deduper {
	return &inMemDeduper{}
}

func NewRedisDeduper(addr string) Deduper {
	return NewInMemDeduper()
}

func (d *inMemDeduper) SeenBefore(ctx context.Context, eventID string) (bool, error) {
	if eventID == "" {
		return false, nil
	}
	if _, ok := d.memCache.Load(eventID); ok {
		return true, nil
	}
	d.memCache.Store(eventID, struct{}{})
	return false, nil
}

func (d *inMemDeduper) Forget(ctx context.Context, eventID string) error {
	if eventID == "" {
		return nil
	}
	d.memCache.Delete(eventID)
	return nil
}
