package cache

import (
	"context"
	"time"

	"github.com/redis/go-redis/v9"
)

const paymentCacheTTL = 24 * time.Hour

type PaymentCache struct {
	client *redis.Client
}

func NewPaymentCache(addr string) *PaymentCache {
	client := redis.NewClient(&redis.Options{
		Addr:         addr,
		DialTimeout:  500 * time.Millisecond,
		ReadTimeout:  500 * time.Millisecond,
		WriteTimeout: 500 * time.Millisecond,
	})

	return &PaymentCache{client: client}
}

func (c *PaymentCache) GetPaymentID(
	ctx context.Context,
	key string,
) (string, error) {
	return c.client.Get(ctx, key).Result()
}

func (c *PaymentCache) SetPaymentID(
	ctx context.Context,
	key string,
	paymentID string,
) error {
	return c.client.Set(ctx, key, paymentID, paymentCacheTTL).Err()
}

func (c *PaymentCache) Ping(ctx context.Context) error {
	return c.client.Ping(ctx).Err()
}

func (c *PaymentCache) Close() error {
	return c.client.Close()
}
