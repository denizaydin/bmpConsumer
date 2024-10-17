package redis

import (
	cfg "bmpConsumer/config"
	"context"
	"time"

	"github.com/go-redis/redis/v8"
	"github.com/golang/glog"
)

type RedisClient struct {
	client *redis.Client
}

// Initialize Redis client
func NewRedisClient(config *cfg.Config) *RedisClient {
	rdb := redis.NewClient(&redis.Options{
		Addr:     config.Redis.Address,
		Password: "", // no password set
		DB:       0,  // use default DB
	})

	return &RedisClient{
		client: rdb,
	}
}

// Close gracefully shuts down the Redis client
func (r *RedisClient) Close() {
	if err := r.client.Close(); err != nil {
		glog.Errorf("Failed to close Redis client: %v", err)
	} else {
		glog.Info("Redis client closed successfully.")
	}
}

// Cache returns true if the key is not in database, or its in the database value is different indicating that it needs to be process in graph database
// It returns false in get error during redis operations for overhelming graph database in case of redis failure
func (r *RedisClient) Cache(ctx context.Context, key string, value string) (bool, error) {
	// Check if the key already exists
	existingValue, err := r.client.Get(ctx, key).Result()
	if err == redis.Nil {
		// Key doesn't exist, insert it into Redis
		if err := r.client.Set(ctx, key, value, time.Duration(1800)*time.Hour).Err(); err != nil {
			glog.Errorf("error %s while inserting key %s with value %s", err, key, value)
			return false, err
		}
		glog.Infof("inserted new key %s with value %s", key, value)
		return true, nil
	} else if err != nil {
		glog.Errorf("error %s while getting key %s", err, key)
		return false, err
	} else {
		if value == existingValue {
			// key exists, just update the TTL
			if err := r.client.Expire(ctx, key, time.Duration(1800)*time.Hour).Err(); err != nil {
				return false, err
			}
			glog.Infof("key %s already exists with value %s, TTL updated", key, existingValue)
			return false, nil
		} else {
			// Key exists but value is different, update the value and TTL
			if err := r.client.Set(ctx, key, value, time.Duration(1800)*time.Hour).Err(); err != nil {
				glog.Errorf("could not update key %s: %v", key, err)
				return false, err
			}
			glog.Infof("updated key %s with new value %s", key, value)
			return true, nil
		}
	}
}
