package main

import (
	"context"
	"log"

	"github.com/redis/go-redis/v9"
)

var ctx = context.Background()

var rdb *redis.Client

func InitRedis(url string, password string, db int) *redis.Client {
	rdb = redis.NewClient(&redis.Options{
		Addr:     url,
		Password: password,
		DB:       db,
	})

	_, err := rdb.Ping(ctx).Result()

	if err != nil {
		log.Fatalf("Failed to connect to Redis: %v", err)
	}

	return rdb
}

func GetClient() *redis.Client {
	return rdb
}

func CloseClient() {
	rdb.Close()
}

func GetValue(key string) (string, error) {
	return rdb.Get(ctx, key).Result()
}

func SetValue(key string, value string) error {
	return rdb.Set(ctx, key, value, 0).Err()
}

func DeleteValue(key string) error {
	return rdb.Del(ctx, key).Err()
}

func GetAllKeys() ([]string, error) {
	return rdb.Keys(ctx, "*").Result()
}

// func GetAllValues() ([]string, error) {
// 	return rdb.Values(ctx, "*").Result()
// }
