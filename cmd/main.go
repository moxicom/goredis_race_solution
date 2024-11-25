package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"strconv"
	"sync"
	"time"

	goredislib "github.com/redis/go-redis/v9"
)

const (
	ExpireTime = time.Second * 20
)

type RedisStorage struct {
	cli *goredislib.Client
	//rSync *redsync.Redsync
}

func New() *RedisStorage {
	client := newRedisClient()

	return &RedisStorage{cli: client}
}

func newRedisClient() *goredislib.Client {
	redisDb, err := strconv.Atoi("1")
	if err != nil {
		log.Fatal("REDIS_DB is not a number")
	}

	cli := goredislib.NewClient(&goredislib.Options{
		Addr:     fmt.Sprintf("%s:%s", "localhost", "6379"),
		Password: "admin",
		DB:       redisDb,
	})

	if err := cli.Ping(context.Background()).Err(); err != nil {
		log.Fatalf("failed to connect to redis %s", err)
	}
	log.Println("created connection to redis")

	return cli
}

func (s *RedisStorage) Set(ctx context.Context, key string, val int) error {
	byteData, err := json.Marshal(val)
	if err != nil {
		return err
	}

	err = s.cli.Set(ctx, key, byteData, time.Second*20).Err()
	if err != nil {
		return err
	}

	log.Println("Set successfully")

	return nil
}

func (s *RedisStorage) Get(ctx context.Context, key string) (int, error) {
	byteData, err := s.cli.Get(ctx, key).Bytes()
	if err != nil {
		return 0, err
	}

	var data int
	err = json.Unmarshal(byteData, &data)
	if err != nil {
		return 0, err
	}

	log.Println("Gt successfully ", data)

	return data, nil
}

func (s *RedisStorage) Update(ctx context.Context, key string, i int) error {
	var err error
	for {
		err = s.cli.Watch(ctx, func(tx *goredislib.Tx) error {
			byteData, err := tx.Get(ctx, key).Bytes()
			if err != nil && !errors.Is(err, goredislib.Nil) {
				return err
			}

			var data int
			err = json.Unmarshal(byteData, &data)
			if err != nil {
				return err
			}

			newValue := data + 2

			// Выполнение обновления только если ключ не изменился
			_, err = tx.TxPipelined(ctx, func(pipe goredislib.Pipeliner) error {
				pipe.Set(ctx, key, newValue, 0)
				return nil
			})

			//if err != nil {
			//	log.Printf("%v txPipeline failed %s\n", i, err)
			//}

			return err
		}, key)

		if err == nil || !errors.Is(err, goredislib.TxFailedErr) {
			break
		} else {
			time.Sleep(time.Millisecond * 100)
		}
	}

	return err
}

func doShit(ctx context.Context, rcli *RedisStorage, key string, wg *sync.WaitGroup, i int) {
	defer wg.Done()

	if err := rcli.Update(ctx, key, i); err != nil {
		panic(err)
	}
	log.Printf("%v updated successfully\n", i)
}

func main() {
	rcli := New()

	ctx := context.Background()

	key := "key"

	// init zero value
	err := rcli.Set(ctx, key, 0)
	if err != nil {
		panic(err)
	}

	var wg sync.WaitGroup

	wg.Add(100)

	for i := 0; i < 100; i++ {
		go doShit(ctx, rcli, key, &wg, i)
	}

	wg.Wait()

	val, err := rcli.Get(ctx, key)
	if err != nil {
		panic(err)
	}

	log.Println(val)
}
