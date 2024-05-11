package main

import (
	"flag"
	"fmt"
	"github.com/go-redis/redis"
	"github.com/schollz/progressbar/v3"
	"log"
	"sync"
	"time"
)

// 批处理大小
const batchSize = 1000
const work = 30

type dataTypeFunc func(key string, srcClient *redis.Client, pipeline redis.Pipeliner)

var dataTypeHandlers = map[string]dataTypeFunc{
	"string": handleString,
	"list":   handleList,
	"hash":   handleHash,
	"set":    handleSet,
	"zset":   handleZSet,
}

// 处理字符串类型数据
func handleString(key string, srcClient *redis.Client, pipeline redis.Pipeliner) {
	val := srcClient.Get(key).Val()
	ttl := srcClient.TTL(key).Val()
	if ttl > 0 {
		ttl = expirationTime(ttl)
	}
	pipeline.Set(key, val, ttl)
}

// 处理列表类型数据
func handleList(key string, srcClient *redis.Client, pipeline redis.Pipeliner) {
	listVal := srcClient.LRange(key, 0, -1).Val()
	ttl := srcClient.TTL(key).Val()
	pipeline.RPush(key, listVal)
	if ttl > 0 {
		pipeline.Expire(key, expirationTime(ttl))
	}

}

// 处理哈希类型数据
func handleHash(key string, srcClient *redis.Client, pipeline redis.Pipeliner) {
	hashVal := srcClient.HGetAll(key).Val()
	ttl := srcClient.TTL(key).Val()

	// 将map[string]string转换为map[string]interface{}
	hashValInterface := make(map[string]interface{}, len(hashVal))
	for k, v := range hashVal {
		hashValInterface[k] = v
	}

	pipeline.HMSet(key, hashValInterface)
	if ttl > 0 {
		pipeline.Expire(key, expirationTime(ttl))
	}
}

// 处理集合类型数据
func handleSet(key string, srcClient *redis.Client, pipeline redis.Pipeliner) {
	setVal := srcClient.SMembers(key).Val()
	ttl := srcClient.TTL(key).Val()

	pipeline.SAdd(key, setVal)
	if ttl > 0 {
		pipeline.Expire(key, expirationTime(ttl))
	}
}

// 处理有序集合类型数据
func handleZSet(key string, srcClient *redis.Client, pipeline redis.Pipeliner) {
	zsetVal := srcClient.ZRangeWithScores(key, 0, -1).Val()
	ttl := srcClient.TTL(key).Val()

	pipeline.ZAdd(key, zsetVal...)
	if ttl > 0 {
		pipeline.Expire(key, expirationTime(ttl))
	}

}

// 数据类型处理函数
func handleDataType(key string, srcClient *redis.Client, pipeline redis.Pipeliner) {
	dataType := srcClient.Type(key).Val()

	handler, ok := dataTypeHandlers[dataType]
	if !ok {
		log.Printf("Unsupported data type for key %s\n", key)
		return
	}

	handler(key, srcClient, pipeline)
}

// 处理key的过期时间
func expirationTime(ttl time.Duration) time.Duration {
	ttlInt64 := int64(ttl.Seconds())
	//如果过期时间小于两分钟 则重新赋值五分钟
	if ttlInt64 < 120 && ttl > 0 {
		ttl = time.Duration(5*60) * time.Second
		return ttl
	}
	return ttl
}

// 迁移Redis数据
func migrateData(srcClient, dstClient *redis.Client) time.Duration {
	// 扫描源Redis数据库的keys
	var cursor uint64
	var keys []string
	startTime := time.Now()
	log.Printf("In execution ...")

	// 创建缓冲信道和等待组
	dataChan := make(chan []string, batchSize)
	var wg sync.WaitGroup

	// 创建一个goroutine用于读取数据到信道中
	go func() {
		defer close(dataChan)
		for {
			var err error
			keys, cursor, err = srcClient.Scan(cursor, "", batchSize).Result()

			if err != nil {
				log.Fatalf("Error scanning source Redis database: %v\n", err)
			}

			// 将批量keys发送到通道
			dataChan <- keys

			if cursor == 0 {
				break
			}
		}
	}()
	// 获取源Redis数据库中的键的总数量
	totalKeys, err := srcClient.DBSize().Result()
	if err != nil {
		log.Fatalf("Error getting total keys: %v\n", err)
	}

	// 创建进度条
	bar := progressbar.Default(totalKeys)
	// 启动多个goroutine来处理数据
	for i := 0; i < work; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()

			for keys := range dataChan {
				// 在每个goroutine内创建一个Pipeline
				pipeline := dstClient.Pipeline()
				for _, key := range keys {
					handleDataType(key, srcClient, pipeline)

				}
				// 执行Pipeline
				_, err := pipeline.Exec()
				if err != nil {
					log.Fatalf("Error executing Pipeline: %v\n", err)
				}
				// 更新进度条
				bar.Add(len(keys))
			}

		}()
	}

	// 等待所有goroutine完成
	wg.Wait()

	return time.Since(startTime)
}

func main() {
	// 解析命令行参数
	srcHost := flag.String("srcHost", "127.0.0.1", "源Redis主机地址")
	srcPort := flag.Int("srcPort", 6379, "源Redis端口号")
	srcDB := flag.Int("srcDB", 0, "源Redis数据库索引")
	srcPassword := flag.String("srcPassword", "", "源Redis密码")

	dstHost := flag.String("dstHost", "127.0.0.1", "目标Redis主机地址")
	dstPort := flag.Int("dstPort", 6379, "目标Redis端口号")
	dstDB := flag.Int("dstDB", 1, "目标Redis数据库索引")
	dstPassword := flag.String("dstPassword", "", "目标Redis密码")

	flag.Parse()

	// 创建源Redis客户端连接池
	srcPool := redis.NewClient(&redis.Options{
		Addr:     fmt.Sprintf("%s:%d", *srcHost, *srcPort),
		Password: *srcPassword,
		DB:       *srcDB,
		PoolSize: 30, // 连接池大小，根据实际情况进行调整
	})

	// 创建目标Redis客户端连接池
	dstPool := redis.NewClient(&redis.Options{
		Addr:        fmt.Sprintf("%s:%d", *dstHost, *dstPort),
		Password:    *dstPassword,
		DB:          *dstDB,
		ReadTimeout: 60 * time.Second,
		PoolSize:    30, // 连接池大小，根据实际情况进行调整
	})

	defer srcPool.Close()
	defer dstPool.Close()

	// 迁移数据并获取所花费的时间
	duration := migrateData(srcPool, dstPool)

	log.Printf("Migration completed in %s.\n", duration.String())
}
