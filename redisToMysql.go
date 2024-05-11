package main

import (
	"database/sql"
	"flag"
	"fmt"
	"github.com/go-redis/redis"
	_ "github.com/go-sql-driver/mysql"
	"github.com/schollz/progressbar/v3"
	"log"
	"strings"
	"sync"
	"time"
)

// 批处理大小
const batchSizes = 1000
const works = 30

type dataTypeFuncs func(key string, srcClient *redis.Client, pipeline redis.Pipeliner)

// 数据类型处理函数
func handleDataTypes(key string, srcClient *redis.Client, pipeline redis.Pipeliner) {
	dataType := srcClient.Type(key).Val()

	handler, ok := dataTypeHandler[dataType]
	if !ok {
		log.Printf("Unsupported data type for key %s\n", key)
		return
	}

	handler(key, srcClient, pipeline)
}

var dataTypeHandler = map[string]dataTypeFuncs{
	"string": handleStrings,
}

// 处理字符串类型数据
func handleStrings(key string, srcClient *redis.Client, pipeline redis.Pipeliner) {
	val := srcClient.Get(key).Val()
	ttl := srcClient.TTL(key).Val()

	pipeline.Set(key, val, expiration(ttl))
}

// 处理key的过期时间
func expiration(ttl time.Duration) time.Duration {
	ttlInt64 := int64(ttl.Seconds())
	//如果过期时间小于0
	if ttlInt64 < 0 {
		ttl = time.Duration(1*86400) * time.Second
		return ttl
	}
	return ttl
}

// 处理符合family_reward 以及string类型的数据
func processKey(keys []string, srcClient *redis.Client) (int64, []string) {
	var stringKeys []string
	for _, key := range keys {
		dataType := srcClient.Type(key).Val()
		if strings.HasPrefix(key, "family_reward") && dataType == "string" {
			stringKeys = append(stringKeys, key)
		}
	}
	return int64(len(stringKeys)), stringKeys
}

// 插入 MySQL 数据
func insertMySQLData(tx *sql.Tx, db *sql.DB, key string, srcClient *redis.Client, pipeline redis.Pipeliner) error {
	// 拆分 key
	part1, part2, part3 := splitKey(key)

	// 查询 Redis 数据类型
	dataType := srcClient.Type(key).Val()

	// 判断是否是字符串类型并且符合条件
	if dataType == "string" && strings.HasPrefix(key, "family_reward") {
		// 检查 MySQL 是否已存在该键
		var exists int
		err := db.QueryRow("SELECT EXISTS(SELECT 1 FROM rewards_markers WHERE prefix_name = ? AND uuid = ? AND rewards_name = ?)", part1, part2, part3).Scan(&exists)
		if err != nil {
			return fmt.Errorf("error checking if key %s exists in MySQL: %v", key, err)
		}

		// 如果存在，则跳过
		if exists == 1 {
			return nil
		}

		// 执行 MySQL 插入操作
		_, err = tx.Exec("INSERT INTO rewards_markers (prefix_name, uuid,rewards_name) VALUES (?, ?,?)", part1, part2, part3)
		if err != nil {
			return fmt.Errorf("error inserting key %s into MySQL: %v", key, err)
		}

		// 对 Redis 的 key TTL 重新赋值
		handleDataTypes(key, srcClient, pipeline)
	}

	return nil
}

// 迁移 Redis 数据到 MySQL
func migrateDatas(srcClient *redis.Client, db *sql.DB) time.Duration {
	// 扫描源 Redis 数据库的 keys
	var cursor uint64
	var keys []string
	// 过滤出类型为 string 的键
	startTime := time.Now()
	log.Printf("In execution ...")

	// 统计总的符合条件的键数量
	totalCount := int64(0)
	for {
		var err error
		keys, cursor, err = srcClient.Scan(cursor, "", batchSizes).Result()
		if err != nil {
			log.Fatalf("Error scanning source Redis database: %v\n", err)
		}
		count, _ := processKey(keys, srcClient)
		totalCount += count
		if cursor == 0 {
			break
		}
	}

	// 创建进度条
	bar := progressbar.Default(totalCount)

	// 创建缓冲信道和等待组
	dataChan := make(chan []string, works)
	var wg sync.WaitGroup

	// 创建一个 goroutine 用于读取数据到信道中
	go func() {
		defer close(dataChan)
		cursor = 0 // 重置游标
		for {
			var err error
			keys, cursor, err = srcClient.Scan(cursor, "", batchSizes).Result()
			if err != nil {
				log.Fatalf("Error scanning source Redis database: %v\n", err)
			}
			_, stringKeys := processKey(keys, srcClient)
			if len(stringKeys) > 0 {
				dataChan <- stringKeys
			}
			if cursor == 0 {
				break
			}
		}
	}()

	// 启动多个 goroutine 来处理数据
	for i := 0; i < works; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()

			for keys := range dataChan {
				pipeline := srcClient.Pipeline()
				// 开启一个 MySQL 事务
				tx, err := db.Begin()
				if err != nil {
					log.Fatalf("Error beginning MySQL transaction: %v\n", err)
				}
				for _, key := range keys {
					// 插入 MySQL 数据
					err := insertMySQLData(tx, db, key, srcClient, pipeline)
					if err != nil {
						log.Println(err)
						continue
					}
					// 更新进度条
					bar.Add(1)
				}

				// 提交事务
				err = tx.Commit()
				if err != nil {
					log.Fatalf("Error committing MySQL transaction: %v\n", err)
				}
				// 执行 Pipeline
				_, err = pipeline.Exec()
				if err != nil {
					log.Fatalf("Error executing Pipeline: %v\n", err)
				}
			}
		}()
	}

	// 等待所有 goroutine 完成
	wg.Wait()

	return time.Since(startTime)
}

// 拆分key
func splitKey(key string) (string, string, string) {
	parts := strings.Split(key, ":")
	if len(parts) != 3 {
		return "", "", ""
	}
	return parts[0], parts[1], parts[2]
}

func main() {
	// 解析命令行参数
	srcHost := flag.String("srcHost", "127.0.0.1", "源Redis主机地址")
	srcPort := flag.Int("srcPort", 6379, "源Redis端口号")
	srcDB := flag.Int("srcDB", 0, "源Redis数据库索引")
	srcPassword := flag.String("srcPassword", "", "源Redis密码")

	// mysql命令行参数
	mysqlHost := flag.String("mysqlHost", "127.0.0.1", "MySQL主机地址")
	mysqlPort := flag.String("mysqlPort", "3306", "MySQL端口号")
	mysqlUser := flag.String("mysqlUser", "root", "MySQL用户名")
	mysqlPassword := flag.String("mysqlPassword", "root", "MySQL密码")
	mysqlDBName := flag.String("mysqlDBName", "test", "MySQL数据库名称")

	flag.Parse()

	// 创建源Redis客户端连接池
	srcPool := redis.NewClient(&redis.Options{
		Addr:     fmt.Sprintf("%s:%d", *srcHost, *srcPort),
		Password: *srcPassword,
		DB:       *srcDB,
		PoolSize: 30, // 连接池大小，根据实际情况进行调整
	})

	// 构建MySQL DSN (数据源名称)
	dsn := fmt.Sprintf("%s:%s@tcp(%s:%s)/%s", *mysqlUser, *mysqlPassword, *mysqlHost, *mysqlPort, *mysqlDBName)

	// 连接到MySQL数据库
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		log.Fatal("连接到MySQL数据库失败:", err)
	}
	// 检查连接是否成功
	err = db.Ping()
	if err != nil {
		log.Fatal("无法连接到MySQL数据库:", err)
	}

	defer srcPool.Close()
	defer db.Close()

	// 迁移数据并获取所花费的时间
	duration := migrateDatas(srcPool, db)

	log.Printf("Migration completed in %s.\n", duration.String())
}
