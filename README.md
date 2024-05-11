# go-redis-to-mysql
使用go脚本命令。redis数据迁移至mysql中

go run redisToMysql.go -srcHost=127.0.0.1  -srcPort=6379 -srcDB=0  -mysqlHost=127.0.0.1 -mysqlPort=3306 -mysqlUser=root -mysqlPassword=root

# go-redis-to-redis
使用go脚本命令。redis某个库迁移至redis另一个库
go run redisMigration.go -srcHost=127.0.0.1  -srcPort=6379 -srcDB=0  -dstHost=127.0.0.1 -dstPort=6380 -dstDB=1
