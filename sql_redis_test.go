package sqlredis

import (
	"testing"
	"time"

	"github.com/go-jar/golog"
	"github.com/go-jar/mysql"
	"github.com/go-jar/redis"
)

/*
CREATE TABLE `demo` (
  `id` int(11) unsigned NOT NULL AUTO_INCREMENT,
  `add_time` datetime,
  `edit_time` datetime NOT NULL DEFAULT current_timestamp() ON UPDATE current_timestamp(),
  `name` varchar(20) COLLATE utf8mb4_bin NOT NULL DEFAULT '',
  `status`varchar(20) COLLATE utf8mb4_bin NOT NULL DEFAULT '',
  PRIMARY KEY (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=0 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;
*/

type SqlBaseEntity struct {
	Id       int64     `mysql:"id" json:"id" redis:"id"`
	AddTime  time.Time `mysql:"add_time" json:"add_time" redis:"add_time"`
	EditTime time.Time `mysql:"edit_time" json:"edit_time" redis:"edit_time"`
}

type DemoEntity struct {
	SqlBaseEntity

	Name   string `mysql:"name" json:"name" redis:"name"`
	Status int    `mysql:"status" json:"status" redis:"status"`
}

func TestSqlRedisBindStore(t *testing.T) {
	mysqlClientPool := newMysqlTestPool()
	redisClientPool := newRedisTestPool()

	sr := &SqlRedis{
		SqlOrm:   mysql.NewSimpleOrm([]byte("TestSqlRedisBindSvc"), mysqlClientPool, true),
		RedisOrm: redis.NewSimpleOrm([]byte("TestSqlRedisBindSvc"), redisClientPool),
	}

	tableName, entityName := "demo", "demo"
	redisKeyPrefix := "test_sql_redis_bind"

	var err error
	var find bool

	t.Log("test Insert")

	item := &DemoEntity{
		SqlBaseEntity: SqlBaseEntity{
			Id:       512,
			AddTime:  time.Now(),
			EditTime: time.Now(),
		},
		Name:   "tdj",
		Status: 1,
	}

	ids, err := sr.Insert(tableName, tableName, "Id", redisKeyPrefix, 10, item)
	if err != nil {
		t.Error(err)
	} else {
		t.Log(ids)
	}

	t.Log("test GetById")

	find, err = sr.GetById(tableName, entityName, redisKeyPrefix, ids[0], 10, item)
	t.Log(find, err, item)

	t.Log("test UpdateById")

	newDemo := &DemoEntity{
		Name: "new-demo",
	}
	updateFields := map[string]bool{"name": true}
	setItems, err := sr.UpdateById(tableName, entityName, redisKeyPrefix, ids[0], newDemo, updateFields, 10)
	if err != nil {
		t.Log(err)
	} else {
		for i, item := range setItems {
			t.Log(i, item)
		}
	}

	item = &DemoEntity{}
	find, err = sr.GetById(tableName, entityName, redisKeyPrefix, ids[0], 10, item)
	t.Log(find, err, item)

	t.Log("test TotalRows")

	total, err := sr.TotalRows(tableName, redisKeyPrefix, 10)
	t.Log(total, err)

	t.Log("test TotalRows")

	find, err = sr.DeleteById(tableName, entityName, redisKeyPrefix, ids[0])
	t.Log(find, err)
}

func newMysqlTestPool() *mysql.Pool {
	config := &mysql.PoolConfig{NewClientFunc: newMysqlTestClient}
	config.MaxConns = 100
	config.MaxIdleTime = time.Second * 5

	mysqlClientPool := mysql.NewPool(config)
	return mysqlClientPool
}

func newMysqlTestClient() (*mysql.Client, error) {
	config := mysql.NewConfig("root", "yuntest#cloud", "10.66.172.152", "3306", "demo")
	config.LogLevel = golog.LevelInfo

	return mysql.NewClient(config, nil)
}

func newRedisTestPool() *redis.Pool {
	config := &redis.PoolConfig{NewClientFunc: newRedisTestClient}
	config.MaxConns = 100
	config.MaxIdleTime = time.Second * 5

	mysqlClientPool := redis.NewPool(config)
	return mysqlClientPool
}

func newRedisTestClient() (*redis.Client, error) {
	logger, _ := golog.NewConsoleLogger(golog.LevelInfo)
	config := redis.NewConfig("127.0.0.1", "6379", "passwd")
	config.ConnectTimeout = time.Second * 3

	return redis.NewClient(config, logger), nil
}
