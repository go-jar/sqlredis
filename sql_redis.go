package sqlredis

import (
	"errors"
	"reflect"
	"strconv"

	"github.com/go-jar/mysql"
	"github.com/go-jar/redis"
)

type SqlRedis struct {
	SqlOrm   *mysql.SimpleOrm
	RedisOrm *redis.SimpleOrm
}

func (sr *SqlRedis) RedisKeyForEntity(id interface{}, prefix, entityName string) string {
	return prefix + "_entity_" + entityName + "_id_" + strconv.FormatInt(id.(int64), 10)
}

func (sr *SqlRedis) RedisKeyForTotalRows(tableName, redisKeyPrefix string) string {
	return redisKeyPrefix + "_total_rows_" + tableName
}

func (sr *SqlRedis) Insert(tableName, idFieldName, redisKeyPrefix string, expireSeconds int64, entities ...interface{}) error {
	if len(entities) == 0 {
		return errors.New("no object to be inserted")
	}

	err := sr.SqlOrm.Insert(tableName, entities...)
	if err != nil {
		return err
	}

	ret := reflect.TypeOf(entities[0])
	var entityName string
	if ret.Kind() == reflect.Ptr {
		entityName = ret.Elem().Name()
	} else {
		entityName = ret.Name()
	}

	for _, entity := range entities {
		rev := reflect.ValueOf(entity)
		id, err := reflectId(rev, idFieldName)
		if err != nil {
			return err
		}

		err = sr.RedisOrm.SaveEntity(sr.RedisKeyForEntity(id, redisKeyPrefix, entityName), entity, expireSeconds)
		if err != nil {
			return err
		}
	}

	return nil
}

func (sr *SqlRedis) GetById(tableName, entityName, redisKeyPrefix string, id, expireSeconds int64, entityPtr interface{}) (bool, error) {
	rk := sr.RedisKeyForEntity(id, redisKeyPrefix, entityName)

	find, err := sr.RedisOrm.GetEntity(rk, entityPtr)
	if err != nil {
		find, err := sr.SqlOrm.GetById(tableName, id, entityPtr)
		return find, err
	}
	if find {
		return true, nil
	}

	find, err = sr.SqlOrm.GetById(tableName, id, entityPtr)
	if err != nil {
		return false, err
	}
	if !find {
		return false, nil
	}

	err = sr.RedisOrm.SaveEntity(rk, entityPtr, expireSeconds)

	return true, err
}

func (sr *SqlRedis) DeleteById(tableName, entityName, redisKeyPrefix string, id int64) (bool, error) {
	result := sr.SqlOrm.Dao().DeleteById(tableName, id)
	defer sr.SqlOrm.PutBackClient()

	if result.Err != nil {
		return false, result.Err
	}

	if result.RowsAffected == 0 {
		return false, nil
	}

	rk := sr.RedisKeyForEntity(id, redisKeyPrefix, entityName)
	err := sr.RedisOrm.Client().Do("del", rk).Err
	defer sr.RedisOrm.PutBackClient()

	return true, err
}

func (sr *SqlRedis) UpdateById(tableName, entityName, redisKeyPrefix string, id int64, newEntityPtr interface{}, updateFields map[string]bool, expireSeconds int64) ([]*mysql.QueryItem, error) {
	setItems, err := sr.SqlOrm.UpdateById(tableName, id, newEntityPtr, updateFields)

	if err != nil {
		return nil, err
	}
	if setItems == nil {
		return nil, nil
	}

	err = sr.RedisOrm.Del(sr.RedisKeyForEntity(id, redisKeyPrefix, entityName))

	return setItems, err
}

func (sr *SqlRedis) TotalRows(tableName, redisKeyPrefix string, expireSeconds int64) (int64, error) {
	rk := sr.RedisKeyForTotalRows(tableName, redisKeyPrefix)

	rClient := sr.RedisOrm.Client()
	defer sr.RedisOrm.PutBackClient()

	reply := rClient.Do("get", rk)
	err := reply.Err
	if err == nil {
		if !reply.SimpleReplyIsNil() {
			total, err := reply.Int64()
			if err == nil {
				return total, nil
			}
			rClient.Do("del", rk)
		}
	}

	total, err := sr.SqlOrm.Dao().SelectTotalAnd(tableName)
	defer sr.SqlOrm.PutBackClient()

	if err != nil {
		return 0, err
	}

	reply = rClient.Do("set", rk, total, "ex", expireSeconds)

	return total, err
}

func (sr *SqlRedis) UpdateEntity(redisKey string, setItems []*mysql.QueryItem, expireSeconds int64) error {
	cnt := len(setItems)*2 + 1
	args := make([]interface{}, cnt)
	args[0] = redisKey

	for si, ai := 0, 1; ai < cnt; si++ {
		args[ai] = setItems[si].Name
		ai++
		args[ai] = setItems[si].Value
		ai++
	}

	rClient := sr.RedisOrm.Client()
	defer sr.RedisOrm.PutBackClient()

	rClient.Send("hmset", args...)
	if expireSeconds > 0 {
		rClient.Send("expire", redisKey, expireSeconds)
	}
	replies, errIndexes := rClient.FlushCmdQueue()
	if len(errIndexes) != 0 {
		rClient.Free()
		msg := "hmset key " + redisKey + " to redis error:"
		for _, i := range errIndexes {
			msg += " " + replies[i].Err.Error()
		}
		return errors.New(msg)
	}

	return nil
}
