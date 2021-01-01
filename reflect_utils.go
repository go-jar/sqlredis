package sqlredis

import (
	"fmt"
	"reflect"
)

func reflectId(rev reflect.Value, idFieldName string) (int64, error) {
	if rev.Kind() == reflect.Ptr {
		rev = rev.Elem()
	}

	var id int64
	id = -1
	ret := rev.Type()

	if rev.Kind() == reflect.Struct {
		for i := 0; i < rev.NumField(); i++ {
			revF := rev.Field(i)

			if revF.Kind() == reflect.Ptr || revF.Kind() == reflect.Struct {
				id, _ = reflectId(revF, idFieldName)
			}

			if ret.Field(i).Name == idFieldName {
				id = revF.Interface().(int64)
				return id, nil
			}
		}
	}

	return -1, fmt.Errorf("invalid field name: %s", idFieldName)
}
