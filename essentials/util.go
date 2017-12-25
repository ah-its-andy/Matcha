package essentials

import (
	"fmt"
	"reflect"
	"time"

	"github.com/pborman/uuid"
)

func FormatTime(t time.Time) string {
	return t.Format(time.RFC3339)
}

func GetTypeName(obj interface{}) string {
	r := reflect.TypeOf(obj).String()
	return r
}

// func Debug(str interface{}) {
// 	fmt.Println(str)
// }

func WrapError(msg string, err error) error {
	return fmt.Errorf("%s : %s", msg, err.Error())
}

func RemoveFromSlice(slice []interface{}, elems ...interface{}) []interface{} {
	isInElems := make(map[interface{}]bool)
	for _, elem := range elems {
		isInElems[elem] = true
	}
	w := 0
	for _, elem := range slice {
		if !isInElems[elem] {
			slice[w] = elem
			w += 1
		}
	}
	return slice[:w]
}

func NewOrderedUUID() string {
	uid := uuid.NewUUID().String()
	return fmt.Sprintf("%s%s%s%s%s", uid[14:18], uid[9:13], uid[0:8], uid[19:23], uid[24:])
}
