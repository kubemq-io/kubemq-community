package actions

import (
	"encoding/json"
	"fmt"
	"reflect"
)

func detectAndConvertToBytesArray(data any) ([]byte, error) {
	switch data.(type) {
	case []byte:
		return data.([]byte), nil
	case string:
		return []byte(data.(string)), nil
	case map[string]interface{}:
		return json.Marshal(data)
	case []interface{}:
		return json.Marshal(data)
	default:
		return nil, fmt.Errorf("unsupported data type: %s", reflect.TypeOf(data))
	}
}

func transformJsonMapToStruct(data map[string]interface{}, obj interface{}) error {
	jsonData, err := json.Marshal(data)
	if err != nil {
		return err
	}
	return json.Unmarshal(jsonData, obj)
}
