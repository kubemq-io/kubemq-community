package actions

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"reflect"
)

func detectAndConvertToBytesArray(data any) ([]byte, string, error) {
	switch data.(type) {
	case []byte:
		return data.([]byte), "[]Bytes", nil
	case string:
		enc := base64.StdEncoding.EncodeToString([]byte(data.(string)))
		return []byte(enc), "string", nil
	case map[string]interface{}:
		bytes, err := json.Marshal(data)
		return bytes, "map[string]interface{}", err
	case []interface{}:
		bytes, err := json.Marshal(data)
		return bytes, "[]interface{}", err
	default:
		return nil, "Unknown", fmt.Errorf("unsupported data type: %s", reflect.TypeOf(data))
	}
}

func detectAndConvertToAny(data []byte) any {
	jsonObject := make(map[string]interface{})
	err := json.Unmarshal(data, &jsonObject)
	if err == nil {
		return jsonObject
	}
	jsonArray := make([]map[string]interface{}, 0)
	err = json.Unmarshal(data, &jsonArray)
	if err == nil {

		return jsonArray
	}
	dec, err := base64.StdEncoding.DecodeString(string(data))
	if err == nil {
		return string(dec)
	} else {
		return data
	}
}
