package networking

import (
	"encoding/json"
	"fmt"
	"time"
)

func ConvertJsonValueToFloat64(value interface{}) (float64, error) {

	valueJN, ok := value.(json.Number)
	if !ok {
		return 0, fmt.Errorf("wrong json number type: %v", value)
	}

	res, err := valueJN.Float64()
	if err != nil {
		return 0, fmt.Errorf("json.Number.Float64: %v", err)
	}

	return res, nil
}

func ConvertJsonValueToInt64(value interface{}) (int64, error) {

	valueJN, ok := value.(json.Number)
	if !ok {
		return 0, fmt.Errorf("wrong json number type: %v", value)
	}

	res, err := valueJN.Int64()
	if err != nil {
		return 0, fmt.Errorf("json.Number.Int64: %v", err)
	}

	return res, nil
}

func ConvertJsonValueToString(value interface{}) (string, error) {

	valueJN, ok := value.(string)
	if !ok {
		return "", fmt.Errorf("wrong json string type: %v", value)
	}

	return valueJN, nil
}

func ConvertJsonValueToTime(value interface{}) (time.Time, error) {

	valueString, err := ConvertJsonValueToString(value)
	if err != nil {
		return time.Time{}, err
	}

	res, err := time.Parse(time.RFC3339, valueString)
	if err != nil {
		return res, fmt.Errorf("time.Parse: %v", err)
	}

	return res, nil
}
