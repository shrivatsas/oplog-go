package main

import (
	"encoding/json"
	"fmt"
	"sort"
	"strings"
)

type OplogEntry struct {
	Op string                 `json:"op"`
	Ns string                 `json:"ns"`
	O  map[string]interface{} `json:"o"`
}

func getColumnValue(val interface{}) string {
	switch val.(type) {
	case string:
		return fmt.Sprintf("'%s'", val)
	case int, float32, float64:
		return fmt.Sprintf("%v", val)
	case bool:
		return fmt.Sprintf("%t", val)
	case nil:
		return "NULL"
	default:
		return fmt.Sprintf("'%s'", val)
	}
}

func ConvertToSQL(oplog string) (string, error) {
	var entry OplogEntry
	if err := json.Unmarshal([]byte(oplog), &entry); err != nil {
		return "", err
	}

	columns := make([]string, 0, len(entry.O))
	for c := range entry.O {
		columns = append(columns, c)
	}

	sort.Strings(columns)
	values := make([]string, 0, len(entry.O))
	for _, c := range columns {
		values = append(values, getColumnValue(entry.O[c]))
	}

	switch entry.Op {
	case "i":
		return fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s);", entry.Ns,
			strings.Join(columns, ", "),
			strings.Join(values, ", ")), nil
	}

	return "", nil
}
