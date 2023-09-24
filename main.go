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
	O2 map[string]interface{} `json:"o2"`
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

func generateInsertSQL(e OplogEntry) (string, error) {
	columns := make([]string, 0, len(e.O))
	for c := range e.O {
		columns = append(columns, c)
	}

	sort.Strings(columns)
	values := make([]string, 0, len(e.O))
	for _, c := range columns {
		values = append(values, getColumnValue(e.O[c]))
	}

	return fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s);", e.Ns,
		strings.Join(columns, ", "),
		strings.Join(values, ", ")), nil
}

func generateUpdateSQL(e OplogEntry) (string, error) {
	diffMap, ok := e.O["diff"].(map[string]interface{})
	if !ok {
		return "", fmt.Errorf("invalid oplog: No 'diff'")
	}

	sql := ""
	if setMap, ok := diffMap["u"].(map[string]interface{}); ok {
		columns := make([]string, 0, len(setMap))
		for n, v := range setMap {
			columns = append(columns, fmt.Sprintf("%s = %s", n, getColumnValue(v)))
		}

		sort.Strings(columns)
		sql = fmt.Sprintf("UPDATE %s SET %s", e.Ns, strings.Join(columns, ", "))
	} else if unsetMap, ok := diffMap["d"].(map[string]interface{}); ok {
		columns := make([]string, 0, len(unsetMap))
		for n := range unsetMap {
			columns = append(columns, fmt.Sprintf("%s = NULL", n))
		}

		sort.Strings(columns)
		sql = fmt.Sprintf("UPDATE %s SET %s", e.Ns, strings.Join(columns, ", "))
	} else {
		return "", fmt.Errorf("invalid oplog: No 'u' or 'd'")
	}

	where := make([]string, 0, len(e.O2))
	for n, v := range e.O2 {
		where = append(where, fmt.Sprintf("%s = %s", n, getColumnValue(v)))
	}

	return fmt.Sprintf("%s WHERE %s;", sql,
		strings.Join(where, " AND ")), nil
}

func generateDeleteSQL(e OplogEntry) (string, error) {
	columns := make([]string, 0, len(e.O))
	for n, v := range e.O {
		columns = append(columns, fmt.Sprintf("%s = %s", n, getColumnValue(v)))
	}

	sort.Strings(columns)

	return fmt.Sprintf("DELETE FROM %s WHERE %s;", e.Ns, strings.Join(columns, " AND ")), nil
}

func ConvertToSQL(oplog string) (string, error) {
	var entry OplogEntry
	if err := json.Unmarshal([]byte(oplog), &entry); err != nil {
		return "", err
	}

	switch entry.Op {
	case "i":
		return generateInsertSQL(entry)
	case "u":
		return generateUpdateSQL(entry)
	case "d":
		return generateDeleteSQL(entry)
	}

	return "", fmt.Errorf("invalid oplog")
}
