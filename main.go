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

func getColumnNames(O map[string]interface{}) []string {
	columns := make([]string, 0, len(O))
	for c := range O {
		columns = append(columns, c)
	}
	sort.Strings(columns)
	return columns
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

func getColumnDataType(name string, val interface{}) string {
	dtype := ""
	if name == "_id" {
		dtype = " PRIMARY KEY"
	}
	switch val.(type) {
	case int, int8, int16, int32, int64:
		return "INTEGER" + dtype
	case float32, float64:
		return "FLOAT" + dtype
	case bool:
		return "BOOLEAN" + dtype
	default:
		return "VARCHAR(255)" + dtype
	}
}

func generateCreateSchemaSQL(schema string) string {
	return fmt.Sprintf("CREATE SCHEMA %s;", schema)
}

func generateCreateTableSQL(e OplogEntry) string {
	columns := getColumnNames(e.O)
	cs := make([]string, 0, len(columns))
	for _, c := range columns {
		v := e.O[c]
		dtype := getColumnDataType(c, v)
		cs = append(cs, fmt.Sprintf("%s %s", c, dtype))
	}

	return fmt.Sprintf("CREATE TABLE %s (%s);", e.Ns, strings.Join(cs, ", "))
}

func generateInsertSQL(e OplogEntry) (string, error) {
	columns := getColumnNames(e.O)
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

func generateSQL(entry OplogEntry, cacheMap map[string]bool) ([]string, error) {
	commands := []string{}

	switch entry.Op {
	case "i":
		nsParts := strings.Split(entry.Ns, ".")
		if exists := cacheMap[nsParts[0]]; !exists {
			commands = append(commands, generateCreateSchemaSQL(nsParts[0]))
			cacheMap[nsParts[0]] = true
		}

		if exists := cacheMap[entry.Ns]; !exists {
			commands = append(commands, generateCreateTableSQL(entry))
			cacheMap[entry.Ns] = true
		}

		c, err := generateInsertSQL(entry)
		if err != nil {
			return commands, err
		}
		commands = append(commands, c)
	case "u":
		c, err := generateUpdateSQL(entry)
		if err != nil {
			return commands, err
		}
		commands = append(commands, c)
	case "d":
		c, err := generateDeleteSQL(entry)
		if err != nil {
			return commands, err
		}
		commands = append(commands, c)
	}

	return commands, nil
}

func ConvertToSQL(oplog string) ([]string, error) {
	commands := []string{}
	var entries []OplogEntry
	if err := json.Unmarshal([]byte(oplog), &entries); err != nil {
		var entry OplogEntry
		if err := json.Unmarshal([]byte(oplog), &entry); err != nil {
			return commands, err
		}

		entries = append(entries, entry)
	}

	cacheMap := make(map[string]bool)
	for _, entry := range entries {
		innerSqls, err := generateSQL(entry, cacheMap)
		if err != nil {
			return []string{}, err
		}
		commands = append(commands, innerSqls...)
	}

	return commands, nil
}
