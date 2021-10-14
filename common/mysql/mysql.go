// example
//
// package main
//
// import (
// 	"context"
// 	"fmt"
// 	"mysql"
// 	"time"
// )
//
// func main() {
// 	conf := mysql.MysqlConf{
// 		Address:      "oss:oss_da@tcp(10.206.30.122:3745)/dbWMP_v3?charset=utf8&allowOldPasswords=1",
// 		Timeout:      1 * time.Second,
// 		MaxIdleConns: 10,
// 		MaxOpenConns: 100,
// 	}
//
// 	ms := mysql.New(conf)
// 	if ms == nil {
// 		fmt.Println("create mysql client failed")
// 	}
// 	res, err := ms.QueryString(context.Background(), "select iStatus,iDocID from tbNewsBaseInfo limit 10")
// 	if err != nil {
// 		fmt.Printf("mysql query failed. info:%s err:%s\n", ms.String(), err.Error())
// 		return
// 	}
//
// 	fmt.Println(res)
// }

// author: tonytang
// desc: modify from going/codec/mysql/mysql.go
package mysql

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"sync"
	"time"

	_ "github.com/go-sql-driver/mysql" // init mysql driver
)

//	[mysql]
//	address="video_chat_test:video_chat_test@tcp(10.198.30.120:3845)/video_chat"
//	timeout="800ms"
//	maxIdleConns=10    // 设置最大空闲连接数
//	maxOpenConns=10000 // 设置最大打开连接数
//
type MysqlConf struct {
	Address      string
	Timeout      time.Duration `default:"500ms"`
	MaxIdleConns int
	MaxOpenConns int
}

// Mysql mysql数据结构体
type Mysql struct {
	db           *sql.DB
	addr         string
	timeout      time.Duration
	maxIdleConns int
	maxOpenConns int

	cmd  string
	cost time.Duration
	err  error
}

var once sync.Once

// New 新建一个mysql结构体
func New(conf MysqlConf) *Mysql {
	m := &Mysql{
		addr:         conf.Address,
		timeout:      conf.Timeout,
		maxIdleConns: conf.MaxIdleConns,
		maxOpenConns: conf.MaxOpenConns,
	}
	var e error
	m.db, e = DB(conf)
	if e != nil {
		return nil
	}

	return m
}

// String debug string
func (m *Mysql) String() string {
	if m.err != nil {
		return fmt.Sprintf("mysql[%s], addr[%s], cost[%s], error[%v]", m.cmd, m.addr, m.cost, m.err)
	}
	return fmt.Sprintf("mysql[%s], addr[%s], cost[%s]", m.cmd, m.addr, m.cost)
}

// DB 全局db，默认配置 [mysql]
func DB(conf MysqlConf) (*sql.DB, error) {
	var e error
	var db *sql.DB

	db, e = sql.Open("mysql", conf.Address)
	if e != nil {
		return nil, e
	}

	if conf.MaxIdleConns > 0 {
		db.SetMaxIdleConns(conf.MaxIdleConns)
	}
	if conf.MaxOpenConns > 0 {
		db.SetMaxOpenConns(conf.MaxOpenConns)
	}

	return db, nil
}

// Query 用于 select
func (m *Mysql) Query(ctx context.Context, query string, args ...interface{}) (*sql.Rows, error) {
	if m.db == nil {
		m.err = errors.New("db empty")
		return nil, m.err
	}
	ctx, _ = context.WithTimeout(ctx, m.timeout)
	begin := time.Now()
	m.cmd = strings.Replace(query, "?", "%v", -1)
	m.cmd = fmt.Sprintf(m.cmd, args...)

	rows, err := m.db.QueryContext(ctx, query, args...)
	m.err = err
	m.cost = time.Now().Sub(begin)
	if err != nil {
		return nil, err
	}

	return rows, err
}

//映射select结果到map[string]string
func (m *Mysql) QueryString(ctx context.Context, query string, args ...interface{}) ([]map[string]string, error) {
	if m.db == nil {
		m.err = errors.New("db empty")
		return nil, m.err
	}

	var res []map[string]string
	ctx, _ = context.WithTimeout(ctx, m.timeout)
	begin := time.Now()
	m.cmd = strings.Replace(query, "?", "%v", -1)
	m.cmd = fmt.Sprintf(m.cmd, args)
	rows, err := m.db.QueryContext(ctx, query, args...)
	m.cost = time.Now().Sub(begin)
	if err != nil {
		m.err = err
		return res, err
	}
	defer rows.Close()
	columns, err := rows.Columns()
	if err != nil {
		m.err = err
		return res, err
	}
	scanArgs := make([]interface{}, len(columns))
	values := make([]interface{}, len(columns))
	for j := range values {
		scanArgs[j] = &values[j]
	}

	for rows.Next() {
		//将行数据保存到record字典
		record := make(map[string]string)
		err = rows.Scan(scanArgs...)
		if err != nil {
			return res, err
		}
		for i, col := range values {
			if col != nil {
				record[columns[i]] = string(col.([]byte))
			}
		}
		res = append(res, record)
	}
	return res, nil
}

// Exec 用于 insert delete update
func (m *Mysql) Exec(ctx context.Context, query string, args ...interface{}) (sql.Result, error) {
	if m.db == nil {
		m.err = errors.New("db empty")
		return nil, m.err
	}
	ctx, _ = context.WithTimeout(ctx, m.timeout)
	begin := time.Now()
	m.cmd = strings.Replace(query, "?", "%v", -1)
	m.cmd = fmt.Sprintf(m.cmd, args...)

	result, err := m.db.ExecContext(ctx, query, args...)
	m.err = err
	m.cost = time.Now().Sub(begin)
	if err != nil {
		return nil, err
	}

	return result, err
}

// Select SelectAndScan 传入自己定义的行记录结构体，每个key代表mysql字段名，通过反射提取出所有字段，并select然后scan返回该结构体数据数组
//	如：
//	type Data struct {
//	    ID   string `sql:"f_id"`
//	    Name string `sql:"f_name"`
//	}
//	rows, err := m.Select(ctx, (*Data)(nil), "table_name where f_id = ?", 1)
func (m *Mysql) Select(ctx context.Context, rowStruct interface{}, tableNameAndWhere string, args ...interface{}) ([]interface{}, error) {
	if m.db == nil {
		m.err = errors.New("db empty")
		return nil, m.err
	}
	typ := reflect.TypeOf(rowStruct).Elem()
	var fieldNames []string
	for i := 0; i < typ.NumField(); i++ {
		if name, ok := typ.Field(i).Tag.Lookup("sql"); ok {
			fieldNames = append(fieldNames, name)
		}
	}
	if len(fieldNames) == 0 {
		m.err = errors.New("fields empty")
		return nil, m.err
	}
	query := fmt.Sprintf("select %s from %s", strings.Join(fieldNames, ","), tableNameAndWhere)
	rows, err := m.Query(ctx, query, args...)
	if err != nil {
		m.err = err
		return nil, err
	}
	defer rows.Close()

	var datas []interface{}
	for rows.Next() {
		row := reflect.New(typ)
		val := row.Elem()
		var fieldValues []interface{}
		for i := 0; i < typ.NumField(); i++ {
			if _, ok := typ.Field(i).Tag.Lookup("sql"); ok {
				field := val.Field(i)
				if field.Kind() != reflect.Ptr && field.CanAddr() {
					fieldValues = append(fieldValues, field.Addr().Interface())
				} else {
					fieldValues = append(fieldValues, field.Interface())
				}
			}
		}
		err = rows.Scan(fieldValues...)
		if err != nil {
			m.err = err
			return nil, err
		}
		datas = append(datas, row.Interface())
	}

	return datas, nil
}

// Insert 插入数据行
func (m *Mysql) Insert(ctx context.Context, rowStruct interface{}, tableName string) error {
	if m.db == nil {
		m.err = errors.New("db empty")
		return m.err
	}
	typ := reflect.TypeOf(rowStruct).Elem()
	val := reflect.ValueOf(rowStruct).Elem()
	var fieldNames []string
	var placeHolder []string
	var fieldValues []interface{}
	for i := 0; i < typ.NumField(); i++ {
		if name, ok := typ.Field(i).Tag.Lookup("sql"); ok {
			fieldNames = append(fieldNames, name)
			placeHolder = append(placeHolder, "?")
			field := val.Field(i)
			if field.Kind() != reflect.Ptr && field.CanAddr() {
				fieldValues = append(fieldValues, field.Addr().Interface())
			} else {
				fieldValues = append(fieldValues, field.Interface())
			}
		}
	}
	if len(fieldNames) == 0 {
		m.err = errors.New("fields empty")
		return m.err
	}

	query := fmt.Sprintf("insert into %s (%s) values (%s)", tableName, strings.Join(fieldNames, ","), strings.Join(placeHolder, ","))
	_, err := m.Exec(ctx, query, fieldValues...)
	if err != nil {
		m.err = err
		return err
	}

	return nil
}
