package elastic

import (
	"database/sql"
	"fmt"
	_ "github.com/lib/pq"
	"github.com/pingcap/errors"
	"github.com/siddontang/go-log/log"
	"strconv"
	"strings"
)

const (
	insertSqlTemplate = "INSERT INTO \"%s\".\"%s\"(%s) VALUES (%s) ON CONFLICT (%s) DO UPDATE SET %s;"
	updateSqlTemplate = "UPDATE \"%s\".\"%s\" SET %s WHERE %s;"
	deleteSqlTemplate = "DELETE FROM \"%s\".\"%s\" WHERE %s;"

	alterTableTemplate  = "ALTER TABLE \"%s\".\"%s\""
	addColumnTemplate   = "ADD COLUMN IF NOT EXISTS %s %s"
	dropColumnTemplate  = "DROP COLUMN IF EXISTS %s"
	alterColumnTemplate = "ALTER COLUMN %s TYPE %s"
)

type PGClient struct {
	db   *sql.DB
	Conf *PGClientConfig
}
type PGClientConfig struct {
	Host     string
	Port     uint16
	User     string
	Password string
	DBName   string
	MaxConn  int
}

func NewPGClient(conf *PGClientConfig) *PGClient {
	c := new(PGClient)
	c.Conf = conf
	pgSqlInfo := fmt.Sprintf("host=%s port=%d user=%s "+"password=%s dbname=%s sslmode=disable", conf.Host, conf.Port, conf.User, conf.Password, conf.DBName)
	db, err := sql.Open("postgres", pgSqlInfo)
	if err != nil {
		panic(err)
	}
	db.SetMaxOpenConns(conf.MaxConn)
	db.SetMaxIdleConns(conf.MaxConn)
	c.db = db
	return c
}

type PGRequest struct {
	Action string
	Data   map[string]interface{}
	Sql    string
}

func (client *PGClient) Bulk(items []*BulkRequest) (err error) {
	var tx *sql.Tx
	//同时处理多个请求需要开启事物
	if len(items) > 1 {
		tx, err = client.db.Begin()
		if err != nil {
			return errors.Trace(err)
		}
	}

	defer func() {
		if tx != nil {
			if err != nil {
				_ = tx.Rollback()
			} else {
				err = tx.Commit()
			}
		}
	}()

	for _, item := range items {
		switch item.Action {
		case ActionIndex:
			err = client.Insert(item, tx)
		case ActionDelete:
			err = client.Delete(item, tx)
		case ActionUpdate:
			err = client.Update(item, tx)
		}
		if err != nil {
			log.Errorf("execute postgresql request error! Schema[%s] Table[%s], Id[%s],error:[%v]", item.Index, item.Type, item.ID, err)
			err = errors.Trace(err)
			break
		}
	}
	return
}

func (client *PGClient) Request(item *BulkRequest) (err error) {
	switch item.Action {
	case ActionIndex:
		err = client.Insert(item, nil)
	case ActionDelete:
		err = client.Delete(item, nil)
	case ActionUpdate:
		err = client.Update(item, nil)
	}
	if err != nil {
		log.Errorf("execute postgresql request error! Schema[%s] Table[%s], Id[%s],error:[%v]", item.Index, item.Type, item.ID, err)
		err = errors.Trace(err)
	}
	return
}

func (client *PGClient) Insert(request *BulkRequest, tx *sql.Tx) error {
	return client.execInsert(request, tx)
}

func (client *PGClient) Delete(request *BulkRequest, tx *sql.Tx) error {
	return client.execDelete(request, tx)
}

func (client *PGClient) Update(request *BulkRequest, tx *sql.Tx) error {
	return client.execUpdate(request, tx)
}

func (client *PGClient) execInsert(request *BulkRequest, tx *sql.Tx) (err error) {
	upsertExps := make([]string, 0, len(request.Data))
	conflictExps := make([]string, 0, len(request.PKData))
	columns := make([]string, 0, len(request.Data))
	valuePlaceholders := make([]string, 0, len(request.Data))
	values := make([]interface{}, 0, len(request.Data))
	var i = 1
	//组装插入数据
	for key, value := range request.Data {
		columns = append(columns, key)
		values = append(values, value)
		valuePlaceholders = append(valuePlaceholders, "$"+strconv.Itoa(i))
		i++
	}

	//组装更新数据
	for key, _ := range request.Data {
		upsertExps = append(upsertExps, key+"=EXCLUDED."+key)
	}
	//组装条件数据（主键）
	for key, _ := range request.PKData {
		conflictExps = append(conflictExps, key)
	}
	insertSql := fmt.Sprintf(insertSqlTemplate,
		request.Index,
		request.Type,
		strings.Join(columns, ","),
		strings.Join(valuePlaceholders, ","),
		strings.Join(conflictExps, ","),
		strings.Join(upsertExps, ","))
	//创建stmt
	var stmt *sql.Stmt
	if tx != nil {
		stmt, err = tx.Prepare(insertSql)
	} else {
		stmt, err = client.db.Prepare(insertSql)
	}

	if err != nil {
		return errors.Trace(err)
	}
	//关闭stmt
	defer stmt.Close()
	result, err := stmt.Exec(values...)
	if err != nil {
		return errors.Trace(err)
	}
	log.Infof("pg %s event execute success! Schema[%s] Table[%s], Id[%s],result[%v],reqId[%v]", request.Action, request.Index, request.Type, request.ID, result, request.ReqId)
	return
}

//执行删除操作
func (client *PGClient) execDelete(request *BulkRequest, tx *sql.Tx) (err error) {
	whereExps := make([]string, 0, len(request.PKData))
	whereValues := make([]interface{}, 0, len(request.PKData))
	var i = 1
	for key, value := range request.PKData {
		whereExps = append(whereExps, key+"=$"+strconv.Itoa(i))
		whereValues = append(whereValues, value)
		i++
	}
	deleteSql := fmt.Sprintf(deleteSqlTemplate, request.Index, request.Type, strings.Join(whereExps, " AND "))
	var stmt *sql.Stmt
	if tx != nil {
		stmt, err = tx.Prepare(deleteSql)
	} else {
		stmt, err = client.db.Prepare(deleteSql)
	}
	if err != nil {
		return errors.Trace(err)
	}
	defer stmt.Close()
	result, err := stmt.Exec(whereValues...)
	if err != nil {
		return errors.Trace(err)
	}
	log.Infof("pg %s event execute success! Schema[%s] Table[%s], Id[%s],result[%v],reqId[%v]", request.Action, request.Index, request.Type, request.ID, result, request.ReqId)
	return
}

//执行更新操作
func (client *PGClient) execUpdate(request *BulkRequest, tx *sql.Tx) (err error) {
	setExps := make([]string, 0, len(request.Data))
	whereExps := make([]string, 0, len(request.Data))
	values := make([]interface{}, 0, len(request.Data))
	var i = 1
	//组装更新数据
	for key, value := range request.Data {
		setExps = append(setExps, key+"=$"+strconv.Itoa(i))
		values = append(values, value)
		i++
	}
	//组装条件数据（主键）
	for key, value := range request.PKData {
		whereExps = append(whereExps, key+"=$"+strconv.Itoa(i))
		values = append(values, value)
		i++
	}

	updateSql := fmt.Sprintf(updateSqlTemplate, request.Index, request.Type, strings.Join(setExps, ","), strings.Join(whereExps, " AND "))

	//创建stmt
	var stmt *sql.Stmt
	if tx != nil {
		stmt, err = tx.Prepare(updateSql)
	} else {
		stmt, err = client.db.Prepare(updateSql)
	}
	if err != nil {
		return errors.Trace(err)
	}
	//关闭stmt
	defer stmt.Close()
	result, err := stmt.Exec(values...)
	if err != nil {
		return errors.Trace(err)
	}
	log.Infof("pg %s event execute success! Schema[%s] Table[%s], Id[%s],result[%v],reqId[%v]", request.Action, request.Index, request.Type, request.ID, result, request.ReqId)
	return
}

//表结构查询语句
var tableSchemaQuery = `SELECT 
		column_name,
		data_type,
		character_maximum_length,
		numeric_precision,
		numeric_scale,
		column_default,
		is_nullable
	FROM information_schema.columns
	WHERE table_schema = $1
	  AND table_name   = $2
	ORDER BY ordinal_position ASC;`

//DataTypeMap mysql数据类型到pg数据类型映射
var DataTypeMap = map[string]string{
	//数值类型
	"tinyint":   "smallint",
	"smallint":  "smallint",
	"year":      "smallint",
	"mediumint": "integer",
	"int":       "integer",
	"bigint":    "bigint",

	//浮点型
	"float":  "real",
	"double": "double precision",

	//字符串
	"char":       "character",
	"varchar":    "character varying",
	"tinytext":   "text",
	"text":       "text",
	"mediumtext": "text",
	"longtext":   "text",
	//二进制
	"tinyblob":   "bytea",
	"blob":       "bytea",
	"mediumblob": "bytea",
	"longblob":   "bytea",

	//时间日期
	"datetime":  "timestamp without time zone",
	"timestamp": "timestamp with time zone",
	"date":      "date",
	"time":      "time",

	//bit
	"bit": "bit",

	//json
	"json": "json",

	//decimal
	"decimal": "numeric",
}

type TableColumn struct {
	Name         string
	DataType     string
	Collation    string
	DefaultValue interface{}
	IsNullAble   bool
}

// 表结构
type TableInfo struct {
	Schema  string
	Name    string
	Columns map[string]TableColumn
}

//TableSchema
type TableSchema struct {
	tableSchema            string
	tableName              string
	ordinalPosition        int
	columnName             string
	dataType               string
	characterMaximumLength sql.NullInt64
	numericPrecision       sql.NullInt64
	numericScale           sql.NullInt64
	isNullable             string
	columnDefault          sql.NullString
	description            sql.NullString
}

//SyncTable 同步表结构
func (client *PGClient) SyncTable(mysqlTableInfo *TableInfo, pgSchema string, pgTable string, skipAction []string) error {
	//获取当前pg表结构
	targetTableInfo, err := client.getTable(pgSchema, pgTable)
	if err != nil {
		return errors.Trace(err)
	}
	//将mysql表结构转化为pg表结构
	sourceTableInfo := convert(mysqlTableInfo, pgSchema, pgTable)

	//获取差异
	addColumns, dropColumns, alterColumns := getChange(sourceTableInfo, targetTableInfo, skipAction)

	sqlStatement := generateAlertSql(addColumns, dropColumns, alterColumns, pgSchema, pgTable)
	if len(sqlStatement) == 0 {
		log.Infof("表结构未发生变化，无需同步:mysql[%s.%s],pg[%s.%s]", mysqlTableInfo.Schema, mysqlTableInfo.Name, pgSchema, pgTable)
	} else {
		log.Infof("同步表结构语句：\n%s", sqlStatement)
	}
	_, err = client.db.Exec(sqlStatement)
	if err != nil {
		log.Errorf("同步表结构失败:%v", err)
		err = errors.Trace(err)
	} else {
		log.Infof("表结构同步成功！！！")
	}
	return err
}

//convert 将mysql表结构转换为pg表结构
func convert(mysqlTableInfo *TableInfo, tableSchema string, tableName string) *TableInfo {
	columns := make(map[string]TableColumn)
	for _, mysqlCol := range mysqlTableInfo.Columns {
		rawType := mysqlCol.DataType

		mysqlDataType := mysqlCol.DataType
		i := strings.Index(mysqlDataType, "(")
		var dataTypeSubfix string
		if i > -1 {
			dataTypeSubfix = mysqlDataType[i:]
			mysqlDataType = mysqlDataType[:i]

		}
		pgDataType, ok := DataTypeMap[mysqlDataType]
		if !ok {
			pgDataType = mysqlDataType
		}

		if !(strings.Contains(rawType, "int") || strings.HasPrefix(rawType, "year")) {
			rawType = pgDataType + dataTypeSubfix
		} else {
			rawType = pgDataType
		}

		columns[mysqlCol.Name] = TableColumn{Name: mysqlCol.Name, DataType: rawType, DefaultValue: mysqlCol.DefaultValue, Collation: mysqlCol.Collation, IsNullAble: mysqlCol.IsNullAble}
	}
	return &TableInfo{Schema: tableSchema, Name: tableName, Columns: columns}
}

//getTable 获取表结构
func (client *PGClient) getTable(schema string, table string) (*TableInfo, error) {
	stmt, err := client.db.Prepare(tableSchemaQuery)
	if err != nil {
		return nil, errors.Trace(err)
	}
	defer stmt.Close()
	rows, err := stmt.Query(schema, table)
	if err != nil {
		return nil, errors.Trace(err)
	}
	data := make(map[string]TableColumn)

	row := new(TableSchema)
	//数据处理
	for rows.Next() {
		err = rows.Scan(&row.columnName, &row.dataType, &row.characterMaximumLength, &row.numericPrecision, &row.numericScale, &row.columnDefault, &row.isNullable)
		if err != nil {
			panic(err)
		}
		column := TableColumn{Name: row.columnName, DataType: row.dataType}
		//字符类型需要处理长度问题
		if row.characterMaximumLength.Valid {
			column.DataType = fmt.Sprintf("%s(%v)", row.dataType, row.characterMaximumLength.Int64)
		}
		//decimal/numeric需要处理长度和精度问题
		if row.dataType == "decimal" || row.dataType == "numeric" {
			column.DataType = fmt.Sprintf("%s(%v,%v)", row.dataType, row.numericPrecision.Int64, row.numericScale.Int64)
		}

		column.IsNullAble = row.isNullable == "YES"
		if row.columnDefault.Valid {
			defaultV := row.columnDefault.String
			startIndex := strings.Index(defaultV, "'")
			endIndex := strings.LastIndex(defaultV, "'")
			if startIndex > 0 && endIndex != startIndex {
				defaultV = defaultV[startIndex+1 : endIndex]
			}
		}
		data[row.columnName] = column
	}
	return &TableInfo{Schema: schema, Name: table, Columns: data}, nil
}

//获取发生变化的列
func getChange(sourceTable, targetTable *TableInfo, skipAction []string) (addColumns, dropColumns, alterColumns []TableColumn) {

	var skipAdd = false
	var skipDrop = false
	var skipAlter = false
	if len(skipAction) > 0 {
		for _, action := range skipAction {
			switch action {
			case "ADD":
				skipAdd = true
			case "DROP":
				skipDrop = true
			case "ALTER":
				skipAlter = true
			}
		}
	}

	addColumns = make([]TableColumn, 0)
	dropColumns = make([]TableColumn, 0)
	alterColumns = make([]TableColumn, 0)

	targetColumns := make(map[string]TableColumn)
	for k, v := range targetTable.Columns {
		targetColumns[k] = v
	}

	for columnName, sourceCol := range sourceTable.Columns {
		targetCol, ok := targetColumns[columnName]
		if ok {
			if !skipAlter && sourceCol.DataType != targetCol.DataType {
				alterColumns = append(alterColumns, sourceCol)
			}
			delete(targetColumns, columnName)
		} else if !skipAdd {
			addColumns = append(addColumns, sourceCol)
		}
	}
	if !skipDrop && len(targetColumns) > 0 {
		for _, v := range targetColumns {
			dropColumns = append(dropColumns, v)
		}
	}
	return
}

func generateAlertSql(addColumns, dropColumns, alterColumns []TableColumn, tableSchema string, tableName string) string {
	var sqlStatement string
	columnSql := make([]string, 0)

	if len(addColumns) > 0 {
		for _, col := range addColumns {
			if col.DefaultValue != nil {
				columnSql = append(columnSql, fmt.Sprintf(addColumnTemplate+" DEFAULT '%s'", col.Name, col.DataType, col.DefaultValue))
			} else {
				columnSql = append(columnSql, fmt.Sprintf(addColumnTemplate, col.Name, col.DataType))
			}
		}
	}

	if len(dropColumns) > 0 {
		for _, col := range dropColumns {
			columnSql = append(columnSql, fmt.Sprintf(dropColumnTemplate, col.Name))
		}
	}

	if len(alterColumns) > 0 {
		for _, col := range alterColumns {
			columnSql = append(columnSql, fmt.Sprintf(alterColumnTemplate, col.Name, col.DataType))
		}
	}
	if len(columnSql) > 0 {
		sqlStatement = fmt.Sprintf(alterTableTemplate, tableSchema, tableName) + "\n" + strings.Join(columnSql, ",\n")
	}
	return sqlStatement
}
