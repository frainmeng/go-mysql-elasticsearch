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
	insertSqlTemplate = "INSERT INTO \"%s\".\"%s\"(%s) VALUES (%s);"
	updateSqlTemplate = "UPDATE \"%s\".\"%s\" SET %s WHERE %s;"
	deleteSqlTemplate = "DELETE FROM \"%s\".\"%s\" WHERE %s;"
)


type PGClient struct {
	db *sql.DB
}
type PGClientConfig struct {
	Host string
	Port int16
	User string
	Password string
	DBName string
}

func NewPGClient(conf *PGClientConfig) *PGClient   {
	c := new(PGClient)
	pgsqlInfo := fmt.Sprintf("host=%s port=%d user=%s "+ "password=%s dbname=%s sslmode=disable", conf.Host, conf.Port, conf.User, conf.Password, conf.DBName)
	db ,err := sql.Open("postgres",pgsqlInfo)
	if err != nil {
		panic(err)
	}

	c.db = db
	return c
}

type PGRequest struct {
	Action string
	Data map[string]interface{}
	Sql string
}



func (client *PGClient) Bulk(items []*BulkRequest) (err error)  {
	var tx *sql.Tx
	//同时处理多个请求需要开启事物
	if len(items) > 1 {
		tx,err = client.db.Begin()
		if err != nil {
			return errors.Trace(err)
		}
	}

	for _, item := range items {
		switch item.Action {
		case ActionIndex:
			err = client.Insert(item,tx)
		case ActionDelete:
			err = client.Delete(item,tx)
		case ActionUpdate:
			err = client.Update(item,tx)
		}
		if err != nil {
			err = errors.Trace(err)
			break
		}
	}
	if tx != nil {
		if err != nil {
			tx.Rollback()
		} else {
			err = tx.Commit()
		}
	}
	return
}

func (client *PGClient) Insert(request *BulkRequest,tx *sql.Tx) error  {
	return client.execInsert(request,tx)
}

func (client *PGClient) Delete(request *BulkRequest,tx *sql.Tx) error  {
	return client.execDelete(request,tx)
}

func (client *PGClient) Update(request *BulkRequest,tx *sql.Tx) error  {
	return client.execUpdate(request,tx)
}


func (client *PGClient) execInsert(request *BulkRequest,tx *sql.Tx) (err error)  {
	columns := make([]string,0,len(request.Data))
	valuePlaceholders := make([]string,0,len(request.Data))
	values := make([]interface{},0,len(request.Data))
	var i  = 1
	//组装插入数据
	for key, value := range request.Data {
		columns = append(columns,key)
		values = append(values,value)
		valuePlaceholders = append(valuePlaceholders,"$"+strconv.Itoa(i))
		i ++
	}

	insertSql := fmt.Sprintf(insertSqlTemplate,request.Index,request.Type,strings.Join(columns,","),strings.Join(valuePlaceholders,","))
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
	result,err:=stmt.Exec(values...)
	if err != nil {
		return errors.Trace(err)
	}
	log.Infof("pg insert event process success:%v",result)
	return
}

//执行删除操作
func (client *PGClient) execDelete(request *BulkRequest,tx *sql.Tx) (err error)  {
	whereExps := make([]string,0,len(request.PKData))
	whereValues := make([]interface{},0,len(request.PKData))
	var i  = 1
	for key, value := range request.PKData {
		whereExps = append(whereExps,key+"=$"+strconv.Itoa(i))
		whereValues = append(whereValues,value)
		i ++
	}
	deleteSql := fmt.Sprintf(deleteSqlTemplate,request.Index,request.Type,strings.Join(whereExps," AND "))
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
	result,err:=stmt.Exec(whereValues...)
	if err != nil {
		return errors.Trace(err)
	}
	log.Infof("pg delete event process success:%v",result)
	return
}

//执行更新操作
func (client *PGClient) execUpdate(request *BulkRequest,tx *sql.Tx) (err error)  {
	setExps := make([]string,0,len(request.Data))
	whereExps := make([]string,0,len(request.Data))
	values := make([]interface{},0,len(request.Data))
	var i  = 1
	//组装更新数据
	for key, value := range request.Data {
		setExps = append(setExps,key+"=$"+strconv.Itoa(i))
		values = append(values,value)
		i ++
	}
	//组装条件数据（主键）
	for key, value := range request.PKData {
		whereExps = append(whereExps,key+"=$"+strconv.Itoa(i))
		values = append(values,value)
		i ++
	}

	updateSql := fmt.Sprintf(updateSqlTemplate,request.Index,request.Type, strings.Join(setExps,","),strings.Join(whereExps," AND "))

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
	result,err:=stmt.Exec(values...)
	if err != nil {
		return errors.Trace(err)
	}
	log.Infof("pg update event process success:%v",result)
	return
}

