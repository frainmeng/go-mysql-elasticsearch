package river

import (
	"bytes"
	"encoding/json"
	"fmt"
	"github.com/siddontang/go/hack"
	"reflect"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/frainmeng/go-mysql/canal"
	"github.com/frainmeng/go-mysql/mysql"
	"github.com/frainmeng/go-mysql/replication"
	"github.com/frainmeng/go-mysql/schema"
	"github.com/juju/errors"
	"github.com/siddontang/go-log/log"
	"github.com/siddontang/go-mysql-elasticsearch/elastic"
)

const (
	syncInsertDoc = iota
	syncDeleteDoc
	syncUpdateDoc
)

const (
	fieldTypeList = "list"
	// for the mysql int type to es date type
	// set the [rule.field] created_time = ",date"
	fieldTypeDate = "date"
)

const mysqlDateFormat = "2006-01-02"

type posSaver struct {
	pos   mysql.Position
	force bool
	reqId uint64
}

type ack struct {
	reqId uint64
	err   error
}

type syncTable struct {
	schema, table string
}

type mtsWait struct {
	nextPos        mysql.Position
	LastCommitted  int64
	SequenceNumber int64
}

type eventHandler struct {
	r *River
}

func (h *eventHandler) OnRotate(e *replication.RotateEvent) error {
	pos := mysql.Position{
		Name: string(e.NextLogName),
		Pos:  uint32(e.Position),
	}

	h.r.syncCh <- posSaver{pos, true, 0}

	return h.r.ctx.Err()
}

func (h *eventHandler) OnTableChanged(schema, table string) error {
	err := h.r.updateRule(schema, table)
	if err != nil && err != ErrRuleNotExist {
		return errors.Trace(err)
	}
	h.r.syncCh <- syncTable{schema, table}
	return nil
}

func (h *eventHandler) OnDDL(nextPos mysql.Position, e *replication.QueryEvent) error {
	dll := string(e.Query)
	fmt.Println(dll)

	expAlterTable := regexp.MustCompile("(?i)^ALTER\\sTABLE\\s.*?`{0,1}(.*?)`{0,1}\\.{0,1}`{0,1}([^`\\.]+?)`{0,1}\\s.*")
	mb := expAlterTable.FindSubmatch(e.Query)

	mbLen := len(mb)
	if mbLen > 0 {
		var db string
		if len(mb[mbLen-2]) == 0 {
			db = string(e.Schema)
		} else {
			db = string(mb[mbLen-2])
		}
		table := string(mb[mbLen-1])

		fmt.Println(db)
		fmt.Println(table)
	}

	h.r.syncCh <- posSaver{nextPos, true, 0}
	return h.r.ctx.Err()
}

func (h *eventHandler) OnXID(nextPos mysql.Position) error {
	h.r.syncCh <- posSaver{nextPos, false, 0}
	return h.r.ctx.Err()
}

func (h *eventHandler) OnRow(e *canal.RowsEvent) error {
	rule, ok := h.r.rules[ruleKey(e.Table.Schema, e.Table.Name)]
	if !ok {
		return nil
	}
	for _, action := range rule.SkipActions {
		if e.Action == action {
			return nil
		}
	}

	var reqs []*elastic.BulkRequest
	var err error
	switch e.Action {
	case canal.InsertAction:
		reqs, err = h.r.makeInsertRequest(rule, e.Rows)
	case canal.DeleteAction:
		reqs, err = h.r.makeDeleteRequest(rule, e.Rows)
	case canal.UpdateAction:
		reqs, err = h.r.makeUpdateRequest(rule, e.Rows)
	default:
		err = errors.Errorf("invalid rows action %s", e.Action)
	}

	if err != nil {
		h.r.cancel()
		return errors.Errorf("make %s postgresql request err %v, close sync", e.Action, err)
	}
	for _, req := range reqs {
		req.Pos = e.Header.LogPos
		req.Timestamp = e.Header.Timestamp
	}
	h.r.syncCh <- reqs

	return h.r.ctx.Err()
}

func (h *eventHandler) OnGTID(gtid mysql.GTIDSet) error {
	return nil
}

func (h *eventHandler) OnGTIDMTS(nextPos mysql.Position, gtidEvent *replication.GTIDEvent) error {
	h.r.syncCh <- mtsWait{nextPos, gtidEvent.LastCommitted, gtidEvent.SequenceNumber}
	return nil
}

func (h *eventHandler) OnPosSynced(pos mysql.Position, set mysql.GTIDSet, force bool) error {
	return nil
}

func (h *eventHandler) String() string {
	return "ESRiverEventHandler"
}

func (r *River) syncLoop() {
	defer r.wg.Done()
	wg := sync.WaitGroup{}
	var mts mtsWait
	//pos := mysql.Position{Name: r.master.Name, Pos: r.master.Pos}
	//reqId := uint64(0)
	reqs := make([]*elastic.BulkRequest, 0)

	for {
		select {
		case v := <-r.syncCh:
			switch v := v.(type) {
			case posSaver:
				r.posChan <- v
				//pos = v.pos
			case syncTable:
				r.syncTableStructure(v.schema, v.table)
			case mtsWait:
				if len(reqs) > 0 {
					//异步并发执行
					wg.Add(1)
					go r.asyncDoPGRequest(reqs, &wg)
					//重置reqs，
					reqs = make([]*elastic.BulkRequest, 0)
				}

				//LastCommitted变更需要等待上一个commit执行完
				if mts.LastCommitted != v.LastCommitted {
					wg.Wait()
					mts = v
				}
			case []*elastic.BulkRequest:
				for _, req := range v {
					//reqId++
					////重置 reqId
					//if reqId == util.MAX_REQ_ID {
					//	reqId = uint64(0)
					//}
					////position
					//pos.Pos = req.Pos
					////posNow := mysql.Position{
					////	Name: string(pos.Name),
					////	Pos:  uint32(pos.Pos),
					////}
					////pos chan
					//r.posChan <- posSaver{force: false, reqId: reqId}
					////set reqId
					//req.ReqId = reqId
					////data chan
					//r.dataChans[req.Hash()%r.c.ConcurrentSize] <- req
					reqs = append(reqs, req)
				}
			}
		case <-r.ctx.Done():
			return
		}
	}
}

//同步数据
func (r *River) syncData(dataChan chan *elastic.BulkRequest, ackChan chan *ack, name string) {
	log.Infof("start sync data routine[%s]", name)
	for {
		select {
		case req := <-dataChan:
			//log.Infof("sync routine[%s] received req[%v]", name, req.ReqId)
			err := r.doPGRequest(req)
			ackChan <- &ack{req.ReqId, err}
		case <-r.ctx.Done():
			log.Infof("close sync data routine[%s]", name)
			return
		}
	}
}

//position 处理
func (r *River) posProcessor(posChan chan posSaver, ackChan chan *ack) {
	defer r.wg.Done()
	waitClose := false
	//ackCache := make(map[uint64]*ack)
	ticket := time.NewTicker(time.Millisecond * 5000)
	var err error
	log.Info("start position processor routine")
	for {
		select {
		case pos := <-posChan:
			if !waitClose {
				if pos.reqId == 0 {
					r.master.SetPos(pos.pos)
				} else {
					//log.Infof("current reqId[%v]", pos.reqId)
					//ack := getAck(ackChan, pos.reqId, ackCache)
					//if ack.err == nil {
					//	//r.master.SetPos(pos.pos)
					//} else {
					//	err = ack.err
					//}
				}
				if pos.force {
					err = r.master.SavePos()
				}
			} else {
				log.Infof("wait for close, ignore pos data...")
			}
		case <-ticket.C:
			err = r.master.SavePos()
		case <-r.Ctx().Done():
			log.Info("close position routine")
			return
		}

		if err != nil {
			r.cancel()
			waitClose = true
		}
	}
}

//获取ack
func getAck(ackChan chan *ack, reqId uint64, ackCache map[uint64]*ack) *ack {
	for {
		ack, ok := ackCache[reqId]
		if ok {
			delete(ackCache, reqId)
			return ack
		}
		select {
		case ack := <-ackChan:
			if ack.reqId == reqId {
				return ack
			} else {
				ackCache[ack.reqId] = ack
			}
		}
	}
}

// 同步表结构,需要等待缓存中的数据处理完成
func (r *River) syncTableStructure(syncTableSchema string, syncTableName string) {
	//等待缓存数据处理完成
	for len(r.posChan) > 0 {
		time.Sleep(time.Millisecond * 200)
	}
	tableInfo, _ := r.getMysqlTable(syncTableSchema, syncTableName)
	//fmt.Println(tableInfo)
	rule, ok := r.rules[ruleKey(syncTableSchema, syncTableName)]
	if ok {
		var pg *elastic.PGClient
		if len(rule.PGName) > 0 {
			pg, _ = r.pgs[rule.PGName]
		} else {
			pg = r.pg
		}
		if pg != nil {
			if err := pg.SyncTable(tableInfo, rule.PGSchema, rule.PGTable, rule.SkipAlterActions); err != nil {
				log.Errorf("sync table struct err: %v, close sync", err)
				r.cancel()
			}
		}
		rule.prepareDataRouter()
	}
}

// for insert and delete
func (r *River) makeRequest(rule *Rule, action string, rows [][]interface{}) ([]*elastic.BulkRequest, error) {
	reqs := make([]*elastic.BulkRequest, 0, len(rows))

	for _, values := range rows {
		id, err := r.getDocID(rule, values)
		if err != nil {
			return nil, errors.Trace(err)
		}

		parentID := ""
		if len(rule.Parent) > 0 {
			if parentID, err = r.getParentID(rule, values, rule.Parent); err != nil {
				return nil, errors.Trace(err)
			}
		}

		pgTable := rule.Table
		if len(rule.PGTable) > 0 {
			pgTable = rule.PGTable
		}

		req := &elastic.BulkRequest{TargetName: rule.PGName, Index: rule.PGSchema, Type: pgTable, ID: id, Parent: parentID, Pipeline: rule.Pipeline}
		routerFilter(rule, values, req)
		if action == canal.DeleteAction {
			r.makeDeleteReqData(req, rule, values)
			r.st.DeleteNum.Add(1)
		} else {
			//主键数据
			r.makeDeleteReqData(req, rule, values)
			r.makeInsertReqData(req, rule, values)
			r.st.InsertNum.Add(1)
		}
		reqs = append(reqs, req)
	}

	return reqs, nil
}

func (r *River) makeInsertRequest(rule *Rule, rows [][]interface{}) ([]*elastic.BulkRequest, error) {
	return r.makeRequest(rule, canal.InsertAction, rows)
}

func (r *River) makeDeleteRequest(rule *Rule, rows [][]interface{}) ([]*elastic.BulkRequest, error) {
	return r.makeRequest(rule, canal.DeleteAction, rows)
}

func (r *River) makeUpdateRequest(rule *Rule, rows [][]interface{}) ([]*elastic.BulkRequest, error) {
	if len(rows)%2 != 0 {
		return nil, errors.Errorf("invalid update rows event, must have 2x rows, but %d", len(rows))
	}

	reqs := make([]*elastic.BulkRequest, 0, len(rows))

	for i := 0; i < len(rows); i += 2 {
		beforeID, err := r.getDocID(rule, rows[i])
		if err != nil {
			return nil, errors.Trace(err)
		}

		afterID, err := r.getDocID(rule, rows[i+1])

		if err != nil {
			return nil, errors.Trace(err)
		}

		beforeParentID, afterParentID := "", ""
		if len(rule.Parent) > 0 {
			if beforeParentID, err = r.getParentID(rule, rows[i], rule.Parent); err != nil {
				return nil, errors.Trace(err)
			}
			if afterParentID, err = r.getParentID(rule, rows[i+1], rule.Parent); err != nil {
				return nil, errors.Trace(err)
			}
		}

		pgTable := rule.Table
		if len(rule.PGTable) > 0 {
			pgTable = rule.PGTable
		}

		//更新前后数据匹配数据路由器
		beforeDataRouter := matchRouterFilter(rule, rows[i])
		afterDataRouter := matchRouterFilter(rule, rows[i+1])
		/*
		 * - 前后数据匹配到的路由器相同（未匹配到的为nil）&& 主键未变更，则update
		 * - 前后数据匹配到的路由器不相同（未匹配到的为nil）|| 主键变更，则delete+insert
		 */
		if beforeDataRouter == afterDataRouter && beforeID == afterID {
			req := &elastic.BulkRequest{TargetName: rule.PGName, Index: rule.PGSchema, Type: pgTable, ID: beforeID, Parent: beforeParentID}
			r.makeUpdateReqData(req, rule, rows[i], rows[i+1])
			changeDataSource(beforeDataRouter, req)
			r.st.UpdateNum.Add(1)
			reqs = append(reqs, req)
		} else {
			/**
			 * 更新操作拆分为删除+新增
			 */
			//build delete req
			req := &elastic.BulkRequest{TargetName: rule.PGName, Index: rule.PGSchema, Type: pgTable, ID: beforeID, Parent: beforeParentID}
			changeDataSource(beforeDataRouter, req)
			r.makeDeleteReqData(req, rule, rows[i])
			r.st.DeleteNum.Add(1)
			reqs = append(reqs, req)

			//build insert req
			req = &elastic.BulkRequest{TargetName: rule.PGName, Index: rule.PGSchema, Type: pgTable, ID: afterID, Parent: afterParentID, Pipeline: rule.Pipeline}
			changeDataSource(afterDataRouter, req)
			//主键数据
			r.makeDeleteReqData(req, rule, rows[i+1])
			r.makeInsertReqData(req, rule, rows[i+1])
			r.st.InsertNum.Add(1)
			reqs = append(reqs, req)

		}

		/*

			if beforeID != afterID || beforeParentID != afterParentID {
				req.Action = elastic.ActionDelete
				reqs = append(reqs, req)

				req = &elastic.BulkRequest{TargetName: rule.PGName, Index: rule.PGSchema, Type: pgTable, ID: afterID, Parent: afterParentID, Pipeline: rule.Pipeline}
				r.makeInsertReqData(req, rule, rows[i+1])

				r.st.DeleteNum.Add(1)
				r.st.InsertNum.Add(1)
			} else {
				if len(rule.Pipeline) > 0 {
					// Pipelines can only be specified on index action
					r.makeInsertReqData(req, rule, rows[i+1])
					// Make sure action is index, not create
					req.Action = elastic.ActionIndex
					req.Pipeline = rule.Pipeline
				} else {
					r.makeUpdateReqData(req, rule, rows[i], rows[i+1])
				}
				r.st.UpdateNum.Add(1)
			}
		*/

	}

	return reqs, nil
}

//数据路由
func routerFilter(rule *Rule, values []interface{}, req *elastic.BulkRequest) {
	//匹配路由
	hitDataRouter := matchRouterFilter(rule, values)
	//更换匹配到的数据源
	changeDataSource(hitDataRouter, req)
}

func changeDataSource(hitDataRouter *DataRouter, req *elastic.BulkRequest) {
	if hitDataRouter != nil {
		//数据源
		req.TargetName = hitDataRouter.Target.DataSource
		//库名
		req.Index = hitDataRouter.Target.SchemaName
		//表名
		req.Type = hitDataRouter.Target.TableName
	}
}

//匹配数据路由
func matchRouterFilter(rule *Rule, values []interface{}) *DataRouter {
	//未配置路由
	if len(rule.DataRouters) == 0 {
		return nil
	}

	for _, dataRouter := range rule.DataRouters {
		isHit := true
		for index, expValue := range dataRouter.FieldValueMap {
			if fmt.Sprint(values[index]) != expValue {
				isHit = false
			}
		}
		//命中路由
		if isHit {
			return dataRouter
		}
	}

	//无匹配路由
	return nil

}

//update操作是否改变的数据路由依据的字段的值,如果修改了则需要将update操作拆分成delete+insert
func isUpdateRouterData(dataRouter *DataRouter, beforeValues []interface{}, afterValues []interface{}) bool {
	for index := range dataRouter.FieldValueMap {
		if fmt.Sprint(beforeValues[index]) != fmt.Sprint(afterValues[index]) {
			return true
		}
	}
	return false

}

func (r *River) makeReqColumnData(col *schema.TableColumn, value interface{}) interface{} {
	switch col.Type {
	case schema.TYPE_ENUM:
		switch value := value.(type) {
		case int64:
			// for binlog, ENUM may be int64, but for dump, enum is string
			eNum := value - 1
			if eNum < 0 || eNum >= int64(len(col.EnumValues)) {
				// we insert invalid enum value before, so return empty
				log.Warnf("invalid binlog enum index %d, for enum %v", eNum, col.EnumValues)
				return ""
			}

			return col.EnumValues[eNum]
		}
	case schema.TYPE_SET:
		switch value := value.(type) {
		case int64:
			// for binlog, SET may be int64, but for dump, SET is string
			bitmask := value
			sets := make([]string, 0, len(col.SetValues))
			for i, s := range col.SetValues {
				if bitmask&int64(1<<uint(i)) > 0 {
					sets = append(sets, s)
				}
			}
			return strings.Join(sets, ",")
		}
	case schema.TYPE_BIT:
		switch value := value.(type) {
		case string:
			// for binlog, BIT is int64, but for dump, BIT is string
			// for dump 0x01 is for 1, \0 is for 0
			if value == "\x01" {
				return int64(1)
			}

			return int64(0)
		}
	case schema.TYPE_STRING:
		switch value := value.(type) {
		case []byte:
			return string(value[:])
		}
	case schema.TYPE_JSON:
		var f interface{}
		var err error
		switch v := value.(type) {
		case string:
			err = json.Unmarshal([]byte(v), &f)
		case []byte:
			err = json.Unmarshal(v, &f)
		}
		if err == nil && f != nil {
			return f
		}
	case schema.TYPE_DATETIME, schema.TYPE_TIMESTAMP:
		switch v := value.(type) {
		case string:
			vt, err := time.ParseInLocation(mysql.TimeFormat, string(v), time.Local)
			if err != nil || vt.IsZero() { // failed to parse date or zero date
				return nil
			}
			return vt.Format(time.RFC3339)
		}
	case schema.TYPE_DATE:
		switch v := value.(type) {
		case string:
			vt, err := time.Parse(mysqlDateFormat, string(v))
			if err != nil || vt.IsZero() { // failed to parse date or zero date
				return nil
			}
			return vt.Format(mysqlDateFormat)
		}
	}

	return value
}

func (r *River) getFieldParts(k string, v string) (string, string, string) {
	composedField := strings.Split(v, ",")

	mysql := k
	elastic := composedField[0]
	fieldType := ""

	if 0 == len(elastic) {
		elastic = mysql
	}
	if 2 == len(composedField) {
		fieldType = composedField[1]
	}

	return mysql, elastic, fieldType
}

func (r *River) makeInsertReqData(req *elastic.BulkRequest, rule *Rule, values []interface{}) {
	req.Data = make(map[string]interface{}, len(values))
	req.Action = elastic.ActionIndex

	for i, c := range rule.TableInfo.Columns {
		if !rule.CheckFilter(c.Name) {
			continue
		}
		mapped := false
		for k, v := range rule.FieldMapping {
			mysql, elastic, fieldType := r.getFieldParts(k, v)
			if mysql == c.Name {
				mapped = true
				req.Data[elastic] = r.getFieldValue(&c, fieldType, values[i])
			}
		}
		if mapped == false {
			req.Data[c.Name] = r.makeReqColumnData(&c, values[i])
		}
	}
}

func (r *River) makeDeleteReqData(req *elastic.BulkRequest, rule *Rule, values []interface{}) {
	req.PKData = make(map[string]interface{}, len(values))
	req.Action = elastic.ActionDelete

	if rule.ID == nil || len(rule.ID) == 0 {
		//使用主键数据
		for _, index := range rule.TableInfo.PKColumns {
			c := rule.TableInfo.Columns[index]
			mapped := false
			for k, v := range rule.FieldMapping {
				mysql, elastic, fieldType := r.getFieldParts(k, v)
				if mysql == c.Name {
					mapped = true
					req.PKData[elastic] = r.getFieldValue(&c, fieldType, values[index])
				}
			}
			if mapped == false {
				req.PKData[c.Name] = r.makeReqColumnData(&c, values[index])
			}
		}

	} else {
		for _, columnName := range rule.ID {
			index := rule.TableInfo.FindColumn(columnName)
			if index < 0 {
				log.Errorf("rule.ID 配置有误，column[%v] 不存在", columnName)
			}
			c := rule.TableInfo.Columns[index]
			mapped := false
			for k, v := range rule.FieldMapping {
				mysql, elastic, fieldType := r.getFieldParts(k, v)
				if mysql == c.Name {
					mapped = true
					req.PKData[elastic] = r.getFieldValue(&c, fieldType, values[index])
				}
			}
			if mapped == false {
				req.PKData[c.Name] = r.makeReqColumnData(&c, values[index])
			}
		}
	}
}

func (r *River) makeUpdateReqData(req *elastic.BulkRequest, rule *Rule,
	beforeValues []interface{}, afterValues []interface{}) {
	//主键数据
	r.makeDeleteReqData(req, rule, beforeValues)

	req.Data = make(map[string]interface{}, len(beforeValues))

	// maybe dangerous if something wrong delete before?
	req.Action = elastic.ActionUpdate

	for i, c := range rule.TableInfo.Columns {
		mapped := false
		if !rule.CheckFilter(c.Name) {
			continue
		}
		if reflect.DeepEqual(beforeValues[i], afterValues[i]) {
			//nothing changed
			continue
		}
		for k, v := range rule.FieldMapping {
			mysql, elastic, fieldType := r.getFieldParts(k, v)
			if mysql == c.Name {
				mapped = true
				req.Data[elastic] = r.getFieldValue(&c, fieldType, afterValues[i])
			}
		}
		if mapped == false {
			req.Data[c.Name] = r.makeReqColumnData(&c, afterValues[i])
		}

	}
}

// If id in toml file is none, get primary keys in one row and format them into a string, and PK must not be nil
// Else get the ID's column in one row and format them into a string
func (r *River) getDocID(rule *Rule, row []interface{}) (string, error) {
	var (
		ids []interface{}
		err error
	)
	if rule.ID == nil {
		ids, err = rule.TableInfo.GetPKValues(row)
		if err != nil {
			return "", err
		}
	} else {
		ids = make([]interface{}, 0, len(rule.ID))
		for _, column := range rule.ID {
			value, err := rule.TableInfo.GetColumnValue(column, row)
			if err != nil {
				return "", err
			}
			ids = append(ids, value)
		}
	}

	var buf bytes.Buffer

	sep := ""
	for i, value := range ids {
		if value == nil {
			return "", errors.Errorf("The %ds id or PK value is nil", i)
		}

		buf.WriteString(fmt.Sprintf("%s%v", sep, value))
		sep = ":"
	}

	return buf.String(), nil
}

func (r *River) getParentID(rule *Rule, row []interface{}, columnName string) (string, error) {
	index := rule.TableInfo.FindColumn(columnName)
	if index < 0 {
		return "", errors.Errorf("parent id not found %s(%s)", rule.TableInfo.Name, columnName)
	}

	return fmt.Sprint(row[index]), nil
}

func (r *River) asyncDoPGRequest(reqs []*elastic.BulkRequest, wg *sync.WaitGroup) {
	defer wg.Done()
	for _, req := range reqs {
		if err := r.doPGRequest(req); err != nil {
			r.cancel()
			break
		}
	}

}

func (r *River) doPGRequest(req *elastic.BulkRequest) error {
	var pg *elastic.PGClient
	if len(req.TargetName) > 0 {
		pg, _ = r.pgs[req.TargetName]
	} else {
		pg = r.pg
	}

	if pg != nil {
		if err := pg.Request(req); err != nil {
			log.Errorf("sync docs err %v after binlog %s", err, r.canal.SyncedPosition())
			return errors.Trace(err)
		}
		//异步记录监控指标
		go func() {
			metricName := fmt.Sprintf(r.metricPrefix, strings.Replace(pg.Conf.Host, ".", "_", -1), pg.Conf.DBName)
			delaySecond := time.Now().Unix() - int64(req.Timestamp)
			if delaySecond >= 0 {
				_ = r.statsdClient.Timing(metricName+"delay", delaySecond*1000, 1.0)
			}
			_ = r.statsdClient.Inc(metricName+req.Action, 1, 1)
		}()

	} else {
		err := errors.Errorf("can not find targetSource[%s] for [%s.%s]", req.Index, req.Type, req.TargetName)
		log.Errorf("sync data error:%v", err)
		return err
	}
	return nil
}

// get mysql field value and convert it to specific value to es
func (r *River) getFieldValue(col *schema.TableColumn, fieldType string, value interface{}) interface{} {
	var fieldValue interface{}
	switch fieldType {
	case fieldTypeList:
		v := r.makeReqColumnData(col, value)
		if str, ok := v.(string); ok {
			fieldValue = strings.Split(str, ",")
		} else {
			fieldValue = v
		}

	case fieldTypeDate:
		if col.Type == schema.TYPE_NUMBER {
			col.Type = schema.TYPE_DATETIME

			v := reflect.ValueOf(value)
			switch v.Kind() {
			case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
				fieldValue = r.makeReqColumnData(col, time.Unix(v.Int(), 0).Format(mysql.TimeFormat))
			}
		}
	}

	if fieldValue == nil {
		fieldValue = r.makeReqColumnData(col, value)
	}
	return fieldValue
}

func (r *River) getMysqlTable(tableSchema, tableName string) (*elastic.TableInfo, error) {
	result, err := r.canal.Execute(fmt.Sprintf("show full columns from `%s`.`%s`", tableSchema, tableName))
	if err != nil {
		return nil, errors.Trace(err)
	}

	column := make(map[string]elastic.TableColumn)

	for i := 0; i < result.RowNumber(); i++ {
		name, _ := result.GetString(i, 0)
		colType, _ := result.GetString(i, 1)
		collation, _ := result.GetString(i, 2)
		isNull, _ := result.GetString(i, 3)
		defaultValue, _ := result.GetValue(i, 5)
		//extra, _ := result.GetString(i, 6)

		var defaultV interface{}
		switch v := defaultValue.(type) {
		case string:
			defaultV = v
		case []byte:
			defaultV = hack.String(v)
		case int, int8, int16, int32, int64,
			uint, uint8, uint16, uint32, uint64:
			defaultV = fmt.Sprintf("%d", v)
		case float32:
			defaultV = strconv.FormatFloat(float64(v), 'f', -1, 64)
		case float64:
			defaultV = strconv.FormatFloat(v, 'f', -1, 64)
		default:
			defaultV = nil
		}

		column[name] = elastic.TableColumn{Name: name, DataType: colType, Collation: collation, DefaultValue: defaultV, IsNullAble: isNull == "YES"}
	}
	return &elastic.TableInfo{Schema: tableSchema, Name: tableName, Columns: column}, nil
}
