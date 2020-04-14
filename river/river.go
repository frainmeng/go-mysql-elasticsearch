package river

import (
	"context"
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/juju/errors"
	"github.com/siddontang/go-log/log"
	"github.com/siddontang/go-mysql-elasticsearch/elastic"
	"github.com/siddontang/go-mysql/canal"

	"github.com/cactus/go-statsd-client/statsd"
)

// ErrRuleNotExist is the error if rule is not defined.
var ErrRuleNotExist = errors.New("rule is not exist")

// River is a pluggable service within Elasticsearch pulling data then indexing it into Elasticsearch.
// We use this definition here too, although it may not run within Elasticsearch.
// Maybe later I can implement a acutal river in Elasticsearch, but I must learn java. :-)
type River struct {
	c *Config

	canal *canal.Canal

	rules map[string]*Rule

	ctx    context.Context
	cancel context.CancelFunc

	wg sync.WaitGroup

	es *elastic.Client

	pg *elastic.PGClient

	pgs map[string]*elastic.PGClient

	statsdClient statsd.Statter

	st *stat

	master *masterInfo

	syncCh chan interface{}

	metricPrefix string

	posChan chan posSaver

	ackChan chan *ack

	dataChans []chan *elastic.BulkRequest
}

// NewRiver creates the River from config
func NewRiver(c *Config) (*River, error) {
	r := new(River)

	tmp := fmt.Sprintf("mysqltopg.%s",
		strings.Replace(strings.Replace(c.MyAddr, ".", "_", -1), ":", "-", -1))
	//mysqltopg.${mysql}.${pgHost}-${pgName}.${action/delay}
	r.metricPrefix = tmp + ".%s-%s."
	r.c = c
	r.rules = make(map[string]*Rule)
	r.syncCh = make(chan interface{}, 1024)
	r.ctx, r.cancel = context.WithCancel(context.Background())

	var err error
	if r.master, err = loadMasterInfo(c.DataDir); err != nil {
		return nil, errors.Trace(err)
	}

	if err = r.newCanal(); err != nil {
		return nil, errors.Trace(err)
	}

	if err = r.prepareRule(); err != nil {
		return nil, errors.Trace(err)
	}

	if err = r.prepareCanal(); err != nil {
		return nil, errors.Trace(err)
	}

	// We must use binlog full row image
	if err = r.canal.CheckBinlogRowImage("FULL"); err != nil {
		return nil, errors.Trace(err)
	}

	if c.ConcurrentAckWin <= 0 {
		c.ConcurrentAckWin = 1
	}
	if c.ConcurrentSize <= 0 {
		c.ConcurrentSize = 1
	}

	r.posChan = make(chan posSaver, c.ConcurrentAckWin)
	//ack 队列
	r.ackChan = make(chan *ack, c.ConcurrentAckWin)
	//数据chan，每个线程一个
	r.dataChans = make([]chan *elastic.BulkRequest, c.ConcurrentSize)

	cfg := new(elastic.ClientConfig)
	cfg.Addr = r.c.ESAddr
	cfg.User = r.c.ESUser
	cfg.Password = r.c.ESPassword
	cfg.HTTPS = r.c.ESHttps
	r.es = elastic.NewClient(cfg)

	if r.c.Targets != nil && len(r.c.Targets) > 0 {
		r.pgs = make(map[string]*elastic.PGClient)
		for _, target := range r.c.Targets {
			pgCfg := new(elastic.PGClientConfig)
			pgCfg.Host = target.PGHost
			pgCfg.Port = target.PGPort
			pgCfg.User = target.PGUser
			pgCfg.Password = target.PGPassword
			pgCfg.DBName = target.PGDBName
			pgCfg.MaxConn = r.c.ConcurrentSize
			r.pgs[target.PGName] = elastic.NewPGClient(pgCfg)
		}
	}

	pgcfg := new(elastic.PGClientConfig)
	pgcfg.Host = r.c.PGHost
	pgcfg.Port = r.c.PGPort
	pgcfg.User = r.c.PGUser
	pgcfg.Password = r.c.PGPassword
	pgcfg.DBName = r.c.PGDBName
	pgcfg.MaxConn = r.c.ConcurrentSize
	r.pg = elastic.NewPGClient(pgcfg)

	if r.c.StatsdHost != "" && r.c.StatsdPort != 0 {
		statsdAddr := fmt.Sprintf("%s:%v", r.c.StatsdHost, r.c.StatsdPort)
		statsdClient, err := statsd.NewClient(statsdAddr, r.c.StatsdPreFix)
		if err == nil {
			r.statsdClient = statsdClient
			log.Infof("开启statsd监控，地址：%s，前缀：%s", statsdAddr, r.c.StatsdPreFix)
		} else {
			log.Warnf("开启statsd监控失败：%v", err)
		}
	}

	r.st = &stat{r: r}
	go r.st.Run(r.c.StatAddr)

	return r, nil
}

func (r *River) newCanal() error {
	cfg := canal.NewDefaultConfig()
	cfg.Addr = r.c.MyAddr
	cfg.User = r.c.MyUser
	cfg.Password = r.c.MyPassword
	cfg.Charset = r.c.MyCharset
	cfg.Flavor = r.c.Flavor

	cfg.ServerID = r.c.ServerID
	cfg.Dump.ExecutionPath = r.c.DumpExec
	cfg.Dump.DiscardErr = false
	cfg.Dump.SkipMasterData = r.c.SkipMasterData

	for _, s := range r.c.Sources {
		for _, t := range s.Tables {
			cfg.IncludeTableRegex = append(cfg.IncludeTableRegex, s.Schema+"\\."+t)
		}
	}

	var err error
	r.canal, err = canal.NewCanal(cfg)
	return errors.Trace(err)
}

func (r *River) prepareCanal() error {
	var db string
	dbs := map[string]struct{}{}
	tables := make([]string, 0, len(r.rules))
	for _, rule := range r.rules {
		db = rule.Schema
		dbs[rule.Schema] = struct{}{}
		tables = append(tables, rule.Table)
	}

	if len(dbs) == 1 {
		// one db, we can shrink using table
		r.canal.AddDumpTables(db, tables...)
	} else {
		// many dbs, can only assign databases to dump
		keys := make([]string, 0, len(dbs))
		for key := range dbs {
			keys = append(keys, key)
		}

		r.canal.AddDumpDatabases(keys...)
	}

	r.canal.SetEventHandler(&eventHandler{r})

	return nil
}

func (r *River) newRule(schema, table string) error {
	key := ruleKey(schema, table)

	if _, ok := r.rules[key]; ok {
		return errors.Errorf("duplicate source %s, %s defined in config", schema, table)
	}

	r.rules[key] = newDefaultRule(schema, table)
	return nil
}

func (r *River) updateRule(schema, table string) error {
	rule, ok := r.rules[ruleKey(schema, table)]
	if !ok {
		return ErrRuleNotExist
	}

	tableInfo, err := r.canal.GetTable(schema, table)
	if err != nil {
		return errors.Trace(err)
	}

	rule.TableInfo = tableInfo

	return nil
}

func (r *River) parseSource() (map[string][]string, error) {
	wildTables := make(map[string][]string, len(r.c.Sources))

	// first, check sources
	for _, s := range r.c.Sources {
		if !isValidTables(s.Tables) {
			return nil, errors.Errorf("wildcard * is not allowed for multiple tables")
		}

		for _, table := range s.Tables {
			if len(s.Schema) == 0 {
				return nil, errors.Errorf("empty schema not allowed for source")
			}

			if regexp.QuoteMeta(table) != table {
				if _, ok := wildTables[ruleKey(s.Schema, table)]; ok {
					return nil, errors.Errorf("duplicate wildcard table defined for %s.%s", s.Schema, table)
				}

				tables := []string{}

				sql := fmt.Sprintf(`SELECT table_name FROM information_schema.tables WHERE
					table_name RLIKE "%s" AND table_schema = "%s";`, buildTable(table), s.Schema)

				res, err := r.canal.Execute(sql)
				if err != nil {
					return nil, errors.Trace(err)
				}

				for i := 0; i < res.Resultset.RowNumber(); i++ {
					f, _ := res.GetString(i, 0)
					err := r.newRule(s.Schema, f)
					if err != nil {
						return nil, errors.Trace(err)
					}

					tables = append(tables, f)
				}

				wildTables[ruleKey(s.Schema, table)] = tables
			} else {
				err := r.newRule(s.Schema, table)
				if err != nil {
					return nil, errors.Trace(err)
				}
			}
		}
	}

	if len(r.rules) == 0 {
		return nil, errors.Errorf("no source data defined")
	}

	return wildTables, nil
}

func (r *River) prepareRule() error {
	wildtables, err := r.parseSource()
	if err != nil {
		return errors.Trace(err)
	}

	if r.c.Rules != nil {
		// then, set custom mapping rule
		for _, rule := range r.c.Rules {
			if len(rule.Schema) == 0 {
				return errors.Errorf("empty schema not allowed for rule")
			}

			if regexp.QuoteMeta(rule.Table) != rule.Table {
				//wildcard table
				tables, ok := wildtables[ruleKey(rule.Schema, rule.Table)]
				if !ok {
					return errors.Errorf("wildcard table for %s.%s is not defined in source", rule.Schema, rule.Table)
				}

				if len(rule.PGSchema) == 0 {
					return errors.Errorf("wildcard table rule %s.%s must have a pg_schema, can not empty", rule.Schema, rule.Table)
				}

				rule.prepare()

				for _, table := range tables {
					rr := r.rules[ruleKey(rule.Schema, table)]
					rr.Index = rule.Index
					rr.Type = rule.Type
					rr.Parent = rule.Parent
					rr.ID = rule.ID
					rr.FieldMapping = rule.FieldMapping

					rr.PGName = rule.PGName
					rr.PGSchema = rule.PGSchema
					rr.PGTable = table
					rr.SkipActions = rule.SkipActions
					rr.SkipAlterActions = rule.SkipAlterActions

				}
			} else {
				key := ruleKey(rule.Schema, rule.Table)
				if _, ok := r.rules[key]; !ok {
					return errors.Errorf("rule %s, %s not defined in source", rule.Schema, rule.Table)
				}
				rule.prepare()
				r.rules[key] = rule
			}
		}
	}

	rules := make(map[string]*Rule)
	for key, rule := range r.rules {
		if rule.TableInfo, err = r.canal.GetTable(rule.Schema, rule.Table); err != nil {
			return errors.Trace(err)
		}

		if len(rule.TableInfo.PKColumns) == 0 {
			if !r.c.SkipNoPkTable {
				return errors.Errorf("%s.%s must have a PK for a column", rule.Schema, rule.Table)
			}

			log.Errorf("ignored table without a primary key: %s\n", rule.TableInfo.Name)
		} else {
			rules[key] = rule
		}
		rule.prepareDataRouter()
	}
	r.rules = rules

	return nil
}

func ruleKey(schema string, table string) string {
	return strings.ToLower(fmt.Sprintf("%s:%s", schema, table))
}

// Run syncs the data from MySQL and inserts to ES.
func (r *River) Run() error {

	dataChanLen := r.c.ConcurrentAckWin / r.c.ConcurrentSize
	if dataChanLen <= 0 {
		dataChanLen = 1
	}
	//启动数据处理线程
	for i := 0; i < len(r.dataChans); i++ {
		r.dataChans[i] = make(chan *elastic.BulkRequest, dataChanLen)
		go r.syncData(r.dataChans[i], r.ackChan, strconv.Itoa(i))
	}
	//position 处理线程
	r.wg.Add(1)
	go r.posProcessor(r.posChan, r.ackChan)

	r.wg.Add(1)
	go r.syncLoop()

	pos := r.master.Position()
	if err := r.canal.RunFrom(pos); err != nil {
		log.Errorf("start canal err %v", err)
		r.cancel()
		return errors.Trace(err)
	}

	return nil
}

// Ctx returns the internal context for outside use.
func (r *River) Ctx() context.Context {
	return r.ctx
}

// Close closes the River
func (r *River) Close() {
	log.Infof("closing river")

	r.cancel()

	r.canal.Close()

	r.master.Close()

	done := make(chan interface{})
	go func() {
		r.wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		log.Info("process exited")
	case <-time.After(3000 * time.Millisecond):
		log.Info("process exited after 3 seconds")
		//os.Exit(0)
		//done <- struct{}{}
	}
}

func isValidTables(tables []string) bool {
	if len(tables) > 1 {
		for _, table := range tables {
			if table == "*" {
				return false
			}
		}
	}
	return true
}

func buildTable(table string) string {
	if table == "*" {
		return "." + table
	}
	return table
}
