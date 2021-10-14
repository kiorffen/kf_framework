package controllers

import (
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"text/template"
	"time"

	"beego_framework/common"

	bc "beego_framework/common/bcache"
	ec "beego_framework/common/elastic"
	mc "beego_framework/common/mysql"
	rc "beego_framework/common/redis"

	log "beego_framework/common/logger"

	"github.com/astaxie/beego"
	"github.com/astaxie/beego/config"
	"go.uber.org/zap"
)

const (
	MODULE_CODE              = 283
	MODULE_SCODE_NORMAL      = 1000
	MODULE_SCODE_SLOW        = -1001
	MODULE_SCODE_SERVER_BUSY = -1002

	MAX_LIMIT_RATE = 2000

	SLOW_THRESHOLD = 500

	TIME_EXPIRE_INTERVAL = 86400

	DEFAULT_CONF_PATH = "./conf/app.conf"
)

type AbstractController struct {
	beego.Controller

	stime time.Time
	etime time.Time
	ctx   []string
}

var (
	G_conf config.Configer

	G_rc    map[string]*rc.Redis
	G_mc    map[string]*mc.Mysql
	G_ec    map[string]*ec.ElasticClient
	G_cache map[string]*bc.Bcache

	G_logger *log.Logger

	G_rateLimit common.Limiter
)

func init() {
	var err error
	err = beego.LoadAppConfig("ini", DEFAULT_CONF_PATH)
	if err != nil {
		panic(err)
	}
	G_conf = beego.AppConfig

	initLogger()

	// init redis
	G_rc = make(map[string]*rc.Redis)
	G_rc["wmp"], err = createRedisClient("redis_wmp")
	if err != nil {
		panic(err)
	}

	// init mysql
	G_mc = make(map[string]*mc.Mysql)
	G_mc["gicp3"], err = createMysqlClient("mysql_gicp3")
	if err != nil {
		panic(err)
	}

	// init elastic
	G_ec = make(map[string]*ec.ElasticClient)
	G_ec["yxs"], err = createEsClient("elastic_yxs")
	if err != nil {
		panic(err)
	}

	// init bcache
	G_cache = make(map[string]*bc.Bcache)
	initBcache()

	G_rateLimit = common.NewTokenBucketLimiter(MAX_LIMIT_RATE)
}

func initLogger() {
	maxDays, _ := G_conf.Int("log::maxDays")
	level, _ := G_conf.Int("log::level")
	conf := log.LoggerConf{
		FilePath:    G_conf.String("log::filePath"),
		IsLocalTime: true,
		MaxSize:     1024, // 1G
		MaxBackups:  10,   // 10*1G
		MaxDays:     maxDays,
		IsCompress:  true,
		Level:       level,
		ServerName:  G_conf.String("ServerName"),
	}

	G_logger, _ = log.NewLogger(conf)
}

func createRedisClient(name string) (*rc.Redis, error) {
	redisTimeout, _ := G_conf.Int(fmt.Sprintf("%s::timeout", name))
	redisMaxIdle, _ := G_conf.Int(fmt.Sprintf("%s::maxIdle", name))
	redisMaxActive, _ := G_conf.Int(fmt.Sprintf("%s::maxActive", name))
	redisConf := rc.RedisConf{
		Address:   G_conf.String(fmt.Sprintf("%s::addr", name)),
		Timeout:   time.Duration(redisTimeout) * time.Second,
		Password:  G_conf.String(fmt.Sprintf("%s::password", name)),
		MaxIdle:   redisMaxIdle,
		MaxActive: redisMaxActive,
	}
	r := rc.New(redisConf)
	if r == nil {
		return nil, errors.New("create redis failed. name: " + name)
	}

	return r, nil
}

func createMysqlClient(name string) (*mc.Mysql, error) {
	mysqlTimeout, _ := G_conf.Int(fmt.Sprintf("%s::timeout", name))
	mysqlMaxIdle, _ := G_conf.Int(fmt.Sprintf("%s::maxIdle", name))
	mysqlMaxOpen, _ := G_conf.Int(fmt.Sprintf("%s::maxOpen", name))
	mysqlConf := mc.MysqlConf{
		Address:      G_conf.String(fmt.Sprintf("%s::addr", name)),
		Timeout:      time.Duration(mysqlTimeout) * time.Second,
		MaxIdleConns: mysqlMaxIdle,
		MaxOpenConns: mysqlMaxOpen,
	}
	m := mc.New(mysqlConf)
	if m == nil {
		return nil, errors.New("create mysql failed. name: " + name)
	}

	return m, nil
}

func createEsClient(name string) (*ec.ElasticClient, error) {
	esMaxRetry, _ := G_conf.Int(fmt.Sprintf("%s::maxRetry", name))
	esConf := ec.ElasticConf{
		Address:  G_conf.String(fmt.Sprintf("%s::addr", name)),
		MaxRetry: esMaxRetry,
		User:     G_conf.String(fmt.Sprintf("%s::user", name)),
		Password: G_conf.String(fmt.Sprintf("%s::password", name)),
	}

	e, err := ec.New(esConf)
	if err != nil {
		return nil, err
	}

	return e, nil
}

func initBcache() {
	G_cache["content_base_info"] = bc.NewBcache("content_base_info", 1024*1024).Ttl(time.Second * 300)
}

func (c *AbstractController) Prepare() {
	c.stime = time.Now()

	ok := G_rateLimit.Acquire()
	if !ok {
		G_logger.Logger().Warn("server is busy",
			zap.Int("code", MODULE_CODE),
			zap.Int("scode", MODULE_SCODE_SERVER_BUSY))
		c.outMsg(-1, "server is busy", "")
	}
}

func (c *AbstractController) Finish() {
	c.etime = time.Now()
	difftime := (c.etime.UnixNano() - c.stime.UnixNano()) / 1e6
	scode := MODULE_SCODE_NORMAL
	if difftime > SLOW_THRESHOLD {
		scode = MODULE_SCODE_SLOW
	}
	G_logger.Logger().Info(fmt.Sprintf("url=%s\nrefer=%s\nreqstart=%s\nresend=%s\ncost=%d\n==ctx==%s\n",
		c.Ctx.Input.URI(),
		c.Ctx.Input.Referer(),
		c.stime.Format("2006-01-02 15:04:05.000"),
		c.etime.Format("2006-01-02 15:04:05.000"),
		difftime,
		strings.Join(c.ctx, "\n")),
		zap.Int("code", MODULE_CODE),
		zap.Int("scode", scode))
}

func (c *AbstractController) AppendCtx(str string) {
	c.ctx = append(c.ctx, str)
}

func (c *AbstractController) OutPut(iRetcode int, sErrorMsg string) {
	res := make(map[string]interface{})
	res["iRet"] = iRetcode
	res["sMsg"] = sErrorMsg
	c.Data["json"] = res
	c.ServeJSON()
}

func (c *AbstractController) Escape(key string) string {
	return template.HTMLEscapeString(template.JSEscapeString(key))
}

func (c *AbstractController) GetCookieByKey(key string) string {
	cookie, err := c.Ctx.Request.Cookie(key)
	if err != nil {
		return ""
	}
	return cookie.Value
}

func (c *AbstractController) outMsg(status int, msg interface{}, data interface{}) {
	c.outMsgJSON(status, msg, data)
}

func (c *AbstractController) outMsgJSON(status int, msg interface{}, data interface{}) {
	defer c.StopRun()
	defer c.Finish()

	c.AppendCtx(fmt.Sprintf("resp.status=%v", status))
	if _, ok := msg.(string); ok {
		c.AppendCtx(fmt.Sprintf("resp.msg=%v", msg))
	}

	res := make(map[string]interface{})
	res["status"] = status
	res["msg"] = msg
	res["data"] = data
	res["from"] = "go"
	_, err := json.Marshal(res)
	if err != nil {
		return
	}
	c.Data["json"] = res
	c.ServeJSON()

	return
}
