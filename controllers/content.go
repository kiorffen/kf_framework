package controllers

import (
	"context"
	"crypto/md5"
	"encoding/json"
	"fmt"
	"math"
	"strconv"
	"strings"
	"time"

	"beego_framework/common"

	elastic "gopkg.in/olivere/elastic.v6"
)

type CCReqMustWithoutShould struct {
	Need  map[string]string `json:"need"`
	Range map[string]string `json:"range"`
	Match map[string]string `json:"match"`
}

type CCReqMust struct {
	Need   map[string]string        `json:"need"`
	Range  map[string]string        `json:"range"`
	Match  map[string]string        `json:"match"`
	Should []CCReqMustWithoutShould `json:"should"`
}

type CCBasic struct {
	IBiz      string `json:"ibiz"`
	Source    string `json:"source"`
	Timestamp string `json:"t"`
	Sign      string `json:"sign"`
	Sort      string `json:"sort"`
	Desc      string `json:"desc"`
	Page      string `json:"page"`
	Pagesize  string `json:"pagesize"`
	Nc        string `json:"nc"`
}

type CCReqParam struct {
	Basic   CCBasic   `json:"basic"`
	Must    CCReqMust `json:"must"`
	MustNot CCReqMust `json:"must_not"`
}

type CCParamData struct {
	Req CCReqParam `json:"req"`
	Res []string   `json:"res"`
}

type ContentController struct {
	AbstractController

	Param CCParamData
	IBiz  int

	esIndex string
	esType  string

	cacheKey string
}

func (c *ContentController) Post() {
	var err error
	res := make(map[string]interface{})

	err = json.Unmarshal(c.Ctx.Input.RequestBody, &c.Param)
	if err != nil {
		c.outMsg(-1, "invalid post data. err: "+err.Error(), res)
	}
	c.AppendCtx(fmt.Sprintf("reqparam=[%s]", string(c.Ctx.Input.RequestBody)))

	c.IBiz, err = strconv.Atoi(c.Param.Req.Basic.IBiz)
	if err != nil || c.Param.Req.Basic.IBiz == "" {
		c.outMsg(-1, "invalid ibiz", res)
	}
	if c.Param.Req.Basic.Source == "" {
		c.outMsg(-1, "invalid source", res)
	}
	if common.CheckSign(c.Param.Req.Basic.Sign,
		c.Param.Req.Basic.Source,
		c.Param.Req.Basic.Timestamp,
		c.IBiz) == false {
		c.outMsg(-1, "check sign error", res)
	}

	// search cache
	nc := "yes"
	if c.Param.Req.Basic.Nc == "no" {
		nc = "no"
	}

	if nc == "yes" {
		c.cacheKey = fmt.Sprintf("%X", md5.Sum(c.Ctx.Input.RequestBody))
		cacheData, err := G_cache["content_info"].Get(c.cacheKey)
		if err == nil && cacheData != "" {
			var data map[string]interface{}
			err = json.Unmarshal([]byte(cacheData), &data)
			if err == nil {
				c.outMsg(0, "OK", data)
			}
		}
	}

	q := elastic.NewBoolQuery()
	// must need
	c.parseMustNeed(q, c.Param.Req.Must.Need)
	// must range
	c.parseMustRange(q, c.Param.Req.Must.Range)
	// must match
	c.parseMustMatch(q, c.Param.Req.Must.Match)
	// must should
	c.parseMustShould(q, c.Param.Req.Must.Should)

	// must not need
	c.parseMustNotNeed(q, c.Param.Req.MustNot.Need)
	// must not match
	c.parseMustNotMatch(q, c.Param.Req.MustNot.Match)
	// must not range
	c.parseMustNotRange(q, c.Param.Req.MustNot.Range)
	// must not should
	c.parseMustNotShould(q, c.Param.Req.MustNot.Should)

	c.esIndex = "index"
	c.esType = "type"
	query := G_ec["yxs"].Client.Search().Index(c.esIndex).Type(c.esType).Preference("_primary_first").Timeout("1s").Query(q)
	if c.Param.Req.Basic.Page != "" {
		page, _ := strconv.Atoi(c.Param.Req.Basic.Page)
		pagesize, _ := strconv.Atoi(c.Param.Req.Basic.Pagesize)
		if page == 0 {
			page = 1
		}
		if pagesize == 0 {
			pagesize = 10
		}
		start := (page - 1) * pagesize
		query = query.From(start)
	}
	if c.Param.Req.Basic.Pagesize != "" {
		limit, _ := strconv.Atoi(c.Param.Req.Basic.Pagesize)
		if limit == 0 {
			limit = 10
		}
		query = query.Size(limit)
	}
	if c.Param.Req.Basic.Sort != "" && c.Param.Req.Basic.Desc != "" {
		sorts := strings.Split(c.Param.Req.Basic.Sort, ",")
		descs := strings.Split(c.Param.Req.Basic.Desc, ",")
		if len(sorts) != len(descs) {
			c.outMsg(-1, "invalid sort and desc", res)
		}
		for idx, d := range descs {
			desc := true
			if d == "yes" {
				desc = false
			}
			sort := sorts[idx]
			query = query.Sort(sort, desc)
		}
	}

	resEs, errEs := query.Do(context.TODO())
	if errEs != nil {
		searchlog := ""
		src, err := q.Source()
		if err == nil {
			bdata, err := json.Marshal(src)
			if err == nil {
				searchlog = string(bdata)
			}
		}
		G_logger.Logger().Warn(searchlog)
		c.outMsg(-1, searchlog, res)
	}

	src, _ := q.Source()
	bdata, _ := json.Marshal(src)
	G_logger.Logger().Info(string(bdata))

	total := resEs.Hits.TotalHits

	// parseDoc
	var items []map[string]interface{}
	for _, hit := range resEs.Hits.Hits {
		item, err := G_ec["yxs"].ParseDoc(hit.Source, c.Param.Res)
		if err != nil {
			c.outMsg(-1, "parse doc failed. err: "+err.Error(), res)
			continue
		}
		items = append(items, item)
	}

	// create result
	res["items"] = items
	res["total"] = total
	if c.Param.Req.Basic.Page != "" {
		page, _ := strconv.Atoi(c.Param.Req.Basic.Page)
		pagesize, _ := strconv.Atoi(c.Param.Req.Basic.Pagesize)
		if page == 0 {
			page = 1
		}
		if pagesize == 0 {
			pagesize = 10
		}
		start := (page - 1) * pagesize
		res["page"] = page
		res["totalpage"] = math.Ceil(float64(total) / float64(pagesize))
		res["pagesize"] = math.Max(math.Min(float64(int(total)-start), float64(pagesize)), 0)
	}

	if nc == "yes" {
		bdata, err := json.Marshal(res)
		if err == nil {
			G_cache["content_info"].Set(c.cacheKey, string(bdata))
		}
		c.AppendCtx(fmt.Sprintf("reqdata=[%s]", string(bdata)))
	}

	c.outMsg(0, "OK", res)
}

func (c *ContentController) parseMustNeed(q *elastic.BoolQuery, need map[string]string) {
	for field, value := range need {
		q = q.Must(elastic.NewTermsQuery(field, common.ParseStringToInterface(value)...))
	}
}

func (c *ContentController) parseMustRange(q *elastic.BoolQuery, rg map[string]string) {
	for field, value := range rg {
		parts := strings.Split(value, "|")
		if len(parts) != 3 {
			c.outMsg(-1, "invalid range: "+value, "")
		}
		switch parts[2] {
		case "T":
			stime := ""
			etime := ""
			if parts[0] != "" {
				tt, err := strconv.ParseInt(parts[0], 10, 64)
				if err != nil {
					c.outMsg(-1, "invalid stime "+parts[0], "")
				}
				stime = time.Unix(tt, 0).Format("2006-01-02 15:04:05")
			}
			if parts[1] != "" {
				tt, err := strconv.ParseInt(parts[1], 10, 64)
				if err != nil {
					c.outMsg(-1, "invalid etime "+parts[1], "")
				}
				etime = time.Unix(tt, 0).Format("2006-01-02 15:04:05")
			}
			q = q.Filter(elastic.NewRangeQuery(field).Format("yyyy-MM-dd HH:mm:ss").Gte(stime))
			q = q.Filter(elastic.NewRangeQuery(field).Format("yyyy-MM-dd HH:mm:ss").Lte(etime))
		case "N":
			q = q.Filter(elastic.NewRangeQuery(field).Gte(parts[0]))
			q = q.Filter(elastic.NewRangeQuery(field).Lte(parts[1]))
		case "S":
			q = q.Filter(elastic.NewRangeQuery(field).Gte(parts[0]))
			q = q.Filter(elastic.NewRangeQuery(field).Lte(parts[1]))
		default:
			q = q.Filter(elastic.NewRangeQuery(field).Gte(parts[0]))
			q = q.Filter(elastic.NewRangeQuery(field).Lte(parts[1]))
		}
	}
}

func (c *ContentController) parseMustMatch(q *elastic.BoolQuery, match map[string]string) {
	for field, value := range match {
		q = q.Must(elastic.NewMatchQuery(field, value))
	}
}

func (c *ContentController) parseMustShould(q *elastic.BoolQuery, should []CCReqMustWithoutShould) {
	if len(should) > 0 {
		qs := elastic.NewBoolQuery()
		for _, s := range should {
			qt := elastic.NewBoolQuery()
			// must should need
			c.parseMustNeed(qt, s.Need)
			// must should range
			c.parseMustRange(qt, s.Range)
			// must should range
			c.parseMustMatch(qt, s.Match)

			qs = qs.Should(qt)
		}
		q = q.Must(qs)
	}
}

func (c *ContentController) parseMustNotNeed(q *elastic.BoolQuery, need map[string]string) {
	for field, value := range need {
		q = q.MustNot(elastic.NewTermsQuery(field, common.ParseStringToInterface(value)...))
	}
}

func (c *ContentController) parseMustNotRange(q *elastic.BoolQuery, rg map[string]string) {
	for field, value := range rg {
		parts := strings.Split(value, "|")
		if len(parts) != 3 {
			c.outMsg(-1, "invalid range: "+value, "")
		}
		switch parts[2] {
		case "T":
			stime := ""
			etime := ""
			if parts[0] != "" {
				tt, err := strconv.ParseInt(parts[0], 10, 64)
				if err != nil {
					c.outMsg(-1, "invalid stime "+parts[0], "")
				}
				stime = time.Unix(tt, 0).Format("2006-01-02 15:04:05")
			}
			if parts[1] != "" {
				tt, err := strconv.ParseInt(parts[1], 10, 64)
				if err != nil {
					c.outMsg(-1, "invalid etime "+parts[1], "")
				}
				etime = time.Unix(tt, 0).Format("2006-01-02 15:04:05")
			}
			qt := elastic.NewBoolQuery()
			qt = qt.Filter(elastic.NewRangeQuery(field).Format("yyyy-MM-dd HH:mm:ss").Gte(stime))
			qt = qt.Filter(elastic.NewRangeQuery(field).Format("yyyy-MM-dd HH:mm:ss").Lte(etime))
			q = q.MustNot(qt)
		case "N":
			qt := elastic.NewBoolQuery()
			qt = qt.Filter(elastic.NewRangeQuery(field).Gte(parts[0]))
			qt = qt.Filter(elastic.NewRangeQuery(field).Lte(parts[1]))
			q = q.MustNot(qt)
		case "S":
			qt := elastic.NewBoolQuery()
			qt = qt.Filter(elastic.NewRangeQuery(field).Gte(parts[0]))
			qt = qt.Filter(elastic.NewRangeQuery(field).Lte(parts[1]))
			q = q.MustNot(qt)
		default:
			qt := elastic.NewBoolQuery()
			qt = qt.Filter(elastic.NewRangeQuery(field).Gte(parts[0]))
			qt = qt.Filter(elastic.NewRangeQuery(field).Lte(parts[1]))
			q = q.MustNot(qt)
		}
	}
}

func (c *ContentController) parseMustNotMatch(q *elastic.BoolQuery, match map[string]string) {
	for field, value := range match {
		q = q.MustNot(elastic.NewMatchQuery(field, value))
	}
}

func (c *ContentController) parseMustNotShould(q *elastic.BoolQuery, should []CCReqMustWithoutShould) {
	if len(should) > 0 {
		qs := elastic.NewBoolQuery()
		for _, s := range should {
			qt := elastic.NewBoolQuery()
			// must should need
			c.parseMustNeed(qt, s.Need)
			// must should range
			c.parseMustRange(qt, s.Range)
			// must should range
			c.parseMustMatch(qt, s.Match)

			qs = qs.Should(qt)
		}
		q = q.MustNot(qs)
	}
}
