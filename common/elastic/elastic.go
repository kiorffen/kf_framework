// example
//
// package main
//
// import (
// 	"context"
// 	"fmt"
//
// 	ec "elastic"
//
// 	elastic "gopkg.in/olivere/elastic.v5"
// )
//
// func main() {
// 	conf := ec.ElasticConf{
// 		Address:  "http://es.sh.sh-global2.db:9200",
// 		MaxRetry: 10,
// 		User:     "yxs",
// 		Password: "o@LEerI^*i1S",
// 	}
//
// 	esClient, err := ec.New(conf)
// 	if err != nil {
// 		fmt.Println("create es client failed")
// 		return
// 	}
//
// 	q := elastic.NewBoolQuery()
// 	q = q.Must(elastic.NewTermQuery("com_biz", "160"))
// 	q = q.Must(elastic.NewTermQuery("com_type", "1"))
// 	rslt, errEs := esClient.Client.Search().Index("yxs_gicp_index").Type("contents").Preference("_primary_first").Timeout("1s").Query(q).Do(context.TODO())
// 	if errEs != nil {
// 		fmt.Printf("search es failed. err:%s\n", errEs.Error())
// 		return
// 	}
// 	fmt.Println(rslt.Hits.TotalHits)
// 	var fields []string
// 	fields = append(fields, "com_docid")
// 	fields = append(fields, "com_title")
// 	for _, hit := range rslt.Hits.Hits {
// 		item, err := esClient.ParseDoc(hit.Source, fields)
// 		if err != nil {
// 			continue
// 		}
// 		fmt.Println(item["com_docid"])
// 		fmt.Println(item["com_title"])
// 	}
// }

// description: A Es Client that init, parse field from source
// author: tonytang

package elastic

import (
	"encoding/json"

	elastic "gopkg.in/olivere/elastic.v6"
)

type ElasticConf struct {
	Address  string
	MaxRetry int
	User     string
	Password string
}

type ElasticClient struct {
	Client *elastic.Client

	address  string
	maxRetry int
	user     string
	password string
}

func New(conf ElasticConf) (*ElasticClient, error) {
	e := &ElasticClient{
		address:  conf.Address,
		maxRetry: conf.MaxRetry,
		user:     conf.User,
		password: conf.Password,
	}

	var err error
	e.Client, err = elastic.NewClient(elastic.SetURL(conf.Address),
		elastic.SetMaxRetries(conf.MaxRetry),
		elastic.SetBasicAuth(conf.User, conf.Password))
	if err != nil {
		return nil, err
	}
	return e, nil
}

func (c *ElasticClient) ParseDoc(source *json.RawMessage, fields []string) (map[string]interface{}, error) {
	res := make(map[string]interface{})

	var jdata map[string]interface{}
	err := json.Unmarshal(*source, &jdata)
	if err != nil {
		return res, nil
	}

	for _, field := range fields {
		res[field], _ = jdata[field]
	}

	return res, nil
}
