package storage

import (
	"fmt"

	"github.com/sapcc/hermes/pkg/util"
	"github.com/spf13/viper"
	"gopkg.in/olivere/elastic.v5"
)

type ElasticSearch struct {
	esClient *elastic.Client
}

func (es *ElasticSearch) client() *elastic.Client {
	// Lazy initialisation - don't connect to ElasticSearch until we need to
	if es.esClient == nil {
		es.init()
	}
	return es.esClient
}

func (es *ElasticSearch) init() {
	util.LogDebug("Initiliasing ElasticSearch()")

	// Create a client
	var err error
	var url = viper.GetString("elasticsearch.url")
	util.LogDebug("Using ElasticSearch URL: %s", url)
	// Added disabling sniffing for Testing from Golang. This corrects a problem. Likely needs to be removed before prod deploy
	es.esClient, err = elastic.NewClient(elastic.SetURL(url), elastic.SetSniff(false))
	//es.esClient, err = elastic.NewClient(elastic.SetURL(url))
	if err != nil {
		panic(err)
	}
}

func indexName(tenantId string) string {
	index := "audit-*"
	if tenantId != "" {
		index = fmt.Sprintf("audit-%s-*", tenantId)
	}
	return index
}
