package sink

import (
	"context"
	"fmt"
	"time"

	elastic "gopkg.in/olivere/elastic.v5"
)

// Take JSON and load into Elasticsearch
type ConnectSinker interface {
	ConnectSink() string
}

// Connection details for Elasticsearch
type Sink struct {
	URI string
}

// Connect to Elasticsearch
func (s Sink) ConnectSink() *elastic.Client {
	// Create a client
	client, err := elastic.NewClient(elastic.SetURL("http://127.0.0.1:9200"), elastic.SetSniff(false))
	if err != nil {
		// Handle error
		panic(err)
	}

	return client
}

// CreateIndexIfNotExist Indexes are not created automatically, so we must create.
func CreateIndexIfNotExist(indexName string, es *elastic.Client) error {
	//
	ctx := context.Background()
	exists, err := es.IndexExists(indexName).Do(ctx)
	if err != nil {
		return err
	}

	if !exists {
		// Create a new index.
		createIndex, err := es.CreateIndex(indexName).Do(ctx)
		if err != nil {
			// Handle error
			panic(err)
		}
		if !createIndex.Acknowledged {
			// Not acknowledged
		}
	}

	// Create Index

	return nil
}

// LoadData Load Data into ElasticSearch
func LoadData(msg []byte, id string, es *elastic.Client) {
	// FIXME - Shouldn't Panic, Shouldnt' have hardcoded index.
	index := indexName("")

	err := CreateIndexIfNotExist(index, es)
	if err != nil {
		panic(err)
	}

	put1, err := es.Index().
		Index(index).
		Type("logs").
		Id(id).
		BodyJson(msg).
		Do(context.Background())

	if err != nil {
		// Handle error
		panic(err)
	}
	fmt.Printf("Indexed Event %s to index %s, type %s\n", put1.Id, put1.Index, put1.Type)

}

// indexName Convienance function to get correct index for ElasticSearch
func indexName(tenantId string) string {
	// Default Index is audit-default-%{+YYYY.MM.dd}
	ymd := time.Now().Format("2006.01.02")
	//fmt.Printf("Time: %s", ymd)
	index := "audit-default-" + ymd

	if tenantId != "" {
		//index = fmt.Sprintf("audit-%s-*", tenantId)
		index = "audit-" + tenantId + "-" + ymd
	}
	//fmt.Printf("Index: %s", index)
	return index
}
