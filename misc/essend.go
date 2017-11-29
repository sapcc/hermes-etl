package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"io/ioutil"
	"encoding/json"
	"reflect"
	"time"
	elastic "gopkg.in/olivere/elastic.v5"
)

// Elasticsearch is @ 127.0.0.1:9200
// user: elastic pass: changeme
func main() {
	// Create a context
	ctx := context.Background()

	// Create a client
	client, err := elastic.NewClient(elastic.SetURL("http://127.0.0.1:9200"), elastic.SetSniff(false))
	if err != nil {
		// Handle error
		panic(err)
	}

	body, err := ioutil.ReadFile("../cadf-example2.json")
	failOnError(err, "Could not Read File")

	var event Event
	
	if err := json.Unmarshal(body, &event); err != nil {
        panic(err)
    }

	indexname := indexName("ae63ddf2076d4342a56eb049e37a7621")
	fmt.Printf("Index is %s", indexname)

	err = createIndexWithLogsIfDoesNotExist(indexname, client)
    if err != nil {
        panic(err)
    }

    

	// Load data into ES 
	put1, err := client.Index().
		Index(indexname).
		Type("logs").
		Id("2").
		BodyJson(event).
		Do(ctx)

	if err != nil {
		// Handle error
		panic(err)
	}
	fmt.Printf("Indexed Event %s to index %s, type %s\n", put1.Id, put1.Index, put1.Type)

	//err = findAndPrintAppLogs(client)
    //if err != nil {
    //    panic(err)
   // }
}

// Helper function for errors for each amqp call
func failOnError(err error, msg string) {
	if err != nil {
		log.Fatalf("%s: %s", msg, err)
		panic(fmt.Sprintf("%s: %s", msg, err))
	}
}

func createIndexWithLogsIfDoesNotExist(indexname string, client *elastic.Client) error {
	// Create a context
	ctx := context.Background()
 
    exists, err := client.IndexExists(indexname).Do(ctx)
    if err != nil {
        return err
    }

    if exists {
        return nil
    }

    res, err := client.CreateIndex(indexname).
        Body(indexMapping).
        Do(ctx)

    if err != nil {
        return err
    }
    if !res.Acknowledged {
        return errors.New("CreateIndex was not acknowledged. Check that timeout value is correct.")
    }

    return nil
}


func findAndPrintAppLogs(indexname string, client *elastic.Client) error { 
	// Create a context
	ctx := context.Background()
  
	
    termQuery := elastic.NewTermQuery("event_type", appName)

    res, err := client.Search(indexname).
        Index(indexname).
        Query(termQuery).
        Sort("time", true).
        Do(ctx)

    if err != nil {
        return err
    }

    fmt.Println("Logs found:")
    var e Event
    for _, item := range res.Each(reflect.TypeOf(e)) {
        e := item.(Event)
        fmt.Printf("time: %s message: %s\n", e.EventType, e.MessageID)
    }

    return nil
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

const (  
    docType      = "logs"
    appName      = "logs"
    indexMapping = `{
  "mappings": {
    "logs": {
      "_all": {
        "enabled": true,
        "norms": false
      },
      "dynamic_templates": [
        {
          "message_field": {
            "match": "message",
            "match_mapping_type": "string",
            "mapping": {
              "fielddata": {
                "format": "disabled"
              },
              "index": "analyzed",
              "omit_norms": true,
              "type": "string"
            }
          }
        },
        {
          "string_fields": {
            "match": "*",
            "match_mapping_type": "string",
            "mapping": {
              "fielddata": {
                "format": "disabled"
              },
              "fields": {
                "raw": {
                  "ignore_above": 256,
                  "index": "not_analyzed",
                  "type": "string",
                  "doc_values": true
                }
              },
              "index": "analyzed",
              "omit_norms": true,
              "type": "string"
            }
          }
        },
        {
          "double_fields": {
            "match": "*",
            "match_mapping_type": "double",
            "mapping": {
              "doc_values": true,
              "type": "double"
            }
          }
        },
        {
          "long_fields": {
            "match": "*",
            "match_mapping_type": "long",
            "mapping": {
              "doc_values": true,
              "type": "long"
            }
          }
        },
        {
          "date_fields": {
            "match": "*",
            "match_mapping_type": "date",
            "mapping": {
              "doc_values": true,
              "type": "date"
            }
          }
        }
      ],
      "properties": {
        "@timestamp": {
          "type": "date"
        },
        "@version": {
          "type": "keyword"
        },
        "_unique_id": {
          "type": "text",
          "norms": false,
          "fields": {
            "raw": {
              "type": "keyword",
              "ignore_above": 256
            }
          }
        },
        "event_type": {
          "type": "text",
          "norms": false,
          "fields": {
            "raw": {
              "type": "keyword",
              "ignore_above": 256
            }
          }
        },
        "geoip": {
          "dynamic": "true",
          "properties": {
            "ip": {
              "type": "ip"
            },
            "latitude": {
              "type": "float"
            },
            "location": {
              "type": "geo_point"
            },
            "longitude": {
              "type": "float"
            }
          }
        },
        "message_id": {
          "type": "text",
          "norms": false,
          "fields": {
            "raw": {
              "type": "keyword",
              "ignore_above": 256
            }
          }
        },
        "payload": {
          "properties": {
            "action": {
              "type": "text",
              "norms": false,
              "fields": {
                "raw": {
                  "type": "keyword",
                  "ignore_above": 256
                }
              }
            },
            "eventTime": {
              "type": "date"
            },
            "eventType": {
              "type": "text",
              "norms": false,
              "fields": {
                "raw": {
                  "type": "keyword",
                  "ignore_above": 256
                }
              }
            },
            "group": {
              "type": "text",
              "norms": false,
              "fields": {
                "raw": {
                  "type": "keyword",
                  "ignore_above": 256
                }
              }
            },
            "id": {
              "type": "text",
              "norms": false,
              "fields": {
                "raw": {
                  "type": "keyword",
                  "ignore_above": 256
                }
              }
            },
            "inherited_to_projects": {
              "type": "boolean"
            },
            "initiator": {
              "properties": {
                "host": {
                  "properties": {
                    "address": {
                      "type": "text",
                      "norms": false,
                      "fields": {
                        "raw": {
                          "type": "keyword",
                          "ignore_above": 256
                        }
                      }
                    },
                    "agent": {
                      "type": "text",
                      "norms": false,
                      "fields": {
                        "raw": {
                          "type": "keyword",
                          "ignore_above": 256
                        }
                      }
                    }
                  }
                },
                "id": {
                  "type": "text",
                  "norms": false,
                  "fields": {
                    "raw": {
                      "type": "keyword",
                      "ignore_above": 256
                    }
                  }
                },
                "project_id": {
                  "type": "text",
                  "norms": false,
                  "fields": {
                    "raw": {
                      "type": "keyword",
                      "ignore_above": 256
                    }
                  }
                },
                "typeURI": {
                  "type": "text",
                  "norms": false,
                  "fields": {
                    "raw": {
                      "type": "keyword",
                      "ignore_above": 256
                    }
                  }
                },
                "user_id": {
                  "type": "text",
                  "norms": false,
                  "fields": {
                    "raw": {
                      "type": "keyword",
                      "ignore_above": 256
                    }
                  }
                }
              }
            },
            "observer": {
              "properties": {
                "id": {
                  "type": "text",
                  "norms": false,
                  "fields": {
                    "raw": {
                      "type": "keyword",
                      "ignore_above": 256
                    }
                  }
                },
                "typeURI": {
                  "type": "text",
                  "norms": false,
                  "fields": {
                    "raw": {
                      "type": "keyword",
                      "ignore_above": 256
                    }
                  }
                }
              }
            },
            "outcome": {
              "type": "text",
              "norms": false,
              "fields": {
                "raw": {
                  "type": "keyword",
                  "ignore_above": 256
                }
              }
            },
            "project": {
              "type": "text",
              "norms": false,
              "fields": {
                "raw": {
                  "type": "keyword",
                  "ignore_above": 256
                }
              }
            },
            "role": {
              "type": "text",
              "norms": false,
              "fields": {
                "raw": {
                  "type": "keyword",
                  "ignore_above": 256
                }
              }
            },
            "target": {
              "properties": {
                "id": {
                  "type": "text",
                  "norms": false,
                  "fields": {
                    "raw": {
                      "type": "keyword",
                      "ignore_above": 256
                    }
                  }
                },
                "typeURI": {
                  "type": "text",
                  "norms": false,
                  "fields": {
                    "raw": {
                      "type": "keyword",
                      "ignore_above": 256
                    }
                  }
                }
              }
            },
            "typeURI": {
              "type": "text",
              "norms": false,
              "fields": {
                "raw": {
                  "type": "keyword",
                  "ignore_above": 256
                }
              }
            },
            "user": {
              "type": "text",
              "norms": false,
              "fields": {
                "raw": {
                  "type": "keyword",
                  "ignore_above": 256
                }
              }
            }
          }
        },
        "priority": {
          "type": "text",
          "norms": false,
          "fields": {
            "raw": {
              "type": "keyword",
              "ignore_above": 256
            }
          }
        },
        "publisher_id": {
          "type": "text",
          "norms": false,
          "fields": {
            "raw": {
              "type": "keyword",
              "ignore_above": 256
            }
          }
        },
        "tenant_id": {
          "type": "text",
          "norms": false,
          "fields": {
            "raw": {
              "type": "keyword",
              "ignore_above": 256
            }
          }
        },
        "timestamp": {
          "type": "text",
          "norms": false,
          "fields": {
            "raw": {
              "type": "keyword",
              "ignore_above": 256
            }
          }
        }
      }
    }
  }
}`
)

type Event struct {
	_timestamp string `json:"@timestamp"`
	_version   string `json:"@version"`
	_uniqueID  string `json:"_unique_id"`
	EventType  string `json:"event_type"`
	MessageID  string `json:"message_id"`
	Payload    struct {
		Action              string `json:"action"`
		EventTime           string `json:"eventTime"`
		EventType           string `json:"eventType"`
		ID                  string `json:"id"`
		InheritedToProjects bool   `json:"inherited_to_projects"`
		Initiator           struct {
			Host struct {
				Address string `json:"address"`
				Agent   string `json:"agent"`
			} `json:"host"`
			ID        string `json:"id"`
			ProjectID string `json:"project_id"`
			TypeURI   string `json:"typeURI"`
			UserID    string `json:"user_id"`
		} `json:"initiator"`
		Observer struct {
			ID      string `json:"id"`
			TypeURI string `json:"typeURI"`
		} `json:"observer"`
		Outcome string `json:"outcome"`
		Project string `json:"project"`
		Role    string `json:"role"`
		Target  struct {
			ID      string `json:"id"`
			TypeURI string `json:"typeURI"`
		} `json:"target"`
		TypeURI string `json:"typeURI"`
		User    string `json:"user"`
	} `json:"payload"`
	Priority    string `json:"priority"`
	PublisherID string `json:"publisher_id"`
	TenantID    string `json:"tenant_id"`
	Timestamp   string `json:"timestamp"`
}