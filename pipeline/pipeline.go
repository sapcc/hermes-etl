package pipeline

import (
	"encoding/json"
	"fmt"
	"strings"

	"github.com/notque/hermes-etl/sink"

	elastic "gopkg.in/olivere/elastic.v5"
)

// Struct for MSG, Change from Kibana syntax to RabbitMQ
type Event struct {
	ID     string      `json:"_id"`
	Index  string      `json:"_index"`
	Score  interface{} `json:"_score"`
	Source struct {
		AtTimestamp string `json:"@timestamp"`
		Version     string `json:"@version"`
		UniqueID    string `json:"_unique_id"`
		EventType   string `json:"event_type"`
		MessageID   string `json:"message_id"`
		Payload     struct {
			Action    string `json:"action"`
			EventTime string `json:"eventTime"`
			EventType string `json:"eventType"`
			ID        string `json:"id"`
			Initiator struct {
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
			Outcome      string `json:"outcome"`
			ResourceInfo string `json:"resource_info"`
			Target       struct {
				ID      string `json:"id"`
				TypeURI string `json:"typeURI"`
			} `json:"target"`
			TypeURI string `json:"typeURI"`
		} `json:"payload"`
		Priority    string `json:"priority"`
		PublisherID string `json:"publisher_id"`
		TenantID    string `json:"tenant_id"`
		Timestamp   string `json:"timestamp"`
	} `json:"_source"`
	Type   string `json:"_type"`
	Fields struct {
		AtTimestamp      []int `json:"@timestamp"`
		PayloadEventTime []int `json:"payload.eventTime"`
	} `json:"fields"`
	Sort []int `json:"sort"`
}

func Incoming(msg []byte, es *elastic.Client) string {
	var event Event
	err := json.Unmarshal(msg, &event)
	if err != nil {
		fmt.Println("error:", err)
	}
	fmt.Println(event.Source.EventType)

	id := event.ID
	// Should really be a rules engine. but isn't.
	// Hardcoded rules.
	if strings.HasPrefix(event.Source.EventType, "identity.authenticate") {
		// FIXME - Acknowledge to RabbitMQ this message was handled.
		// Drop Rule
		return "Not sending to ElasticSearch"
	}

	if strings.HasPrefix(event.Source.EventType, "dns.") {
		// FIXME - Acknowledge
		// Drop Rule
		return "Not sending to ElasticSearch"
	}

	// Add Field...
	if strings.HasPrefix(event.Source.EventType, "identity.OS-TRUST") {
		// Add Tenant ID to Msg.
		event.Source.TenantID = event.Source.Payload.Initiator.ProjectID
		//fmt.Println(event)
	}
	msg, err = json.Marshal(event)
	if err != nil {
		fmt.Printf("Cannot Marshal Event: %s", err)
	}
	// Call func to load into ElasticSearch or.... load into ElasticSearch
	sink.LoadData(msg, id, es)
	//then Acknowledge rabbitmq if successful

	return "test"
}

/*
func main() {

	// RabbitMQ logic

	bytes, err := ioutil.ReadFile("cadf-example.json")

	if err != nil {
		panic(err)
	}

	jsonParsed, err := gabs.ParseJSON(bytes)
	value, ok := jsonParsed.Path("_source.event_type").Data().(string)
	fmt.Println(value)
	fmt.Println(ok)

	jsonParsed.Set("something", "fake_value")
	jsonOutput := jsonParsed.String()
	fmt.Println(jsonOutput)
}
*/
