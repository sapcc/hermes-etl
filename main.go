package main

import (
	"github.com/notque/hermes-etl/sink"
	"github.com/notque/hermes-etl/source"
)

func main() {
	//URI := "amqp://guest:guest@localhost:5672/"
	//fmt.Println(URI)
	sinkconn := sink.Sink{URI: "http://127.0.0.1:9200"}
	sink := sinkconn.ConnectSink()
	mqconn := source.Source{URI: "amqp://guest:guest@localhost:5672/", Queue: "hello"}
	mqconn.ConnectSource(sink)

}
