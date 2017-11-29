package source

/*
Source will be the RabbitMQ connectivity.

URI for connecting to RabbitMQ, Queue
*/

import (
	"fmt"
	"log"

	"github.com/notque/hermes-etl/pipeline"
	"github.com/streadway/amqp"
	elastic "gopkg.in/olivere/elastic.v5"
)

type ConnectSourcer interface {
	ConnectSource() string
}

// Connection details for RabbitMQ
type Source struct {
	URI   string
	Queue string
}

// Connect to RabbitMQ
func (s Source) ConnectSource(es *elastic.Client) string {
	conn, err := amqp.Dial(s.URI)
	failOnError(err, "Failed to connect to RabbitMQ")
	defer conn.Close()

	ch, err := conn.Channel()
	failOnError(err, "Failed to open a channel")
	defer ch.Close()

	q, err := ch.QueueDeclare(
		s.Queue, // name
		false,   // durable
		false,   // delete when unused
		false,   // exclusive
		false,   // no-wait
		nil,     // arguments
	)
	failOnError(err, "Failed to declare a queue")

	msgs, err := ch.Consume(
		q.Name, //Queue
		"",     // consumer
		true,   // auto-ack
		false,  // exclusive
		false,  // no-local
		false,  // no-wait
		nil,    //args
	)
	failOnError(err, "Failed to register a consumer")

	forever := make(chan bool)

	go func() {
		for d := range msgs {
			// Transform Step with Drop and Rename Methods
			//Transform.Rules(d.Body) excepts a pointer to the message
			//log.Printf("Received a message: %s", d.Body)
			pipeline.Incoming(d.Body, es)
		}
	}()

	log.Printf(" [*] Waiting for messages. To exit press CTRL+C")
	<-forever

	return "test"
}

// Helper function for errors for each amqp call
func failOnError(err error, msg string) {
	if err != nil {
		log.Fatalf("%s: %s", msg, err)
		panic(fmt.Sprintf("%s: %s", msg, err))
	}
}
