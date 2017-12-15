package source

/*
Source will be the RabbitMQ connectivity.

URI for connecting to RabbitMQ, Queue
*/

import (
	"bytes"
	"encoding/json"
	"fmt"
	"log"

	"github.com/streadway/amqp"
)

type Consumer struct {
	Conn    *amqp.Connection
	Channel *amqp.Channel
	Tag     string
	Done    chan error
}

func NewConsumer(amqpURI, exchangeType, queueName, key, ctag string) (*Consumer, error) {
	c := &Consumer{
		Conn:    nil,
		Channel: nil,
		Tag:     ctag,
		Done:    make(chan error),
	}

	var err error

	log.Printf("dialing %q", amqpURI)
	c.Conn, err = amqp.Dial(amqpURI)
	if err != nil {
		return nil, fmt.Errorf("Dial: %s", err)
	}

	go func() {
		fmt.Printf("closing: %s", <-c.Conn.NotifyClose(make(chan *amqp.Error)))
	}()

	log.Printf("got Connection, getting Channel")
	c.Channel, err = c.Conn.Channel()
	if err != nil {
		return nil, fmt.Errorf("Channel: %s", err)
	}

	log.Printf("declaring Queue %q", queueName)
	queue, err := c.Channel.QueueDeclare(
		queueName, // name of the queue
		false,      // durable
		false,     // delete when usused
		false,     // exclusive
		false,     // noWait
		nil,       // arguments
	)
	if err != nil {
		return nil, fmt.Errorf("Queue Declare: %s", err)
	}

	log.Printf("declared Queue (%q %d messages, %d consumers)",
		queue.Name, queue.Messages, queue.Consumers)

	return c, nil
}

func (c *Consumer) Shutdown() error {
	// will close() the deliveries channel
	if err := c.Channel.Cancel(c.Tag, true); err != nil {
		return fmt.Errorf("Consumer cancel failed: %s", err)
	}

	if err := c.Conn.Close(); err != nil {
		return fmt.Errorf("AMQP connection close error: %s", err)
	}

	defer log.Printf("AMQP shutdown OK")

	// wait for handle() to exit
	return <-c.Done
}

func ConsumeQueue(c *amqp.Channel, queue string, done chan error) error {
	deliveries, err := c.Consume(queue, "", false, false, false, false, nil)
	if err != nil {
		log.Panicf("All the problems")
	}

	for {
		select {
		case <-done:
			return nil
		case msg := <-deliveries:
			var result map[string]interface{}
			err := json.NewDecoder(bytes.NewReader(msg.Body)).Decode(&result)
			if err != nil {
				log.Panicf("couldn't decode message to JSON: %s", err)
			}
			
			//Handle Message
			log.Printf(
				"got %dB delivery: [%v] %q",
				len(msg.Body),
				msg.DeliveryTag,
				msg.Body,
			)
			msg.Ack(false)
		}
	}

}
