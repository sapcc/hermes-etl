package cadf-test

import (
	"fmt"
	"log"

	"io/ioutil"
)

// RabbitMQ at 127.0.0.1
func main() {
	dat, err := ioutil.ReadFile("cadf-example.json")
	failOnError(err, "Could not Read File")

	fmt.Printf(string(dat))

}

// Helper function for errors for each amqp call
func failOnError(err error, msg string) {
	if err != nil {
		log.Fatalf("%s: %s", msg, err)
		panic(fmt.Sprintf("%s: %s", msg, err))
	}
}
