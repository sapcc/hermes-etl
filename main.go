package main

import (
	"flag"
	"fmt"
	"os"

	"github.com/sapcc/hermes-etl/sink"
	"github.com/sapcc/hermes-etl/source"
	"github.com/spf13/viper"
)

func main() {
	// Handle Config options, command line support for config location
	configPath := parseCmdFlags()
	setDefaultConfig()
	readConfig(configPath)

	//URI := "amqp://guest:guest@localhost:5672/"
	//fmt.Println(URI)
	sinkconn := sink.Sink{URI: viper.GetString("elasticsearch.uri")}
	sink := sinkconn.ConnectSink()
	mqconn := source.Source{URI: viper.GetString("rabbitmq.uri"), Queue: viper.GetString("rabbitmq.queue")}
	mqconn.ConnectSource(sink)

}

// parseCmdFlags grabs the location to hermes-etl.conf and parses it, or prints Usage
func parseCmdFlags() *string {
	// Get config file location
	configPath := flag.String("f", "hermes-etl.conf", "specifies the location of the TOML-format configuration file")
	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, "Usage of %s:\n", os.Args[0])
		flag.PrintDefaults()
	}
	flag.Parse()
	return configPath
}

func setDefaultConfig() {
	viper.SetDefault("elasticsearch.uri", "http://127.0.0.1:9200")
	viper.SetDefault("rabbitmq.uri", "amqp://guest:guest@localhost:5672/")
	viper.SetDefault("rabbitmq.queue", "test")
}

// readConfig reads the configuration file from the configPath
func readConfig(configPath *string) {
	// Don't read config file if the default config file isn't there,
	//  as we will just fall back to config defaults in that case
	var shouldReadConfig = true
	if _, err := os.Stat(*configPath); os.IsNotExist(err) {
		shouldReadConfig = *configPath != flag.Lookup("f").DefValue
	}
	// Now we sorted that out, read the config
	fmt.Printf("Should read config: %v, config file is %s", shouldReadConfig, *configPath)
	if shouldReadConfig {
		viper.SetConfigFile(*configPath)
		viper.SetConfigType("toml")
		err := viper.ReadInConfig()
		if err != nil { // Handle errors reading the config file
			panic(fmt.Errorf("Fatal error config file: %s", err))
		}
	}
}
