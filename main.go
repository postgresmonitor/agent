package main

import (
	"flag"
	"os"
	"postgres-monitor/agent"
	"postgres-monitor/config"
	"postgres-monitor/logger"

	_ "github.com/heroku/x/hmetrics/onload"
	"github.com/joho/godotenv"
)

func main() {
	flag.Bool("help", false, "Help flag")
	var testFlag = flag.Bool("test", false, "Flag to test postgres monitor agent setup")
	var testLogsFlag = flag.Bool("test-logs", false, "Flag to test postgres logs setup")
	var devFlag = flag.Bool("dev", false, "Flag to toggle dev mode")
	flag.Parse()

	if *devFlag {
		// load .env file
		err := godotenv.Load()
		if err != nil {
			logger.Error("Error loading .env file")
			os.Exit(1)
		}
	}

	config := config.New()
	if *testFlag || *testLogsFlag {
		config.SetTestMode()
	}
	agent := agent.New(config)

	if *testFlag {
		agent.Test()
	} else if *testLogsFlag {
		agent.TestLogs()
	} else {
		agent.Run()
	}
}
