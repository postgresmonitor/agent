package agent

import (
	"agent/api"
	"agent/config"
	"agent/data"
	"agent/db"
	"agent/logger"
	"agent/logs"
	"agent/schedule"
	"agent/util"
	"math/rand"
	"net/http"
	"time"

	"github.com/gammazero/deque"
)

const maxBufferedRequests = 10

type Agent struct {
	config              config.Config
	data                *data.Data
	requests            *deque.Deque[*api.ReportRequest]
	logMetricChannel    chan data.LogMetrics
	logTestChannel      chan string
	serverChannel       chan *db.PostgresServer
	databaseChannel     chan *db.Database
	replicationChannel  chan *db.Replication
	metricsChannel      chan *db.Metric
	queryStatsChannel   chan *db.QueryStats
	settingsChannel     chan *db.Setting
	rawSlowQueryChannel chan *db.SlowQuery
}

func New(config config.Config) *Agent {
	return &Agent{
		config:              config,
		data:                &data.Data{},
		requests:            deque.New[*api.ReportRequest](maxBufferedRequests, maxBufferedRequests),
		logMetricChannel:    make(chan data.LogMetrics),
		logTestChannel:      make(chan string),
		serverChannel:       make(chan *db.PostgresServer),
		databaseChannel:     make(chan *db.Database),
		replicationChannel:  make(chan *db.Replication),
		metricsChannel:      make(chan *db.Metric),
		queryStatsChannel:   make(chan *db.QueryStats),
		settingsChannel:     make(chan *db.Setting),
		rawSlowQueryChannel: make(chan *db.SlowQuery),
	}
}

func (a *Agent) Run() {
	logger.Info("Starting Postgres Monitor Agent", "uuid", a.config.UUID.String(), "version", a.config.Version)

	// brokers messages between channels
	go a.updateDataChannels()

	// report every 60 seconds with some initial jitter delay to smooth out requests
	delayJitter := time.Duration(rand.Intn(30)) * time.Second
	go schedule.Schedule(a.sendRequest, 60*time.Second, delayJitter)

	// starts it's own go routines
	go a.startPostgresObserver()

	// doesn't return and runs in main thread
	a.startServer()
}

// build servers and metadata and report them once
func (a *Agent) Test() {
	logger.Info("Testing Postgres Monitor agent is setup correctly")

	// brokers messages between channels
	go a.updateDataChannels()

	// bootstrap pgbouncer, server metadata and schemas
	a.newObserver().BootstrapMetatdataAndSchemas()

	// wait a few seconds for bootstrapped data to be sent to the data channels
	logger.Info("Waiting 5 seconds to send initial request...")
	time.Sleep(5 * time.Second)

	a.sendRequest()
}

// raises error in first db and makes sure that logs server receives it
func (a *Agent) TestLogs() {
	logger.Info("Testing database logs are setup correctly")

	logger.Info("Writing log test message")
	a.newObserver().WriteLogTestMessage()

	logger.Info("Successfully wrote log test message!")
}

func (a *Agent) startServer() {
	logsServer := logs.NewServer(a.config, a.logMetricChannel, a.logTestChannel, a.rawSlowQueryChannel)
	logsServer.Start() // doesn't return
}

func (a *Agent) startPostgresObserver() {
	a.newObserver().Start()
}

func (a *Agent) newObserver() *db.Observer {
	return db.NewObserver(a.config, a.serverChannel, a.databaseChannel, a.replicationChannel, a.metricsChannel, a.queryStatsChannel, a.settingsChannel, a.rawSlowQueryChannel)
}

// runs forever
func (a *Agent) updateDataChannels() {
	for {
		select {
		case logMetrics := <-a.logMetricChannel:
			a.data.AddLogMetrics(logMetrics)
		case <-a.logTestChannel:
			// log test messages are only sent when a --test-logs flag run occurs
			// send an immediate request up with the log test messsage timestamp set
			logger.Info("Log test message was received")
			a.data.AddLogTestMessageReceivedAt(time.Now().Unix())
			go a.sendRequest()
		case postgresServer := <-a.serverChannel:
			a.data.AddPostgresServer(postgresServer)
		case database := <-a.databaseChannel:
			a.data.AddDatabase(database)
		case replication := <-a.replicationChannel:
			a.data.AddReplication(replication)
		case metric := <-a.metricsChannel:
			a.data.AddMetric(metric)
		case setting := <-a.settingsChannel:
			a.data.AddSetting(setting)
		case stats := <-a.queryStatsChannel:
			a.data.AddQueryStats(stats)
		}
	}
}

// scheduled
func (a *Agent) sendRequest() {
	d := a.data.CopyAndReset()
	request := api.NewReportRequest(a.config, d, time.Now().UTC().Unix())

	if request.Valid() {
		// add latest request to buffered requests, sending the latest two requests each call to this method
		// at 2 requests per call, we'll backfill data gradually

		// uses a deque to store last 10 requests as a LIFO stack
		// we keep the most recent 10 requests - overwriting the oldest first
		// the deque implementation does not support a max capacity so we
		// manage it ourselves by removing the oldest request once we reach the max # of requests
		if a.requests.Len() == maxBufferedRequests {
			a.requests.PopFront() // remove the oldest request
		}

		a.requests.PushBack(&request)

		// send the last two requests
		for i := 0; i < 2; i++ {
			if a.requests.Len() > 0 {
				// get last request that was just added
				apiRequest := a.requests.PopBack()
				success := a.sendSingleRequest(apiRequest)
				if !success {
					logger.Info("Saving failed request to retry later")
					a.requests.PushBack(apiRequest)
					break
				}
				time.Sleep(1 * time.Second)
			}
		}
	}
}

func (a *Agent) sendSingleRequest(request *api.ReportRequest) bool {
	var success bool

	compressed, err := request.ToCompressedJSON()

	if a.config.IsLogDebug() {
		json, err := request.ToJSON()
		if err == nil {
			jsonBytes := len(json)
			logger.Debug("JSON Request", "json", string(json))
			logger.Debug("JSON Bytes", "bytes", jsonBytes)

			compressedBytes := compressed.Len()
			compression := float64(jsonBytes) / float64(compressedBytes)
			logger.Debug("Compressed JSON Bytes", "bytes", compressedBytes, "compression", util.Round(compression))
		}
	}

	if err != nil {
		logger.Error("Error generating JSON", "err", err)
	} else {
		request, err := http.NewRequest("POST", a.config.APIEndpoint, compressed)
		request.Header.Set("Authorization", "Bearer "+a.config.APIKey)
		request.Header.Set("Content-Encoding", "gzip")
		request.Header.Set("Content-Type", "application/json; charset=utf-8")
		request.Header.Set("User-Agent", "postgres-monitor-agent/"+a.config.Version)

		client := &http.Client{
			Timeout: 10 * time.Second,
		}
		response, err := client.Do(request)

		if err != nil {
			logger.Error("Request error", "err", err)
			// treat non 500 request failures as successes so we drop the request
			if response != nil && response.StatusCode < 500 {
				success = true
			}
		} else {
			defer response.Body.Close()

			logger.Info("Request status", "status", response.Status)
			if response.StatusCode == 200 {
				success = true
				if a.config.TestMode {
					logger.Info("Test Success!")
				}
			} else if response.StatusCode == 401 {
				logger.Warn("Invalid API Key", "status", 401)
				success = true
			} else if response.StatusCode < 500 {
				// treat non 500 request failures as successes so we drop the request
				success = true
			}
		}
	}

	return success
}
