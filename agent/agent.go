package agent

import (
	"agent/api"
	"agent/aws"
	"agent/config"
	"agent/data"
	"agent/db"
	"agent/errors"
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
	config                  config.Config
	data                    *data.Data
	requests                *deque.Deque[*api.ReportRequest]
	startLogsServerChannel  chan bool
	logTestChannel          chan string
	dataChannel             chan interface{}
	rawSlowQueryChannel     chan *db.SlowQuery
	awsInstanceFoundChannel chan *db.RDSInstanceFoundEvent
	stats                   *util.Stats
}

func New(config config.Config) *Agent {
	return &Agent{
		config:                  config,
		data:                    &data.Data{},
		requests:                deque.New[*api.ReportRequest](maxBufferedRequests, maxBufferedRequests),
		startLogsServerChannel:  make(chan bool, 1),
		dataChannel:             make(chan interface{}, 100),
		logTestChannel:          make(chan string, 10),
		rawSlowQueryChannel:     make(chan *db.SlowQuery, 100),
		awsInstanceFoundChannel: make(chan *db.RDSInstanceFoundEvent, 10),
		stats:                   &util.Stats{},
	}
}

func (a *Agent) Run() {
	logger.Info("Starting Postgres Monitor Agent", "uuid", a.config.UUID.String(), "version", a.config.Version)

	// brokers messages between channels
	go a.updateDataChannels()

	// report every 60 seconds with some initial jitter delay to smooth out requests
	delayJitter := time.Duration(rand.Intn(30)) * time.Second
	go schedule.Schedule(a.sendRequest, 60*time.Second, delayJitter)

	if a.config.MonitorCloudwatchMetrics {
		go a.newAWSObserver().Run()
	}

	// doesn't return and runs in main thread
	a.startPostgresObserver()
}

// build servers and metadata and report them once
func (a *Agent) Test() {
	logger.Info("Testing Postgres Monitor agent is setup correctly")

	// brokers messages between channels
	go a.updateDataChannels()

	// bootstrap pgbouncer, server metadata and schemas
	a.newPostgresObserver().BootstrapMetatdataAndSchemas()

	// wait a few seconds for bootstrapped data to be sent to the data channels
	logger.Info("Waiting 5 seconds to send initial request...")
	time.Sleep(5 * time.Second)

	a.sendRequest()
}

// raises error in first db and makes sure that logs server receives it
func (a *Agent) TestLogs() {
	logger.Info("Testing database logs are setup correctly")

	logger.Info("Writing log test message")
	a.newPostgresObserver().WriteLogTestMessage()

	logger.Info("Successfully wrote log test message!")
}

func (a *Agent) startServer() {
	logsServer := logs.NewServer(a.config, a.dataChannel, a.logTestChannel, a.rawSlowQueryChannel, a.stats)
	logsServer.Start() // doesn't return
}

func (a *Agent) startPostgresObserver() {
	a.newPostgresObserver().Start()
}

func (a *Agent) newPostgresObserver() *db.Observer {
	return db.NewObserver(a.config, a.dataChannel, a.startLogsServerChannel, a.rawSlowQueryChannel, a.awsInstanceFoundChannel)
}

func (a *Agent) newAWSObserver() *aws.Observer {
	return aws.NewObserver(a.config, a.awsInstanceFoundChannel, a.dataChannel, a.rawSlowQueryChannel)
}

// runs forever
func (a *Agent) updateDataChannels() {
	// TODO: consider using separate select {} blocks per channel to ensure
	// that processing of all channels happens equally in situations where
	// one channel is overwhelmingly more busy than the others
	for {
		select {
		case startLogsServer := <-a.startLogsServerChannel:
			// only start logs server if the current platform supports it
			if startLogsServer {
				go a.startServer()
			}
		case data := <-a.dataChannel:
			// general channel for multiple kinds of data
			a.data.AddData(data)
		case <-a.logTestChannel:
			// log test messages are only sent when a --test-logs flag run occurs
			// send an immediate request up with the log test messsage timestamp set
			logger.Info("Log test message was received")
			a.data.AddLogTestMessageReceivedAt(time.Now().Unix())
			go a.sendRequest()
		case err := <-errors.ErrorsChannel:
			a.data.AddErrorReport(err)
		}
	}
}

// scheduled
func (a *Agent) sendRequest() {
	d := a.data.CopyAndReset()
	stats := a.stats.CopyAndReset()
	request := api.NewReportRequest(a.config, d, time.Now().UTC().Unix(), stats)

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
