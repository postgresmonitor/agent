package logs

import (
	"agent/config"
	"agent/data"
	"agent/db"
	"agent/logger"
	"io"
	"net/http"

	"github.com/gin-gonic/gin"
)

// Server starts a gin based router for a Heroku postgres /logs endpoint
type Server struct {
	config              config.Config
	logMetricChannel    chan data.LogMetrics
	logTestChannel      chan string
	rawSlowQueryChannel chan *db.SlowQuery
	router              *gin.Engine
}

func NewServer(config config.Config, logMetricChannel chan data.LogMetrics, logTestChannel chan string, rawSlowQueryChannel chan *db.SlowQuery) *Server {
	return &Server{
		config:              config,
		logMetricChannel:    logMetricChannel,
		logTestChannel:      logTestChannel,
		rawSlowQueryChannel: rawSlowQueryChannel,
	}
}

func (s *Server) Start() {
	// always in release mode
	gin.SetMode(gin.ReleaseMode)

	// don't use default logging since it will spam heroku logs
	router := gin.New()

	// use recovery middleware to handle any panics and returns a 500 if there was one
	router.Use(gin.Recovery())

	// authenticate that log messages are legitimate
	router.Use(Authentication())

	// ignore trusted proxies since we don't use any and there's a warning for this
	router.SetTrustedProxies(nil)

	router.POST("/logs", s.PostLogs)

	// redirect to setup flow if hitting the agent root endpoint
	router.GET("/", func(c *gin.Context) {
		c.Redirect(http.StatusFound, "https://postgresmonitor.com/app/setup/")
	})

	logger.Info("Starting /logs server", "port", s.config.Port)

	// doesn't return
	router.Run(":" + s.config.Port)
}

func (s *Server) PostLogs(c *gin.Context) {
	body, err := io.ReadAll(c.Request.Body)
	if err != nil {
		return
	}

	line := string(body)
	go s.processLogLine(line)

	c.Status(http.StatusOK)
}

func (s *Server) processLogLine(line string) {
	if shouldHandleLogLine(line) {
		s.handleLogLine(line)
	}

	if shouldHandleTestLogLine(line) {
		s.logTestChannel <- line
	}
}

func (s *Server) handleLogLine(line string) {
	if s.config.LogPostgresLogs || s.config.IsDevelopment() {
		logger.Info("Log line", "line", line)
	}

	parsedLines := parseLogLine(line)
	if parsedLines == nil || len(parsedLines) == 0 {
		return
	}

	for _, parsed := range parsedLines {
		if len(parsed.Metrics) > 0 {
			s.logMetricChannel <- parsed.Metrics
		}

		if parsed.SlowQuery != nil {
			s.rawSlowQueryChannel <- parsed.SlowQuery
		}
	}
}
