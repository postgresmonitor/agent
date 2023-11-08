package aws

import (
	"agent/config"
	"agent/db"
	"agent/logger"
	"agent/schedule"
	"agent/util"
	"context"
	"encoding/json"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awssdk "github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/cloudwatch"
	"github.com/aws/aws-sdk-go-v2/service/cloudwatch/types"
	"github.com/aws/aws-sdk-go-v2/service/cloudwatchlogs"
)

var RDSMetrics = []string{
	"CheckpointLag",
	"ConnectionAttempts",
	"CPUUtilization",
	"DiskQueueDepth",
	"FreeableMemory",
	"FreeStorageSpace",
	"MaximumUsedTransactionIDs",
	"NetworkReceiveThroughput",
	"NetworkTransmitThroughput",
	"OldestReplicationSlotLag",
	"ReadIOPS",
	"ReadLatency",
	"ReadThroughput",
	"ReplicaLag",
	"ReplicationSlotDiskUsage",
	"SwapUsage",
	"TransactionLogsDiskUsage",
	"TransactionLogsGeneration",
	"WriteIOPS",
	"WriteLatency",
	"WriteThroughput",
}

var AuroraMetrics = []string{
	"AuroraReplicaLag",
	"AuroraReplicaLagMaximum",
	"AuroraReplicaLagMinimum",
	"BufferCacheHitRatio",
	"CommitLatency",
	"CommitThroughput",
	"ConnectionAttempts",
	"CPUUtilization",
	"Deadlocks",
	"DiskQueueDepth",
	"FreeableMemory",
	"FreeLocalStorage",
	"MaximumUsedTransactionIDs",
	"NetworkReceiveThroughput",
	"NetworkTransmitThroughput",
	"RDSToAuroraPostgreSQLReplicaLag",
	"ReadIOPS",
	"ReadLatency",
	"ReadThroughput",
	"ReplicationSlotDiskUsage",
	"StorageNetworkReceiveThroughput",
	"StorageNetworkTransmitThroughput",
	"SwapUsage",
	"TransactionLogsDiskUsage",
	"WriteIOPS",
	"WriteLatency",
	"WriteThroughput",
}

type Observer struct {
	config                       config.Config
	awsInstanceDiscoveredChannel chan *db.RDSInstanceFoundEvent
	dataChannel                  chan interface{}
	slowQueryChannel             chan *db.SlowQuery
	awsLogFiles                  []*AWSLogFile
	rdsInstances                 []*RDSInstance
	mu                           sync.Mutex // to protect rdsInstances
	scheduledMonitors            bool
}

type RDSInstanceMetrics struct {
	RDSInstance   *RDSInstance
	MetricResults []MetricResult
}

type MetricResult struct {
	MetricName string
	Datapoints []MetricDatapoint
}

type MetricDatapoint struct {
	Time  time.Time
	Value float64
}

type AWSLogFile struct {
	InstanceID  string
	LogFileName string
	Marker      string
}

func NewObserver(config config.Config, awsInstanceDiscoveredChannel chan *db.RDSInstanceFoundEvent, dataChannel chan interface{}, slowQueryChannel chan *db.SlowQuery) *Observer {
	return &Observer{
		config:                       config,
		awsInstanceDiscoveredChannel: awsInstanceDiscoveredChannel,
		dataChannel:                  dataChannel,
		slowQueryChannel:             slowQueryChannel,
		rdsInstances:                 make([]*RDSInstance, 0),
		awsLogFiles:                  make([]*AWSLogFile, 0),
		mu:                           sync.Mutex{},
	}
}

// Starts go routines to monitor cloudwatch metrics and logs
func (o *Observer) Run() {
	go o.WatchForRDSInstances()
}

func (o *Observer) WatchForRDSInstances() {
	for {
		select {
		case event := <-o.awsInstanceDiscoveredChannel:
			go o.TrackRDSInstance(event)
		}
	}
}

func (o *Observer) TrackRDSInstance(event *db.RDSInstanceFoundEvent) {
	o.mu.Lock()
	defer o.mu.Unlock()

	logger.Info("Observer: tracking RDS Instance", "instance_id", event.InstanceID, "is_aurora", event.IsAurora)

	rdsInstance := GetRDSInstance(event.InstanceID)
	rdsInstance.IsAurora = event.IsAurora

	o.rdsInstances = append(o.rdsInstances, rdsInstance)

	if !o.scheduledMonitors {
		if o.config.MonitorCloudwatchMetrics {
			go schedule.ScheduleAndRunNow(o.MonitorCloudwatchMetrics, o.config.MonitorCloudwatchMetricsInterval)
			go schedule.ScheduleAndRunNow(o.MonitorCloudwatchLogs, o.config.MonitorCloudwatchLogsInterval)
		}

		if o.config.MonitorAWSLogs {
			go schedule.ScheduleAndRunNow(o.MonitorAWSLogs, o.config.MonitorAWSLogsInterval)
		}

		o.scheduledMonitors = true
	}
}

func (o *Observer) MonitorCloudwatchMetrics() {
	o.mu.Lock()
	defer o.mu.Unlock()

	for _, rdsInstance := range o.rdsInstances {
		metricResults := o.FetchMetrics(rdsInstance)
		if len(metricResults) > 0 {
			o.dataChannel <- &RDSInstanceMetrics{
				RDSInstance:   rdsInstance,
				MetricResults: metricResults,
			}
		}
	}
}

func (o *Observer) FetchMetrics(rdsInstance *RDSInstance) []MetricResult {
	logger.Debug("AWS Observer: fetching metrics", "instance_id", rdsInstance.InstanceID)

	ctx := context.Background()

	var awsConfig awssdk.Config
	var err error
	// support config profiles
	if os.Getenv("AWS_CONFIG_PROFILE") != "" {
		awsConfig, err = awsconfig.LoadDefaultConfig(ctx,
			awsconfig.WithSharedConfigProfile(os.Getenv("AWS_CONFIG_PROFILE")),
		)
	} else {
		awsConfig, err = awsconfig.LoadDefaultConfig(ctx)
	}
	if err != nil {
		logger.Error(err.Error())
		return []MetricResult{}
	}

	// cloudwatch metrics can take up to 10 minutes to finalize
	startTime := GetAWSTime(-15 * time.Minute)
	endTime := GetAWSTime(-10 * time.Minute)

	client := cloudwatch.NewFromConfig(awsConfig)

	output, err := client.GetMetricData(ctx, &cloudwatch.GetMetricDataInput{
		StartTime:         startTime,
		EndTime:           endTime,
		ScanBy:            types.ScanByTimestampDescending,
		MetricDataQueries: o.BuiltMetricDataQueries(rdsInstance),
	})
	if err != nil {
		logger.Error(err.Error())
		return []MetricResult{}
	}

	var metricResults []MetricResult

	for _, metricResult := range output.MetricDataResults {
		var datapoints []MetricDatapoint
		for index, timestamp := range metricResult.Timestamps {
			datapoints = append(datapoints, MetricDatapoint{
				Time:  timestamp,
				Value: metricResult.Values[index],
			})
		}

		if len(datapoints) > 0 {
			metricResults = append(metricResults, MetricResult{
				MetricName: *metricResult.Label,
				Datapoints: datapoints,
			})
		}
	}

	return metricResults
}

// https://github.com/awsdocs/aws-doc-sdk-examples/blob/main/gov2/cloudwatch/GetMetricData/GetMetricDatav2.go
func (o *Observer) BuiltMetricDataQueries(rdsInstance *RDSInstance) []types.MetricDataQuery {
	var metricDataQueries []types.MetricDataQuery

	var metricNames []string
	if rdsInstance.IsAurora {
		metricNames = AuroraMetrics
	} else {
		metricNames = RDSMetrics
	}

	for _, metricName := range metricNames {
		metricDataQueries = append(metricDataQueries, types.MetricDataQuery{
			Id: aws.String(strings.ToLower(metricName)),
			MetricStat: &types.MetricStat{
				Metric: &types.Metric{
					Namespace:  aws.String("AWS/RDS"),
					MetricName: aws.String(metricName),
					Dimensions: []types.Dimension{
						{
							Name:  aws.String("DBInstanceIdentifier"),
							Value: aws.String(rdsInstance.InstanceID),
						},
					},
				},
				Period: aws.Int32(60), // get 1 minute datapoints for 5 minute window
				Stat:   aws.String("Average"),
			},
		})
	}

	return metricDataQueries
}

func (o *Observer) MonitorCloudwatchLogs() {
	o.mu.Lock()
	defer o.mu.Unlock()

	for _, rdsInstance := range o.rdsInstances {
		// enhanced monitoring is required for rds os logs
		if rdsInstance.EnhancedMonitoringEnabled {
			metricResults := o.FetchRDSOSMetrics(rdsInstance)
			if len(metricResults) > 0 {
				o.dataChannel <- &RDSInstanceMetrics{
					RDSInstance:   rdsInstance,
					MetricResults: metricResults,
				}
			}
		}
	}
}

func (o *Observer) FetchRDSOSMetrics(rdsInstance *RDSInstance) []MetricResult {
	logger.Debug("AWS Observer: fetching rds os metrics", "instance_id", rdsInstance.InstanceID)

	ctx := context.Background()

	var awsConfig awssdk.Config
	var err error
	// support config profiles
	if os.Getenv("AWS_CONFIG_PROFILE") != "" {
		awsConfig, err = awsconfig.LoadDefaultConfig(ctx,
			awsconfig.WithSharedConfigProfile(os.Getenv("AWS_CONFIG_PROFILE")),
		)
	} else {
		awsConfig, err = awsconfig.LoadDefaultConfig(ctx)
	}
	if err != nil {
		logger.Error(err.Error())
		return []MetricResult{}
	}

	client := cloudwatchlogs.NewFromConfig(awsConfig)

	input := &cloudwatchlogs.GetLogEventsInput{
		LogGroupName:  aws.String("RDSOSMetrics"),
		LogStreamName: aws.String(rdsInstance.MonitoringResourceId),
		Limit:         aws.Int32(1), // get last log event
	}

	response, err := client.GetLogEvents(ctx, input)
	if err != nil {
		logger.Error(err.Error())
		return []MetricResult{}
	}

	if len(response.Events) > 0 {
		return o.ConvertRDSOSLogEventIntoMetrics(*response.Events[0].Message)
	}

	return []MetricResult{}
}

func (o *Observer) ConvertRDSOSLogEventIntoMetrics(message string) []MetricResult {
	var metricResults []MetricResult

	event := RDSOSMetricsEvent{}
	err := json.Unmarshal([]byte(message), &event)
	if err != nil {
		logger.Error(err.Error())
		return metricResults
	}

	timestamp := util.ParseTimestampToTime(event.Timestamp)

	// cpu
	metricResults = append(metricResults, o.BuildMetricResult("aws.rds.os.cpu.guest", timestamp, event.CPUUtilization.Guest))
	metricResults = append(metricResults, o.BuildMetricResult("aws.rds.os.cpu.irq", timestamp, event.CPUUtilization.IRQ))
	metricResults = append(metricResults, o.BuildMetricResult("aws.rds.os.cpu.idle", timestamp, event.CPUUtilization.Idle))
	metricResults = append(metricResults, o.BuildMetricResult("aws.rds.os.cpu.nice", timestamp, event.CPUUtilization.Nice))
	metricResults = append(metricResults, o.BuildMetricResult("aws.rds.os.cpu.steal", timestamp, event.CPUUtilization.Steal))
	metricResults = append(metricResults, o.BuildMetricResult("aws.rds.os.cpu.system", timestamp, event.CPUUtilization.System))
	metricResults = append(metricResults, o.BuildMetricResult("aws.rds.os.cpu.total", timestamp, event.CPUUtilization.Total))
	metricResults = append(metricResults, o.BuildMetricResult("aws.rds.os.cpu.user", timestamp, event.CPUUtilization.User))
	metricResults = append(metricResults, o.BuildMetricResult("aws.rds.os.cpu.wait", timestamp, event.CPUUtilization.Wait))

	// load avg
	metricResults = append(metricResults, o.BuildMetricResult("aws.rds.os.load_avg.1m", timestamp, event.LoadAverage.OneMinute))
	metricResults = append(metricResults, o.BuildMetricResult("aws.rds.os.load_avg.5m", timestamp, event.LoadAverage.FiveMinute))
	metricResults = append(metricResults, o.BuildMetricResult("aws.rds.os.load_avg.15m", timestamp, event.LoadAverage.FifteenMinute))

	// memory
	metricResults = append(metricResults, o.BuildMetricResult("aws.rds.os.memory.active", timestamp, float64(event.Memory.Active)))
	metricResults = append(metricResults, o.BuildMetricResult("aws.rds.os.memory.inactive", timestamp, float64(event.Memory.Inactive)))
	metricResults = append(metricResults, o.BuildMetricResult("aws.rds.os.memory.free", timestamp, float64(event.Memory.Free)))
	metricResults = append(metricResults, o.BuildMetricResult("aws.rds.os.memory.cached", timestamp, float64(event.Memory.Cached)))
	metricResults = append(metricResults, o.BuildMetricResult("aws.rds.os.memory.total", timestamp, float64(event.Memory.Total)))
	metricResults = append(metricResults, o.BuildMetricResult("aws.rds.os.memory.dirty", timestamp, float64(event.Memory.Dirty)))
	metricResults = append(metricResults, o.BuildMetricResult("aws.rds.os.memory.page_tables", timestamp, float64(event.Memory.PageTables)))
	metricResults = append(metricResults, o.BuildMetricResult("aws.rds.os.memory.mapped", timestamp, float64(event.Memory.Mapped)))
	metricResults = append(metricResults, o.BuildMetricResult("aws.rds.os.memory.slab", timestamp, float64(event.Memory.Slab)))
	metricResults = append(metricResults, o.BuildMetricResult("aws.rds.os.memory.buffers", timestamp, float64(event.Memory.Buffers)))
	metricResults = append(metricResults, o.BuildMetricResult("aws.rds.os.memory.write_back", timestamp, float64(event.Memory.Writeback)))
	metricResults = append(metricResults, o.BuildMetricResult("aws.rds.os.memory.huge_pages.free", timestamp, float64(event.Memory.HugePagesFree)))
	metricResults = append(metricResults, o.BuildMetricResult("aws.rds.os.memory.huge_pages.reserved", timestamp, float64(event.Memory.HugePagesReserved)))
	metricResults = append(metricResults, o.BuildMetricResult("aws.rds.os.memory.huge_pages.surplus", timestamp, float64(event.Memory.HugePagesSurplus)))
	metricResults = append(metricResults, o.BuildMetricResult("aws.rds.os.memory.huge_pages.size", timestamp, float64(event.Memory.HugePagesSize)))
	metricResults = append(metricResults, o.BuildMetricResult("aws.rds.os.memory.huge_pages.total", timestamp, float64(event.Memory.HugePagesTotal)))

	// swap
	metricResults = append(metricResults, o.BuildMetricResult("aws.rds.os.memory.swap.cached", timestamp, float64(event.Swap.Cached)))
	metricResults = append(metricResults, o.BuildMetricResult("aws.rds.os.memory.swap.total", timestamp, float64(event.Swap.Total)))
	metricResults = append(metricResults, o.BuildMetricResult("aws.rds.os.memory.swap.free", timestamp, float64(event.Swap.Free)))

	// disk io
	for _, diskIO := range event.DiskIO {
		deviceName := diskIO.Device
		if deviceName == "" {
			deviceName = "default"
		}
		metricPrefix := "aws.rds.os.disk.io." + deviceName
		metricResults = append(metricResults, o.BuildMetricResult(metricPrefix+".avg_queue_length", timestamp, diskIO.AverageQueueLength))
		metricResults = append(metricResults, o.BuildMetricResult(metricPrefix+".avg_request_size", timestamp, diskIO.AverageRequestSize))
		metricResults = append(metricResults, o.BuildMetricResult(metricPrefix+".await", timestamp, diskIO.Await))
		metricResults = append(metricResults, o.BuildMetricResult(metricPrefix+".rrqmps", timestamp, diskIO.RRQMPS))
		metricResults = append(metricResults, o.BuildMetricResult(metricPrefix+".read.iops", timestamp, diskIO.ReadIOPS))
		metricResults = append(metricResults, o.BuildMetricResult(metricPrefix+".read.kb", timestamp, diskIO.ReadKB))
		metricResults = append(metricResults, o.BuildMetricResult(metricPrefix+".read.kbps", timestamp, diskIO.ReadKBPS))
		metricResults = append(metricResults, o.BuildMetricResult(metricPrefix+".read.latency", timestamp, diskIO.ReadLatency))
		metricResults = append(metricResults, o.BuildMetricResult(metricPrefix+".read.throughput", timestamp, diskIO.ReadThroughput))
		metricResults = append(metricResults, o.BuildMetricResult(metricPrefix+".transactions_per_sec", timestamp, diskIO.TransactionsPerSec))
		metricResults = append(metricResults, o.BuildMetricResult(metricPrefix+".util", timestamp, diskIO.Util))
		metricResults = append(metricResults, o.BuildMetricResult(metricPrefix+".wrqmps", timestamp, diskIO.WRQMPS))
		metricResults = append(metricResults, o.BuildMetricResult(metricPrefix+".write.iops", timestamp, diskIO.WriteIOPS))
		metricResults = append(metricResults, o.BuildMetricResult(metricPrefix+".write.kb", timestamp, diskIO.WriteKB))
		metricResults = append(metricResults, o.BuildMetricResult(metricPrefix+".write.kbps", timestamp, diskIO.WriteKBPS))
		metricResults = append(metricResults, o.BuildMetricResult(metricPrefix+".write.latency", timestamp, diskIO.WriteLatency))
		metricResults = append(metricResults, o.BuildMetricResult(metricPrefix+".write.throughput", timestamp, diskIO.WriteThroughput))
	}

	// file sys
	for _, fileSys := range event.FileSys {
		fileSysName := fileSys.Name
		if fileSysName == "" {
			// fallback to mountpoint for an identifier
			fileSysName = strings.ReplaceAll(fileSys.MountPoint, "/", "")
		}
		metricPrefix := "aws.rds.os.filesys." + fileSysName
		metricResults = append(metricResults, o.BuildMetricResult(metricPrefix+".used", timestamp, float64(fileSys.Used)))
		metricResults = append(metricResults, o.BuildMetricResult(metricPrefix+".total", timestamp, float64(fileSys.Total)))
		metricResults = append(metricResults, o.BuildMetricResult(metricPrefix+".used_percent", timestamp, fileSys.UsedPercent))
		metricResults = append(metricResults, o.BuildMetricResult(metricPrefix+".used_files", timestamp, float64(fileSys.UsedFiles)))
		metricResults = append(metricResults, o.BuildMetricResult(metricPrefix+".max_files", timestamp, float64(fileSys.MaxFiles)))
		metricResults = append(metricResults, o.BuildMetricResult(metricPrefix+".used_files_percent", timestamp, fileSys.UsedFilesPercent))
	}

	return metricResults
}

func (o *Observer) BuildMetricResult(name string, timestamp time.Time, value float64) MetricResult {
	return MetricResult{
		MetricName: name,
		Datapoints: []MetricDatapoint{
			{
				Time:  timestamp,
				Value: value,
			},
		},
	}
}

func (o *Observer) MonitorAWSLogs() {
	o.mu.Lock()
	defer o.mu.Unlock()

	for _, rdsInstance := range o.rdsInstances {
		logFileNames := ListRDSLogFiles(rdsInstance.InstanceID)
		for _, logFileName := range logFileNames {

			cachedLogFile := o.FindOrCreateCachedAWSLogFile(rdsInstance.InstanceID, logFileName)
			marker := cachedLogFile.Marker

			logFile, newMarker := GetRDSLogFile(rdsInstance.InstanceID, logFileName, &marker)

			if logFile != nil {
				cachedLogFile.Marker = *newMarker

				if len(*logFile) > 0 {
					// TODO: remove this or make debug
					logger.Info("RDS Log File", "instanceID", rdsInstance.InstanceID, "fileName", logFileName, "len", len(*logFile), "marker", *newMarker)

					rdsLogLines := ParseRDSLogFile(*logFile)
					o.ProcessRDSLogLines(rdsLogLines, rdsInstance.InstanceID)
				}
			}
		}

		// remove stale cached log files for instance id
		o.RemoveStaleCachedAWSLogFiles(rdsInstance.InstanceID, logFileNames)
	}
}

func (o *Observer) FindOrCreateCachedAWSLogFile(instanceID string, fileName string) *AWSLogFile {
	for _, awsLogFile := range o.awsLogFiles {
		if awsLogFile.InstanceID == instanceID && awsLogFile.LogFileName == fileName {
			return awsLogFile
		}
	}

	awsLogFile := &AWSLogFile{
		InstanceID:  instanceID,
		LogFileName: fileName,
	}

	o.awsLogFiles = append(o.awsLogFiles, awsLogFile)

	return awsLogFile
}

func (o *Observer) RemoveStaleCachedAWSLogFiles(instanceID string, logFileNames []string) {
	var keptLogFiles []*AWSLogFile

	for _, existingLogFile := range o.awsLogFiles {
		// keep all log files that are for another instance id
		if existingLogFile.InstanceID != instanceID {
			keptLogFiles = append(keptLogFiles, existingLogFile)
			continue
		}

		// keep log file name matches that are still around
		for _, logFileName := range logFileNames {
			if existingLogFile.LogFileName == logFileName {
				keptLogFiles = append(keptLogFiles, existingLogFile)
				continue
			}
		}
	}

	o.awsLogFiles = keptLogFiles
}
