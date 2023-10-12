package aws

import (
	"agent/logger"
	"context"
	"os"

	awssdk "github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/rds"
)

type RDSInstanceFoundEvent struct {
	InstanceID string
	IsAurora   bool
}

type RDSInstance struct {
	EnhancedMonitoringEnabled bool
	InstanceID                string
	InstanceClass             string
	MonitoringResourceId      string // the DbiResourceId is needed for cloudwatch log calls
	IsAurora                  bool
}

// https://docs.aws.amazon.com/AmazonRDS/latest/UserGuide/USER_Monitoring-Available-OS-Metrics.html
type RDSOSMetricsEvent struct {
	Engine             string `json:"engine"`
	InstanceID         string `json:"instanceID"`
	InstanceResourceID string `json:"instanceResourceID"`
	Timestamp          string `json:"timestamp"`
	Version            int32  `json:"version"`
	Uptime             string `json:"uptime"`
	NumVCPUs           int32  `json:"numVCPUs"`

	CPUUtilization RDSCPUUtilization `json:"cpuUtilization"`
	LoadAverage    RDSLoadAverage    `json:"loadAverageMinute"`
	Memory         RDSMemory         `json:"memory"`
	Swap           RDSSwap           `json:"swap"`
	DiskIO         []RDSDiskIO       `json:"diskIO"`
	FileSys        []RDSFileSys      `json:"fileSys"`

	// ignore network for now since deltas are needed
	// ignore processlist
}

type RDSCPUUtilization struct {
	Guest  float64 `json:"guest"`
	IRQ    float64 `json:"irq"`
	System float64 `json:"system"`
	Wait   float64 `json:"wait"`
	Idle   float64 `json:"idle"`
	User   float64 `json:"user"`
	Total  float64 `json:"total"`
	Steal  float64 `json:"steal"`
	Nice   float64 `json:"nice"`
}

type RDSLoadAverage struct {
	OneMinute     float64 `json:"one"`
	FiveMinute    float64 `json:"five"`
	FifteenMinute float64 `json:"fifteen"`
}

type RDSMemory struct {
	Writeback         int64 `json:"writeback"`
	HugePagesFree     int64 `json:"hugePagesFree"`
	HugePagesReserved int64 `json:"hugePagesRsvd"`
	HugePagesSurplus  int64 `json:"hugePagesSurp"`
	HugePagesSize     int64 `json:"hugePagesSize"`
	HugePagesTotal    int64 `json:"hugePagesTotal"`
	Cached            int64 `json:"cached"`
	Free              int64 `json:"free"`
	Active            int64 `json:"active"`
	Inactive          int64 `json:"inactive"`
	Total             int64 `json:"total"`
	Dirty             int64 `json:"dirty"`
	PageTables        int64 `json:"pageTables"`
	Mapped            int64 `json:"mapped"`
	Slab              int64 `json:"slab"`
	Buffers           int64 `json:"buffers"`
}

type RDSSwap struct {
	Cached int64 `json:"cached"`
	Total  int64 `json:"total"`
	Free   int64 `json:"free"`
}

type RDSDiskIO struct {
	Device             string  `json:"device"`
	AverageQueueLength float64 `json:"avgQueueLen"`
	AverageRequestSize float64 `json:"avgReqSz"`
	Await              float64 `json:"await"`
	ReadLatency        float64 `json:"readLatency"`
	WriteLatency       float64 `json:"writeLatency"`
	WriteThroughput    float64 `json:"writeThroughput"`
	ReadThroughput     float64 `json:"readThroughput"`
	ReadIOPS           float64 `json:"readIOsPS"`
	ReadKB             float64 `json:"readKb"`
	ReadKBPS           float64 `json:"readKbPS"`
	RRQMPS             float64 `json:"rrqmPS"`
	TransactionsPerSec float64 `json:"tps"`
	WriteIOPS          float64 `json:"writeIOsPS"`
	WriteKB            float64 `json:"writeKb"`
	WriteKBPS          float64 `json:"writeKbPS"`
	WRQMPS             float64 `json:"wrqmPS"`
	Util               float64 `json:"util"`
}

type RDSFileSys struct {
	Name             string  `json:"name"`
	MountPoint       string  `json:"mountPoint"`
	Used             int64   `json:"used"`
	Total            int64   `json:"total"`
	UsedPercent      float64 `json:"usedPercent"`
	UsedFiles        int64   `json:"usedFiles"`
	MaxFiles         int64   `json:"maxFiles"`
	UsedFilesPercent float64 `json:"usedFilePercent"`
}

func GetRDSInstance(instanceId string) *RDSInstance {
	var rdsInstance RDSInstance

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
		return nil
	}

	rdsClient := rds.NewFromConfig(awsConfig)

	input := &rds.DescribeDBInstancesInput{
		DBInstanceIdentifier: &instanceId,
	}
	dbInstances, err := rdsClient.DescribeDBInstances(ctx, input)
	if err != nil {
		logger.Error(err.Error())
	} else {
		dbInstance := dbInstances.DBInstances[0]
		instanceClass := dbInstance.DBInstanceClass
		resourceId := dbInstance.DbiResourceId
		enhancedMonitoring := dbInstance.EnhancedMonitoringResourceArn != nil

		logger.Info("Found RDS DB Instance:", "instance_id", instanceId, "instance_class", instanceClass, "monitoring_resource_id", resourceId, "enhanced_monitoring", enhancedMonitoring)

		rdsInstance = RDSInstance{
			EnhancedMonitoringEnabled: enhancedMonitoring,
			InstanceID:                instanceId,
			InstanceClass:             *instanceClass,
			MonitoringResourceId:      *resourceId,
		}
	}

	return &rdsInstance
}
