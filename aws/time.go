package aws

import (
	"time"

	awstime "github.com/aws/aws-sdk-go-v2/aws"
)

// https://github.com/awsdocs/aws-doc-sdk-examples/blob/main/gov2/cloudwatch/GetMetricData/GetMetricDatav2.go
func GetAWSTime(duration time.Duration) *time.Time {
	return awstime.Time(time.Unix(time.Now().Add(time.Duration(duration)).Unix(), 0))
}
