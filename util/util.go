package util

import (
	"math"
	"time"
)

// round to 2 decimal places
func Round(value float64) float64 {
	return RoundPlaces(value, 2)
}

// round to 4 decimal places
func Round4(value float64) float64 {
	return RoundPlaces(value, 4)
}

func RoundPlaces(value float64, places int) float64 {
	round := math.Pow(10, float64(places))
	return math.Ceil(value*round) / round
}

func Percent(n float64, d float64) float64 {
	if d == 0.0 {
		return 0.0
	} else {
		return Round4(n / d)
	}
}

func HitPercent(hit float64, read float64) float64 {
	total := hit + read
	if total == 0 {
		return 0.0
	} else {
		return Round4(hit / (total))
	}
}

func ParseTimestampToUnix(timestamp string) int64 {
	return ParseTimestampToTime(timestamp).Unix()
}

// Parses 2022-03-24T23:59:31+00:00 into a time object
func ParseTimestampToTime(timestamp string) time.Time {
	parsed, err := time.Parse(time.RFC3339, timestamp)
	if err != nil {
		parsed = time.Now() // fallback to now
	}

	return parsed
}
