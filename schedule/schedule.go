package schedule

import (
	"time"
)

func Schedule(f func(), t time.Duration, delay time.Duration) {
	// support delay/jitter when scheduling tasks
	if delay > 0 {
		time.Sleep(delay)
	}

	ticker := time.NewTicker(t)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			f()
		}
	}
}

func ScheduleAndRunNow(f func(), t time.Duration) {
	// run the function now but don't block
	go f()

	// schedule for the future with no delay
	Schedule(f, t, 0)
}
