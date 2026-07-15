package protocol

import "time"

const (
	CollectDuration = 5 * time.Minute
)

type TimeoutConfig struct {
	pingDuration time.Duration
	pongDuration time.Duration
	writeTimeout time.Duration
	readTimeout  time.Duration
}

func NewTimeoutConfig() TimeoutConfig {
	return TimeoutConfig{
		pingDuration: 60 * time.Second,
		pongDuration: 60 * time.Second,
		writeTimeout: 40 * time.Second,
		readTimeout:  40 * time.Second,
	}
}
