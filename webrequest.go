package networking

import (
	"time"

	"github.com/Sirupsen/logrus"
)

type RequestInfo struct {
	Logger   *logrus.Entry
	Period   time.Duration
	ErrorMsg string
	Request  func() error
}

func ExecuteRequest(ri *RequestInfo) bool {

	begin := time.Now()
	timeout := ri.Period * 2 / 3
	if timeout == 0 {
		timeout = time.Hour * 1
	}

	err := ri.Request()

	for err != nil {

		ri.Logger.WithField("error", err).Error(ri.ErrorMsg)
		time.Sleep(5 * time.Second)

		if time.Since(begin) > timeout {
			ri.Logger.Errorf("%s (request timeout)", ri.ErrorMsg)
			return false
		}

		err = ri.Request()
	}

	return true
}
