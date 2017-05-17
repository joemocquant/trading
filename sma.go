package metrics

import (
	"fmt"
	"time"
	"trading/networking"
	"trading/networking/database"

	ifxClient "github.com/influxdata/influxdb/client/v2"
)

func computeSMA(from *indicator) {

	indexPeriod := from.indexPeriod + 1

	if len(conf.Sma.Periods) <= indexPeriod {
		return
	}

	p, err := time.ParseDuration(conf.Sma.Periods[indexPeriod])
	if err != nil {
		logger.WithField("error", err).Fatal("computeSMA: time.ParseDuration")
	}

	ind := &indicator{
		nextRun:     from.nextRun,
		period:      p,
		indexPeriod: indexPeriod,
		dataSource:  from.dataSource,
		source:      "sma_" + conf.Sma.Periods[indexPeriod-1],
		destination: "sma_" + conf.Sma.Periods[indexPeriod],
	}

	ind.callback = func() {
		computeSMA(ind)
	}

	ind.computeTimeIntervals()

	res := getSMA(ind)
	if res == nil {
		return
	}

	msmas := formatSMA(ind, res)
	prepareSMAPoints(ind, msmas)
}

func getSMA(ind *indicator) []ifxClient.Result {

	query := fmt.Sprintf(
		`SELECT SUM(sum_close),
      COUNT(count_close)
    FROM %s
    WHERE time >= %d AND time < %d AND exchange = '%s'
    GROUP BY time(%s), market;`,
		ind.source,
		ind.timeIntervals[0].UnixNano(), ind.nextRun,
		ind.dataSource.Schema["database"],
		ind.period)

	var res []ifxClient.Result

	request := func() (err error) {
		res, err = database.QueryDB(
			dbClient, query, conf.Schema["database"])
		return err
	}

	success := networking.ExecuteRequest(&networking.RequestInfo{
		Logger:   logger.WithField("query", query),
		Period:   ind.period,
		ErrorMsg: "runQuery: database.QueryDB",
		Request:  request,
	})

	if !success {
		return nil
	}

	return res
}
