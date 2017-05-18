package metrics

import (
	"fmt"
	"trading/networking"
	"trading/networking/database"

	ifxClient "github.com/influxdata/influxdb/client/v2"
)

func computeOHLC(from *indicator) {

	indexPeriod := from.indexPeriod + 1

	if len(conf.Metrics.Periods) <= indexPeriod {
		return
	}

	ind := &indicator{
		nextRun:     from.nextRun,
		indexPeriod: indexPeriod,
		period:      conf.Metrics.Periods[indexPeriod],
		dataSource:  from.dataSource,
		source:      "ohlc_" + conf.Metrics.PeriodsStr[from.indexPeriod],
		destination: "ohlc_" + conf.Metrics.PeriodsStr[indexPeriod],
		exchange:    from.exchange,
	}

	ind.callback = func() {
		computeOHLC(ind)
	}

	ind.computeTimeIntervals()

	res := getOHLC(ind)
	if res == nil {
		return
	}

	mohlcs := formatOHLC(ind, res)
	prepareOHLCPoints(ind, mohlcs)
}

func getOHLC(ind *indicator) []ifxClient.Result {

	query := fmt.Sprintf(
		`SELECT SUM(volume) AS volume,
      SUM(quantity) AS quantity,
      FIRST(open) AS open,
      MAX(high) AS high,
      MIN(low) AS low,
      LAST(close) AS close
    FROM %s
    WHERE time >= %d AND time < %d AND exchange = '%s'
    GROUP BY time(%s), market;`,
		ind.source,
		ind.timeIntervals[0].UnixNano(), ind.nextRun,
		ind.exchange,
		ind.period)

	var res []ifxClient.Result

	request := func() (err error) {
		res, err = database.QueryDB(
			dbClient, query, conf.Metrics.Schema["database"])
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
