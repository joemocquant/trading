package metrics

import (
	"fmt"
	"trading/networking"
	"trading/networking/database"

	ifxClient "github.com/influxdata/influxdb/client/v2"
)

func computeOHLC(from *indicator) {

	indexPeriod := from.indexPeriod + 1

	if len(conf.Metrics.OhlcPeriods) <= indexPeriod {
		return
	}

	ind := &indicator{
		nextRun:     from.nextRun,
		indexPeriod: indexPeriod,
		period:      conf.Metrics.OhlcPeriods[indexPeriod],
		dataSource:  from.dataSource,
		source:      "ohlc_" + conf.Metrics.OhlcPeriodsStr[from.indexPeriod],
		destination: "ohlc_" + conf.Metrics.OhlcPeriodsStr[indexPeriod],
		exchange:    from.exchange,
	}

	ind.computeTimeIntervals(0)

	ind.callback = func() {
		go computeOHLC(ind)
		cacheLastOHLC(ind, conf.Metrics.LengthMax)
		go computeOBV(ind)
		go computeMA(ind)
	}

	mohlc := getOHLC(ind)
	if mohlc == nil {
		return
	}

	prepareOHLCPoints(ind, mohlc)
}

func getOHLC(ind *indicator) map[int64]map[string]*ohlc {

	query := fmt.Sprintf(
		`SELECT SUM(volume) AS volume,
      SUM(quantity) AS quantity,
      FIRST(open) AS open,
      MAX(high) AS high,
      MIN(low) AS low,
      LAST(close) AS close
    FROM %s
    WHERE time >= %d AND time < %d AND exchange = '%s'
    GROUP BY time(%s), market`,
		ind.source,
		ind.timeIntervals[0], ind.nextRun,
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

	return formatOHLC(ind, res)
}
