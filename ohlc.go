package metrics

import (
	"fmt"
	"time"
	"trading/networking"
	"trading/networking/database"

	ifxClient "github.com/influxdata/influxdb/client/v2"
)

func computeOHLC(from *indicator) {

	indexPeriod := from.indexPeriod + 1

	if len(conf.Ohlc.Periods) <= indexPeriod {
		return
	}

	p, err := time.ParseDuration(conf.Ohlc.Periods[indexPeriod])
	if err != nil {
		logger.WithField("error", err).Fatal("computeOHLC: time.ParseDuration")
	}

	ind := &indicator{
		nextRun:     from.nextRun,
		period:      p,
		indexPeriod: indexPeriod,
		dataSource:  from.dataSource,
		source:      "ohlc_" + conf.Ohlc.Periods[indexPeriod-1],
		destination: "ohlc_" + conf.Ohlc.Periods[indexPeriod],
	}

	ind.callback = func() {
		computeOHLC(ind)
	}

	res := getOHLC(ind)
	if res == nil {
		return
	}

	mohlcs := formatOHLCs(ind, res)
	prepareOHLCPoints(ind, mohlcs)
}

func getOHLC(ind *indicator) []ifxClient.Result {

	delta := ind.nextRun % int64(ind.period)
	start := int64(ind.nextRun) - delta - 2*int64(ind.period)
	end := ind.nextRun

	query := fmt.Sprintf(
		`SELECT SUM(volume) AS volume,
      SUM(quantity) AS quantity,
      FIRST(open) AS open,
      MAX(high) AS high,
      MIN(low) AS low,
      LAST(close) AS close
    FROM %s
    WHERE time >= %d AND time < %d
    GROUP BY time(%s), market;`,
		ind.source,
		start, end,
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
