package metrics

import (
	"fmt"
	"trading/networking"
	"trading/networking/database"

	ifxClient "github.com/influxdata/influxdb/client/v2"
)

func computeOBV(from *indicator) {

	indexPeriod := from.indexPeriod + 1

	if len(conf.Metrics.Periods) <= indexPeriod {
		return
	}

	ind := &indicator{
		nextRun:     from.nextRun,
		indexPeriod: indexPeriod,
		period:      conf.Metrics.Periods[indexPeriod],
		dataSource:  from.dataSource,
		source:      "obv_" + conf.Metrics.PeriodsStr[from.indexPeriod],
		destination: "obv_" + conf.Metrics.PeriodsStr[indexPeriod],
		exchange:    from.exchange,
	}

	ind.callback = func() {
		computeOBV(ind)
	}

	ind.computeTimeIntervals()

	res := getOBV(ind)
	if res == nil {
		return
	}

	mobvs := formatOBV(ind, res)
	prepareOBVPoints(ind, mobvs)
}

func getOBV(ind *indicator) []ifxClient.Result {

	query := fmt.Sprintf(
		`SELECT SUM(obv)
    FROM %s
    WHERE time >= %d AND time < %d AND exchange = '%s'
    GROUP BY time(%s), market;`,
		ind.source,
		ind.timeIntervals[0].UnixNano()-int64(ind.period), ind.nextRun,
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
