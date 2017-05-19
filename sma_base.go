package metrics

import (
	"fmt"
	"time"
	"trading/networking"
	"trading/networking/database"

	"github.com/Sirupsen/logrus"
	ifxClient "github.com/influxdata/influxdb/client/v2"
)

type sma struct {
	timestamp time.Time
	sum       float64
	count     int64
	sma       float64
}

func computeBaseSMA(from *indicator) {

	getBaseSMA(&indicator{
		nextRun:     from.nextRun,
		period:      conf.Metrics.Periods[from.indexPeriod],
		indexPeriod: from.indexPeriod,
		dataSource:  from.dataSource,
		source:      from.destination,
		destination: "sma_" + conf.Metrics.PeriodsStr[from.indexPeriod],
		exchange:    from.exchange,
	})
}

func getBaseSMA(ind *indicator) {

	ind.callback = func() {
		computeSMA(ind)
	}

	ind.computeTimeIntervals()

	res := getSMAFromOHLC(ind)
	if res == nil {
		return
	}

	msmas := formatSMA(ind, res)
	prepareSMAPoints(ind, msmas)
}

func getSMAFromOHLC(ind *indicator) []ifxClient.Result {

	query := fmt.Sprintf(
		`SELECT SUM(close),
      COUNT(close)
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

func formatSMA(ind *indicator, res []ifxClient.Result) map[string][]*sma {

	smas := make(map[string][]*sma, len(res[0].Series))

	for _, serie := range res[0].Series {

		market := serie.Tags["market"]

		for i, smaRec := range serie.Values {

			timestamp, err := networking.ConvertJsonValueToTime(smaRec[0])
			if err != nil {
				logger.WithFields(logrus.Fields{
					"error":    err,
					"exchange": ind.exchange,
					"market":   market,
				}).Error("formatOHLC: networking.ConvertJsonValueToTime")

				timestamp = ind.timeIntervals[i]
			}

			if smaRec[1] == nil || smaRec[2] == nil {
				logger.WithField("error", "Encountered nil value").Errorf("formatSMA %s %#v", market, smaRec)
				continue
			}

			sum, err := networking.ConvertJsonValueToFloat64(smaRec[1])
			if err != nil {
				logger.WithFields(logrus.Fields{
					"error":    err,
					"exchange": ind.exchange,
					"market":   market,
				}).Error("formatSMA: networking.ConvertJsonValueToFloat64")
				continue
			}

			count, err := networking.ConvertJsonValueToInt64(smaRec[2])
			if err != nil {
				logger.WithFields(logrus.Fields{
					"error":    err,
					"exchange": ind.exchange,
					"market":   market,
				}).Error("formatSMA: networking.ConvertJsonValueToFloat64")
				continue
			}

			smaValue := 0.0
			if count != 0.0 {
				smaValue = sum / float64(count)
			}

			smas[market] = append(smas[market], &sma{
				timestamp: timestamp,
				sum:       sum,
				count:     count,
				sma:       smaValue,
			})
		}
	}

	return smas
}

func prepareSMAPoints(ind *indicator, msmas map[string][]*sma) {

	measurement := ind.destination
	points := make([]*ifxClient.Point, 0)

	for market, smas := range msmas {

		for _, sma := range smas {

			timestamp := sma.timestamp

			tags := map[string]string{
				"market":   market,
				"exchange": ind.exchange,
			}

			fields := map[string]interface{}{
				"sum_close":   sma.sum,
				"count_close": sma.count,
				"sma_close":   sma.sma,
			}

			pt, err := ifxClient.NewPoint(measurement, tags, fields, timestamp)
			if err != nil {
				logger.WithField("error", err).Error(
					"prepareSMAPoints: ifxClient.NewPoint")
			}
			points = append(points, pt)
		}
	}

	if len(points) == 0 {
		return
	}

	batchsToWrite <- &database.BatchPoints{
		TypePoint: ind.dataSource.Schema["database"] + "SMA",
		Points:    points,
		Callback:  ind.callback,
	}
}
