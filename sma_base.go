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

	period, err := time.ParseDuration(conf.Sma.Periods[0])
	if err != nil {
		logger.WithField("error", err).Fatal("computeBaseSMA: time.ParseDuration")
	}

	frequency, err := time.ParseDuration(conf.Sma.Frequency)
	if err != nil {
		logger.WithField("error", err).Fatal("computeBaseSMA: time.ParseDuration")
	}

	getBaseSMA(&indicator{
		nextRun:     from.nextRun,
		period:      period,
		indexPeriod: 0,
		dataSource:  from.dataSource,
		source:      from.destination,
		destination: "sma_" + conf.Sma.Periods[0],
	}, frequency)
}

func getBaseSMA(ind *indicator, frequency time.Duration) {

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

func formatSMA(ind *indicator, res []ifxClient.Result) map[string][]*sma {

	smas := make(map[string][]*sma, len(res[0].Series))

	for _, serie := range res[0].Series {

		market := serie.Tags["market"]

		for i, smaRec := range serie.Values {

			timestamp, err := networking.ConvertJsonValueToTime(smaRec[0])
			if err != nil {
				logger.WithFields(logrus.Fields{
					"error":  err,
					"market": market,
				}).Error("formatOHLC: networking.ConvertJsonValueToTime")

				timestamp = ind.timeIntervals[i]
			}

			if smaRec[1] == nil {

				if len(smas[market]) != 0 {

					smas[market] = append(smas[market], &sma{
						timestamp: timestamp,
						count:     0.0,
						sum:       0.0,
						sma:       smas[market][len(smas[market])-1].sma,
					})
				}
				continue
			}

			sum, err := networking.ConvertJsonValueToFloat64(smaRec[1])
			if err != nil {
				logger.WithFields(logrus.Fields{
					"error":  err,
					"market": market,
				}).Error("formatSMA: networking.ConvertJsonValueToFloat64")
				continue
			}

			count, err := networking.ConvertJsonValueToInt64(smaRec[2])
			if err != nil {
				logger.WithFields(logrus.Fields{
					"error":  err,
					"market": market,
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
				"exchange": ind.dataSource.Schema["database"],
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
