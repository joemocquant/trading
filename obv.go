package metrics

import (
	"fmt"
	"time"
	"trading/networking"
	"trading/networking/database"

	"github.com/Sirupsen/logrus"
	ifxClient "github.com/influxdata/influxdb/client/v2"
)

func computeOBV(from *indicator) {

	ind := &indicator{
		nextRun:     from.nextRun,
		period:      conf.Metrics.OhlcPeriods[from.indexPeriod],
		indexPeriod: from.indexPeriod,
		dataSource:  from.dataSource,
		source:      from.destination,
		destination: "obv_" + conf.Metrics.OhlcPeriodsStr[from.indexPeriod],
		exchange:    from.exchange,
	}

	ind.computeTimeIntervals(1)

	imobv := getOBV(ind)
	if imobv == nil {
		return
	}

	prepareOBVPoints(ind, imobv)
}

func getOBV(ind *indicator) map[int64]map[string]float64 {

	query := fmt.Sprintf(
		`SELECT volume
    FROM %s
    WHERE time >= %d AND time < %d AND exchange = '%s'
    GROUP BY market;`,
		ind.source,
		ind.timeIntervals[0], ind.nextRun,
		ind.exchange)

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

	imobv := formatOBV(ind, res)
	// setCachedOBV(ind, imobv)

	return imobv
}

func formatOBV(ind *indicator,
	res []ifxClient.Result) map[int64]map[string]float64 {

	imvol := make(map[int64]map[string]float64, len(ind.timeIntervals))
	for _, interval := range ind.timeIntervals {
		imvol[interval] = make(map[string]float64, len(res[0].Series))
	}

	for _, serie := range res[0].Series {

		market := serie.Tags["market"]

		for _, volRec := range serie.Values {

			if volRec[0] == nil || volRec[1] == nil {
				continue
			}

			timestamp, err := networking.ConvertJsonValueToTime(volRec[0])
			if err != nil {
				logger.WithFields(logrus.Fields{
					"error":    err,
					"exchange": ind.exchange,
					"market":   market,
				}).Error("formatOBV: networking.ConvertJsonValueToTime")
				continue
			}

			volume, err := networking.ConvertJsonValueToFloat64(volRec[1])
			if err != nil {
				logger.WithFields(logrus.Fields{
					"error":    err,
					"exchange": ind.exchange,
					"market":   market,
				}).Error("formatOBV: networking.ConvertJsonValueToFloat64")
				continue
			}

			if mvol, ok := imvol[timestamp.UnixNano()]; !ok {

				logger.WithFields(logrus.Fields{
					"error":    "timestamp not matching",
					"exchange": ind.exchange,
					"market":   market,
				}).Error("formatOBV")

			} else {
				mvol[market] = volume
			}
		}
	}

	imobv := make(map[int64]map[string]float64, len(ind.timeIntervals))
	for _, interval := range ind.timeIntervals {
		imobv[interval] = make(map[string]float64, len(res[0].Series))
	}

	for _, serie := range res[0].Series {

		market := serie.Tags["market"]

		for interval, mvol := range imvol {

			if prevVol, ok := imvol[interval-int64(ind.period)][market]; ok {

				if vol, ok := mvol[market]; ok {

					obvValue := 0.0

					if vol > prevVol {
						obvValue = prevVol + vol

					} else if vol == prevVol {
						obvValue = vol

					} else {
						obvValue = prevVol - vol
					}

					if mobv, ok := imobv[interval]; !ok {

						logger.WithFields(logrus.Fields{
							"error":    "timestamp not matching",
							"exchange": ind.exchange,
							"market":   market,
						}).Error("formatOBV")

					} else {
						mobv[market] = obvValue
					}
				}
			}
		}
	}

	return imobv
}

func prepareOBVPoints(ind *indicator, imobv map[int64]map[string]float64) {

	measurement := ind.destination
	points := make([]*ifxClient.Point, 0)

	for interval, mobv := range imobv {

		timestamp := time.Unix(0, interval)

		for market, obv := range mobv {

			tags := map[string]string{
				"market":   market,
				"exchange": ind.exchange,
			}

			fields := map[string]interface{}{
				"obv": obv,
			}

			pt, err := ifxClient.NewPoint(measurement, tags, fields, timestamp)
			if err != nil {
				logger.WithField("error", err).Error(
					"prepareOBVPoints: ifxClient.NewPoint")
			}
			points = append(points, pt)
		}
	}

	if len(points) == 0 {
		return
	}

	batchsToWrite <- &database.BatchPoints{
		TypePoint: ind.dataSource.Schema["database"] + "OBV",
		Points:    points,
		Callback:  ind.callback,
	}
}
