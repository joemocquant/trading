package metrics

import (
	"fmt"
	"time"
	"trading/networking"
	"trading/networking/database"

	"github.com/Sirupsen/logrus"
	ifxClient "github.com/influxdata/influxdb/client/v2"
)

type obv struct {
	timestamp time.Time
	obv       float64
}

func computeBaseOBV(from *indicator) {

	getBaseOBV(&indicator{
		nextRun:     from.nextRun,
		period:      conf.Metrics.Periods[from.indexPeriod],
		indexPeriod: from.indexPeriod,
		dataSource:  from.dataSource,
		source:      from.destination,
		destination: "obv_" + conf.Metrics.PeriodsStr[from.indexPeriod],
		exchange:    from.exchange,
	})
}

func getBaseOBV(ind *indicator) {

	ind.callback = func() {
		computeOBV(ind)
	}

	ind.computeTimeIntervals()

	res := getOBVFromOHLC(ind)
	if res == nil {
		return
	}

	mobvs := formatOBV(ind, res)
	prepareOBVPoints(ind, mobvs)
}

func getOBVFromOHLC(ind *indicator) []ifxClient.Result {

	query := fmt.Sprintf(
		`SELECT SUM(volume)
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

func formatOBV(ind *indicator, res []ifxClient.Result) map[string][]*obv {

	mvolumes := make(map[string][]*obv, len(res[0].Series))

	for i, serie := range res[0].Series {

		market := serie.Tags["market"]

		for _, volume := range serie.Values {

			timestamp, err := networking.ConvertJsonValueToTime(volume[0])
			if err != nil {
				logger.WithFields(logrus.Fields{
					"error":    err,
					"exchange": ind.exchange,
					"market":   market,
				}).Error("formatOBV: networking.ConvertJsonValueToTime")

				ind.timeIntervals[i].Add(-ind.period)
			}

			if volume[1] == nil {
				mvolumes[market] = append(mvolumes[market], &obv{timestamp, -1})
				continue
			}

			volume, err := networking.ConvertJsonValueToFloat64(volume[1])
			if err != nil {
				logger.WithFields(logrus.Fields{
					"error":    err,
					"exchange": ind.exchange,
					"market":   market,
				}).Error("formatOBV: networking.ConvertJsonValueToFloat64")

				mvolumes[market] = append(mvolumes[market], &obv{timestamp, -1})
				continue
			}

			mvolumes[market] = append(mvolumes[market], &obv{timestamp, volume})
		}
	}

	mobvs := make(map[string][]*obv)

	for market, volumes := range mvolumes {

		for i, rec := range volumes {

			if i == 0 {
				continue
			}

			volume := rec.obv
			previousVolume := volumes[i-1].obv

			if volume == -1 || previousVolume == -1 {
				continue
			}

			obvValue := 0.0

			if volume > previousVolume {
				obvValue = previousVolume + volume

			} else if volume == previousVolume {
				obvValue = volume

			} else {
				obvValue = previousVolume - volume
			}

			mobvs[market] = append(mobvs[market], &obv{
				timestamp: rec.timestamp,
				obv:       obvValue,
			})

		}
	}

	return mobvs
}

func prepareOBVPoints(ind *indicator, mobvs map[string][]*obv) {

	measurement := ind.destination
	points := make([]*ifxClient.Point, 0)

	for market, obvs := range mobvs {

		for _, obv := range obvs {

			timestamp := obv.timestamp

			tags := map[string]string{
				"market":   market,
				"exchange": ind.exchange,
			}

			fields := map[string]interface{}{
				"obv": obv.obv,
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
