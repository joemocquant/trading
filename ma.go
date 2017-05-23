package metrics

import (
	"fmt"
	"strconv"
	"strings"
	"time"
	"trading/networking"
	"trading/networking/database"

	"github.com/Sirupsen/logrus"
	ifxClient "github.com/influxdata/influxdb/client/v2"
)

type ma struct {
	smas map[int]float64
	emas map[int]float64
}

func computeMA(from *indicator) {

	ind := &indicator{
		nextRun:     from.nextRun,
		period:      conf.Metrics.OhlcPeriods[from.indexPeriod],
		indexPeriod: from.indexPeriod,
		dataSource:  from.dataSource,
		source:      from.destination,
		destination: "ma_" + conf.Metrics.OhlcPeriodsStr[from.indexPeriod],
		exchange:    from.exchange,
	}

	offset := conf.Metrics.MaMax - 1
	if offset < 1 {
		logger.WithField("error", "conf: ma_max must be greater than 1").Fatal(
			"ComputeMA")
	}

	ind.computeTimeIntervals(offset)

	res := getMAFromOHLC(ind)
	if res == nil {
		return
	}

	imma := formatMA(ind, res)
	prepareMAPoints(ind, imma)
}

func getMAFromOHLC(ind *indicator) []ifxClient.Result {

	subQuery1 := fmt.Sprintf(
		`SELECT close
    FROM %s
    WHERE time >= %d AND time < %d AND exchange = '%s'
    GROUP BY market;`,
		ind.source,
		ind.timeIntervals[0], ind.nextRun,
		ind.exchange)

	fields := make([]string, conf.Metrics.MaMax)
	for i := 0; i < conf.Metrics.MaMax; i++ {
		fields[i] = "ema_" + strconv.Itoa(i+1)
	}
	selectedFields := strings.Join(fields, ", ")

	subqQuery2 := fmt.Sprintf(
		`SELECT %s
    FROM %s
    WHERE time = %d AND exchange = '%s'
    GROUP BY market;`,
		selectedFields,
		ind.destination,
		ind.timeIntervals[conf.Metrics.MaMax-2],
		ind.exchange)

	query := subQuery1 + subqQuery2
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

func formatMA(ind *indicator,
	res []ifxClient.Result) map[int64]map[string]*ma {

	imclose := make(map[int64]map[string]float64, len(ind.timeIntervals))
	for _, interval := range ind.timeIntervals {
		imclose[interval] = make(map[string]float64, len(res[0].Series))
	}

	for _, serie := range res[0].Series {

		market := serie.Tags["market"]

		for _, closeRec := range serie.Values {

			if closeRec[0] == nil || closeRec[1] == nil {
				continue
			}

			timestamp, err := networking.ConvertJsonValueToTime(closeRec[0])
			if err != nil {
				logger.WithFields(logrus.Fields{
					"error":    err,
					"exchange": ind.exchange,
					"market":   market,
				}).Error("formatSMA: networking.ConvertJsonValueToTime")
				continue
			}

			close, err := networking.ConvertJsonValueToFloat64(closeRec[1])
			if err != nil {
				logger.WithFields(logrus.Fields{
					"error":    err,
					"exchange": ind.exchange,
					"market":   market,
				}).Error("formatSMA: networking.ConvertJsonValueToFloat64")
				continue
			}

			if mclose, ok := imclose[timestamp.UnixNano()]; !ok {

				logger.WithFields(logrus.Fields{
					"error":    "timestamp not matching",
					"exchange": ind.exchange,
					"market":   market,
				}).Error("formatSMA")

			} else {
				mclose[market] = close
			}
		}
	}

	imma := make(map[int64]map[string]*ma, len(ind.timeIntervals))
	for _, interval := range ind.timeIntervals[conf.Metrics.MaMax-1:] {
		imma[interval] = make(map[string]*ma, len(res[0].Series))
	}

	memas := getPreviousEMA(ind, res)

	for _, serie := range res[0].Series {

		market := serie.Tags["market"]

		if _, ok := memas[market]; !ok {
			memas[market] = make(map[int]float64, conf.Metrics.MaMax)
		}

		for i, interval := range ind.timeIntervals[conf.Metrics.MaMax-1:] {

			imma[interval][market] = &ma{
				smas: make(map[int]float64, conf.Metrics.MaMax),
				emas: make(map[int]float64, conf.Metrics.MaMax),
			}
			cumulativeSum := 0.0

			for maLength := 1; maLength <= conf.Metrics.MaMax; maLength++ {

				intervalOffset := ind.timeIntervals[i+conf.Metrics.MaMax-maLength]

				if close, ok := imclose[intervalOffset][market]; ok {

					cumulativeSum += close
					sma := cumulativeSum / float64(maLength)
					imma[interval][market].smas[maLength] = sma

				} else {
					break
				}
			}

			for maLength := 1; maLength <= conf.Metrics.MaMax; maLength++ {

				var emaSeed float64
				if ema, ok := memas[market][maLength]; ok {
					emaSeed = ema
				} else if close, ok := imclose[interval-int64(ind.period)][market]; ok {
					emaSeed = close
				} else {
					continue
				}

				if close, ok := imclose[interval][market]; ok {

					multiplier := 2.0 / float64(maLength+1)
					ema := (close-emaSeed)*multiplier + emaSeed
					imma[interval][market].emas[maLength] = ema
					memas[market][maLength] = ema
				} else {
					break
				}
			}
		}
	}

	return imma
}

func getPreviousEMA(ind *indicator,
	res []ifxClient.Result) map[string]map[int]float64 {

	memas := make(map[string]map[int]float64, len(res[1].Series))

	for _, serie := range res[1].Series {

		market := serie.Tags["market"]

		for _, emasRec := range serie.Values {

			memas[market] = make(map[int]float64, conf.Metrics.MaMax)

			for i := 1; i <= conf.Metrics.MaMax; i++ {
				if emasRec[i] == nil {
					continue
				}

				ema, err := networking.ConvertJsonValueToFloat64(emasRec[1])
				if err != nil {
					logger.WithFields(logrus.Fields{
						"error":    err,
						"exchange": ind.exchange,
						"market":   market,
					}).Error("formatMA: networking.ConvertJsonValueToFloat64")
					continue
				}

				memas[market][i] = ema
			}
		}
	}

	return memas
}

func prepareMAPoints(ind *indicator, imma map[int64]map[string]*ma) {

	measurement := ind.destination
	points := make([]*ifxClient.Point, 0)

	for interval, mma := range imma {

		timestamp := time.Unix(0, interval)

		for market, ma := range mma {

			tags := map[string]string{
				"market":   market,
				"exchange": ind.exchange,
			}

			fields := make(map[string]interface{}, len(ma.smas)+len(ma.emas))

			for i, sma := range ma.smas {

				smaLength := fmt.Sprintf("sma_%d", i)
				fields[smaLength] = sma
			}

			for i, ema := range ma.emas {

				emaLength := fmt.Sprintf("ema_%d", i)
				fields[emaLength] = ema
			}

			pt, err := ifxClient.NewPoint(measurement, tags, fields, timestamp)
			if err != nil {
				logger.WithField("error", err).Error(
					"prepareMAPoints: ifxClient.NewPoint")
			}
			points = append(points, pt)
		}
	}

	if len(points) == 0 {
		return
	}

	batchsToWrite <- &database.BatchPoints{
		TypePoint: ind.dataSource.Schema["database"] + "MA",
		Points:    points,
		Callback:  ind.callback,
	}
}
