package metrics

import (
	"fmt"
	"time"
	"trading/networking"
	"trading/networking/database"

	"github.com/Sirupsen/logrus"
	ifxClient "github.com/influxdata/influxdb/client/v2"
)

type ohlc struct {
	timestamp       time.Time
	volume          float64
	quantity        float64
	weightedAverage float64
	open            float64
	high            float64
	low             float64
	close           float64
}

func computeBaseOHLC() {

	indexPeriod := 0

	getBaseOHLC(&indicator{
		indexPeriod: indexPeriod,
		period:      conf.Metrics.Periods[indexPeriod],
		dataSource:  conf.Metrics.Sources.Bittrex,
		destination: "ohlc_" + conf.Metrics.PeriodsStr[indexPeriod],
		exchange:    conf.Metrics.Sources.Bittrex.Schema["database"],
	})

	getBaseOHLC(&indicator{
		indexPeriod: indexPeriod,
		period:      conf.Metrics.Periods[indexPeriod],
		dataSource:  conf.Metrics.Sources.Poloniex,
		destination: "ohlc_" + conf.Metrics.PeriodsStr[indexPeriod],
		exchange:    conf.Metrics.Sources.Poloniex.Schema["database"],
	})
}

func getBaseOHLC(ind *indicator) {

	go networking.RunEvery(conf.Metrics.Frequency, func(nextRun int64) {

		ind.nextRun = nextRun

		ind.callback = func() {
			go computeOHLC(ind)
			go computeBaseSMA(ind)
			go computeBaseOBV(ind)
		}

		ind.computeTimeIntervals()

		res := getOHLCFromTrades(ind)
		if res == nil {
			return
		}

		mohlcs := formatOHLC(ind, res)
		addLastIfNoVolume(ind, res, mohlcs)
		prepareOHLCPoints(ind, mohlcs)
	})
}

func getOHLCFromTrades(ind *indicator) []ifxClient.Result {

	subQuery1 := fmt.Sprintf(
		`SELECT SUM(total) AS volume,
      SUM(quantity) AS quantity,
      FIRST(rate) AS open,
      MAX(rate) AS high,
      MIN(rate) AS low,
      LAST(rate) AS close
    FROM %s
    WHERE time >= %d AND time < %d
    GROUP BY time(%s), market;`,
		ind.dataSource.Schema["trades_measurement"],
		ind.timeIntervals[0].UnixNano(), ind.nextRun,
		ind.period)

	subQuery2 := fmt.Sprintf(
		`SELECT last
    FROM %s
    WHERE time >= now() - 1h
    GROUP BY market
    ORDER BY time LIMIT 1`,
		ind.dataSource.Schema["ticks_measurement"])

	query := subQuery1 + subQuery2

	var res []ifxClient.Result

	request := func() (err error) {
		res, err = database.QueryDB(
			dbClient, query, ind.dataSource.Schema["database"])
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

func formatOHLC(ind *indicator, res []ifxClient.Result) map[string][]*ohlc {

	ohlcs := make(map[string][]*ohlc, len(res[0].Series))

	for _, serie := range res[0].Series {

		market := serie.Tags["market"]

		for i, ohlcRec := range serie.Values {

			timestamp, err := networking.ConvertJsonValueToTime(ohlcRec[0])
			if err != nil {
				logger.WithFields(logrus.Fields{
					"error":    err,
					"exchange": ind.exchange,
					"market":   market,
				}).Error("formatOHLC: networking.ConvertJsonValueToTime")

				timestamp = ind.timeIntervals[i]
			}

			if ohlcRec[1] == nil || ohlcRec[2] == nil || ohlcRec[3] == nil ||
				ohlcRec[4] == nil || ohlcRec[5] == nil || ohlcRec[6] == nil {
				continue
			}

			volume, err := networking.ConvertJsonValueToFloat64(ohlcRec[1])
			if err != nil {
				logger.WithFields(logrus.Fields{
					"error":    err,
					"exchange": ind.exchange,
					"market":   market,
				}).Error("formatOHLC: networking.ConvertJsonValueToFloat64")
				continue
			}

			quantity, err := networking.ConvertJsonValueToFloat64(ohlcRec[2])
			if err != nil {
				logger.WithFields(logrus.Fields{
					"error":    err,
					"exchange": ind.exchange,
					"market":   market,
				}).Error("formatOHLC: networking.ConvertJsonValueToFloat64")
				continue
			}

			weightedAverage := 0.0
			if quantity != 0.0 {
				weightedAverage = volume / quantity
			}

			open, err := networking.ConvertJsonValueToFloat64(ohlcRec[3])
			if err != nil {
				logger.WithFields(logrus.Fields{
					"error":    err,
					"exchange": ind.exchange,
					"market":   market,
				}).Error("formatOHLC: networking.ConvertJsonValueToFloat64")
				continue
			}

			high, err := networking.ConvertJsonValueToFloat64(ohlcRec[4])
			if err != nil {
				logger.WithFields(logrus.Fields{
					"error":    err,
					"exchange": ind.exchange,
					"market":   market,
				}).Error("formatOHLC: networking.ConvertJsonValueToFloat64")
				continue
			}

			low, err := networking.ConvertJsonValueToFloat64(ohlcRec[5])
			if err != nil {
				logger.WithFields(logrus.Fields{
					"error":    err,
					"exchange": ind.exchange,
					"market":   market,
				}).Error("formatOHLC: networking.ConvertJsonValueToFloat64")
				continue
			}

			close, err := networking.ConvertJsonValueToFloat64(ohlcRec[6])
			if err != nil {
				logger.WithFields(logrus.Fields{
					"error":    err,
					"exchange": ind.exchange,
					"market":   market,
				}).Error("formatOHLC: networking.ConvertJsonValueToFloat64")
				continue
			}

			ohlcs[market] = append(ohlcs[market], &ohlc{
				timestamp:       timestamp,
				volume:          volume,
				quantity:        quantity,
				weightedAverage: weightedAverage,
				open:            open,
				high:            high,
				low:             low,
				close:           close,
			})
		}
	}

	return ohlcs
}

func addLastIfNoVolume(ind *indicator, res []ifxClient.Result,
	mohlcs map[string][]*ohlc) {

	lasts := formatLasts(res)

	for market, last := range lasts {

		if _, ok := mohlcs[market]; !ok {

			for _, interval := range ind.timeIntervals {
				mohlcs[market] = append(mohlcs[market], &ohlc{
					interval, 0.0, 0.0, 0.0, last, last, last, last,
				})
			}
		} else {

			i := 0
			for _, interval := range ind.timeIntervals {

				for _, ohlcRec := range mohlcs[market][i:] {

					if interval.Equal(ohlcRec.timestamp) {
						last = ohlcRec.close
						i++
						break
					}

					if interval.Before(ohlcRec.timestamp) {

						mohlcs[market] = append(mohlcs[market], nil)
						copy(mohlcs[market][i+1:], mohlcs[market][i:])

						mohlcs[market][i] = &ohlc{
							interval, 0.0, 0.0, 0.0, last, last, last, last,
						}
						i++
						break
					}
				}

				if interval.After(mohlcs[market][len(mohlcs[market])-1].timestamp) {
					mohlcs[market] = append(mohlcs[market], &ohlc{
						interval, 0.0, 0.0, 0.0, last, last, last, last,
					})
				}
			}
		}
	}
}

func formatLasts(res []ifxClient.Result) map[string]float64 {

	lastTicks := make(map[string]float64, len(res[1].Series))

	for _, serie := range res[1].Series {

		market := serie.Tags["market"]

		last, err := networking.ConvertJsonValueToFloat64(serie.Values[0][1])
		if err != nil {
			logger.WithFields(logrus.Fields{
				"error": err,
			}).Error("formatLasts: networking.ConvertJsonValueToFloat64")
			continue
		}

		lastTicks[market] = last
	}

	return lastTicks
}

func prepareOHLCPoints(ind *indicator, mohlcs map[string][]*ohlc) {

	measurement := ind.destination
	points := make([]*ifxClient.Point, 0)

	for market, ohlcs := range mohlcs {

		for _, ohlc := range ohlcs {

			timestamp := ohlc.timestamp

			tags := map[string]string{
				"market":   market,
				"exchange": ind.exchange,
			}

			fields := map[string]interface{}{
				"volume":           ohlc.volume,
				"quantity":         ohlc.quantity,
				"weighted_average": ohlc.weightedAverage,
				"open":             ohlc.open,
				"high":             ohlc.high,
				"low":              ohlc.low,
				"close":            ohlc.close,
			}

			pt, err := ifxClient.NewPoint(measurement, tags, fields, timestamp)
			if err != nil {
				logger.WithField("error", err).Error(
					"prepareOHLCPoints: ifxClient.NewPoint")
			}
			points = append(points, pt)
		}
	}

	if len(points) == 0 {
		return
	}

	batchsToWrite <- &database.BatchPoints{
		TypePoint: ind.dataSource.Schema["database"] + "OHLC",
		Points:    points,
		Callback:  ind.callback,
	}
}
