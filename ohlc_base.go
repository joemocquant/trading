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

	period, err := time.ParseDuration(conf.Ohlc.Periods[0])
	if err != nil {
		logger.WithField("error", err).Fatal("computeBaseOHLC: time.ParseDuration")
	}

	frequency, err := time.ParseDuration(conf.Ohlc.Frequency)
	if err != nil {
		logger.WithField("error", err).Fatal("computeBaseOHLC: time.ParseDuration")
	}

	getBaseOHLC(&indicator{
		period:      period,
		indexPeriod: 0,
		dataSource:  conf.Sources.Bittrex,
		destination: "ohlc_" + conf.Ohlc.Periods[0],
	}, frequency)

	getBaseOHLC(&indicator{
		period:      period,
		indexPeriod: 0,
		dataSource:  conf.Sources.Poloniex,
		destination: "ohlc_" + conf.Ohlc.Periods[0],
	}, frequency)
}

func getBaseOHLC(ind *indicator, frequency time.Duration) {

	go networking.RunEvery(frequency, func(nextRun int64) {

		ind.nextRun = nextRun

		ind.callback = func() {
			go computeOHLC(ind)
			go computeBaseSMA(ind)
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
    WHERE time >= now() - 6h
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
					"error":  err,
					"market": market,
				}).Error("formatOHLC: networking.ConvertJsonValueToTime")

				timestamp = ind.timeIntervals[i]
			}

			if ohlcRec[1] == nil && ohlcRec[2] == nil && ohlcRec[3] == nil &&
				ohlcRec[4] == nil && ohlcRec[5] == nil && ohlcRec[6] == nil {

				if len(ohlcs[market]) != 0 {

					ohlcs[market] = append(ohlcs[market], &ohlc{
						timestamp:       timestamp,
						volume:          0.0,
						quantity:        0.0,
						weightedAverage: 0.0,
						open:            ohlcs[market][len(ohlcs[market])-1].open,
						high:            ohlcs[market][len(ohlcs[market])-1].high,
						low:             ohlcs[market][len(ohlcs[market])-1].low,
						close:           ohlcs[market][len(ohlcs[market])-1].close,
					})
				}
				continue
			}

			volume, err := networking.ConvertJsonValueToFloat64(ohlcRec[1])
			if err != nil {
				logger.WithFields(logrus.Fields{
					"error":  err,
					"market": market,
				}).Error("formatOHLC: networking.ConvertJsonValueToFloat64")
				continue
			}

			quantity, err := networking.ConvertJsonValueToFloat64(ohlcRec[2])
			if err != nil {
				logger.WithFields(logrus.Fields{
					"error":  err,
					"market": market,
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
					"error":  err,
					"market": market,
				}).Error("formatOHLC: networking.ConvertJsonValueToFloat64")
				continue
			}

			high, err := networking.ConvertJsonValueToFloat64(ohlcRec[4])
			if err != nil {
				logger.WithFields(logrus.Fields{
					"error":  err,
					"market": market,
				}).Error("formatOHLC: networking.ConvertJsonValueToFloat64")
				continue
			}

			low, err := networking.ConvertJsonValueToFloat64(ohlcRec[5])
			if err != nil {
				logger.WithFields(logrus.Fields{
					"error":  err,
					"market": market,
				}).Error("formatOHLC: networking.ConvertJsonValueToFloat64")
				continue
			}

			close, err := networking.ConvertJsonValueToFloat64(ohlcRec[6])
			if err != nil {
				logger.WithFields(logrus.Fields{
					"error":  err,
					"market": market,
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
	timestamp := ind.timeIntervals[len(ind.timeIntervals)-1]

	for market, last := range lasts {

		defaultOHLC := &ohlc{
			timestamp:       timestamp,
			volume:          0.0,
			quantity:        0.0,
			weightedAverage: 0.0,
			open:            last,
			high:            last,
			low:             last,
			close:           last,
		}

		if _, ok := mohlcs[market]; !ok {
			mohlcs[market] = append(mohlcs[market], defaultOHLC)
		}
	}
}

func formatLasts(res []ifxClient.Result) map[string]float64 {

	lasts := make(map[string]float64, len(res[1].Series))

	for _, serie := range res[1].Series {

		market := serie.Tags["market"]

		last, err := networking.ConvertJsonValueToFloat64(serie.Values[0][1])
		if err != nil {
			logger.WithFields(logrus.Fields{
				"error":  err,
				"market": market,
			}).Error("formatLasts: networking.ConvertJsonValueToFloat64")
			continue
		}

		lasts[market] = last
	}

	return lasts
}

func prepareOHLCPoints(ind *indicator, mohlcs map[string][]*ohlc) {

	measurement := ind.destination
	points := make([]*ifxClient.Point, 0)

	for market, ohlcs := range mohlcs {

		for _, ohlc := range ohlcs {

			timestamp := ohlc.timestamp

			tags := map[string]string{
				"market":   market,
				"exchange": ind.dataSource.Schema["database"],
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
