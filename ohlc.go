package metrics

import (
	"fmt"
	"math"
	"time"
	"trading/networking"
	"trading/networking/database"

	"github.com/Sirupsen/logrus"
	ifxClient "github.com/influxdata/influxdb/client/v2"
)

type ohlc struct {
	volume          float64
	quantity        float64
	weightedAverage float64
	open            float64
	high            float64
	low             float64
	close           float64
	change          float64
	changePercent   float64
}

func computeBaseOHLC() {

	indexPeriod := 0

	for exchange, _ := range conf.Metrics.Sources {

		ind := &indicator{
			period:      conf.Metrics.OhlcPeriods[indexPeriod],
			indexPeriod: indexPeriod,
			dataSource:  conf.Metrics.Sources[exchange],
			destination: "ohlc_" + conf.Metrics.OhlcPeriodsStr[indexPeriod],
			exchange:    exchange,
		}

		go networking.RunEvery(conf.Metrics.Frequency, func(nextRun int64) {

			ind.nextRun = nextRun
			ind.computeTimeIntervals(0)

			imohlc := getOHLCFromTrades(ind)
			updateCacheLastOHLC(ind, imohlc)
			triggerDependencies(ind)
			prepareOHLCPoints(ind, imohlc)
		})
	}
}

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

	subimohlc := getCachedLastOHLC(from)
	imohlc := make(map[int64]map[string]*ohlc, len(ind.timeIntervals))

	for _, interval := range ind.timeIntervals {

		imohlc[interval] = make(map[string]*ohlc)

		for subi := interval; subi < interval+int64(ind.period) &&
			subi < ind.nextRun; subi += int64(from.period) {

			for market, subohlc := range subimohlc[subi] {

				if _, ok := imohlc[interval][market]; !ok {
					imohlc[interval][market] = &ohlc{}
				}
				ohlcVal := imohlc[interval][market]

				ohlcVal.volume += subohlc.volume
				ohlcVal.quantity += subohlc.quantity

				ohlcVal.weightedAverage = 0.0
				if ohlcVal.quantity != 0.0 {
					ohlcVal.weightedAverage = ohlcVal.volume / ohlcVal.quantity
				}

				ohlcVal.high = math.Max(ohlcVal.high, subohlc.high)
				ohlcVal.low = math.Min(ohlcVal.low, subohlc.low)
				ohlcVal.close = subohlc.close
				ohlcVal.change = ohlcVal.close - ohlcVal.open

				ohlcVal.changePercent = 0.0
				if ohlcVal.open != 0.0 {
					ohlcVal.changePercent = ohlcVal.change * 100 / ohlcVal.open
				}
			}
		}
	}

	updateCacheLastOHLC(ind, imohlc)
	triggerDependencies(ind)
	prepareOHLCPoints(ind, imohlc)
}

func triggerDependencies(ind *indicator) {
	go computeOHLC(ind)
	go getOBV(ind)
	go getMA(ind)
	go getRSI(ind)
}

func getOHLCFromTrades(ind *indicator) map[int64]map[string]*ohlc {

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
		ind.timeIntervals[0], ind.nextRun,
		ind.period)

	subQuery2 := fmt.Sprintf(
		`SELECT last
    FROM %s
    WHERE time >= now() - 1h
    GROUP BY market
    ORDER BY time DESC
    LIMIT 1`,
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

	imohlc := formatOHLC(ind, res)
	addLastIfNoVolume(ind, res, imohlc)

	return imohlc
}

func formatOHLC(ind *indicator,
	res []ifxClient.Result) map[int64]map[string]*ohlc {

	imohlc := make(map[int64]map[string]*ohlc, len(ind.timeIntervals))
	for _, interval := range ind.timeIntervals {
		imohlc[interval] = make(map[string]*ohlc, len(res[0].Series))
	}

	for _, serie := range res[0].Series {

		market := serie.Tags["market"]

		for _, ohlcRec := range serie.Values {

			if ohlcRec[0] == nil || ohlcRec[1] == nil || ohlcRec[2] == nil ||
				ohlcRec[3] == nil || ohlcRec[4] == nil || ohlcRec[5] == nil ||
				ohlcRec[6] == nil {
				continue
			}

			timestamp, err := networking.ConvertJsonValueToTime(ohlcRec[0])
			if err != nil {
				logger.WithFields(logrus.Fields{
					"error":    err,
					"exchange": ind.exchange,
					"market":   market,
				}).Error("formatOHLC: networking.ConvertJsonValueToTime")
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

			change := close - open

			changePercent := 0.0
			if open != 0.0 {
				changePercent = change * 100 / open
			}

			if mohlc, ok := imohlc[timestamp.UnixNano()]; !ok {

				logger.WithFields(logrus.Fields{
					"error":    "timestamp not matching",
					"exchange": ind.exchange,
					"market":   market,
				}).Error("formatOHLC")

			} else {

				mohlc[market] = &ohlc{
					volume:          volume,
					quantity:        quantity,
					weightedAverage: weightedAverage,
					open:            open,
					high:            high,
					low:             low,
					close:           close,
					change:          change,
					changePercent:   changePercent,
				}
			}
		}
	}

	return imohlc
}

func addLastIfNoVolume(ind *indicator, res []ifxClient.Result,
	imohlc map[int64]map[string]*ohlc) {

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

	for _, interval := range ind.timeIntervals {

		mohlc := imohlc[interval]

		for market, last := range lastTicks {

			if _, ok := mohlc[market]; !ok {
				mohlc[market] = &ohlc{
					0.0, 0.0, 0.0, last, last, last, last, 0.0, 0.0,
				}
			} else {
				lastTicks[market] = mohlc[market].close
			}
		}

		for _, serie := range res[0].Series {

			market := serie.Tags["market"]
			last := lastTicks[market]

			if _, ok := mohlc[market]; !ok {
				mohlc[market] = &ohlc{
					0.0, 0.0, 0.0, last, last, last, last, 0.0, 0.0,
				}
			} else {
				lastTicks[market] = mohlc[market].close
			}
		}
	}
}

func prepareOHLCPoints(ind *indicator, imohlc map[int64]map[string]*ohlc) {

	measurement := ind.destination
	points := make([]*ifxClient.Point, 0)

	for interval, mohlc := range imohlc {

		timestamp := time.Unix(0, interval)

		for market, ohlc := range mohlc {

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
				"change":           ohlc.change,
				"change_percent":   ohlc.changePercent,
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
		TypePoint: ind.exchange + "OHLC",
		Points:    points,
		Callback:  ind.callback,
	}
}
