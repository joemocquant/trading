package metrics

import (
	"fmt"
	"time"
	"trading/networking/database"

	ifxClient "github.com/influxdata/influxdb/client/v2"
)

type rsi struct {
	rsis map[int]float64
}

func getRSI(from *indicator) {

	ind := &indicator{
		nextRun:     from.nextRun,
		period:      conf.Metrics.OhlcPeriods[from.indexPeriod],
		indexPeriod: from.indexPeriod,
		dataSource:  from.dataSource,
		source:      from.destination,
		destination: "rsi_" + conf.Metrics.OhlcPeriodsStr[from.indexPeriod],
		exchange:    from.exchange,
	}

	ind.computeTimeIntervals(conf.Metrics.LengthMax - 1)

	imobv := computeRSI(ind)
	prepareRSIPoints(ind, imobv)
}

func computeRSI(ind *indicator) map[int64]map[string]*rsi {

	imohlc := getCachedLastOHLC(ind)
	if imohlc == nil {
		return nil
	}

	imrsi := make(map[int64]map[string]*rsi, len(ind.timeIntervals))

	for i, interval := range ind.timeIntervals[conf.Metrics.LengthMax-1:] {

		imrsi[interval] = make(map[string]*rsi, len(imohlc[interval]))

		for market, _ := range imohlc[interval] {

			imrsi[interval][market] = &rsi{
				rsis: make(map[int]float64, conf.Metrics.LengthMax),
			}

			avgUp, avgDown := 0.0, 0.0

			for maLength := 1; maLength <= conf.Metrics.LengthMax; maLength++ {

				intervalOffset := ind.timeIntervals[i+conf.Metrics.LengthMax-maLength]

				if ohlc, ok := imohlc[intervalOffset][market]; ok {

					if ohlc.change > 0.0 {
						avgUp += ohlc.change
					} else if ohlc.change < 0.0 {
						avgDown -= ohlc.change
					}

					rsi := 100.0
					if avgDown != 0.0 {

						rsi = 100.0 - 100.0/(1+avgUp/avgDown)
					}

					imrsi[interval][market].rsis[maLength] = rsi

				} else {
					break
				}
			}
		}
	}

	return imrsi
}

func prepareRSIPoints(ind *indicator, imrsi map[int64]map[string]*rsi) {

	measurement := ind.destination
	points := make([]*ifxClient.Point, 0)

	for interval, mrsi := range imrsi {

		timestamp := time.Unix(0, interval)

		for market, rsi := range mrsi {

			tags := map[string]string{
				"market":   market,
				"exchange": ind.exchange,
			}

			fields := make(map[string]interface{}, len(rsi.rsis))

			for i, rsi := range rsi.rsis {

				rsiLength := fmt.Sprintf("rsi_%d", i)
				fields[rsiLength] = rsi
			}

			pt, err := ifxClient.NewPoint(measurement, tags, fields, timestamp)
			if err != nil {
				logger.WithField("error", err).Error(
					"prepareRSIPoints: ifxClient.NewPoint")
			}
			points = append(points, pt)
		}
	}

	if len(points) == 0 {
		return
	}

	batchsToWrite <- &database.BatchPoints{
		TypePoint: ind.exchange + "RSI",
		Points:    points,
		Callback:  ind.callback,
	}
}
