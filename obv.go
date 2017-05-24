package metrics

import (
	"time"
	"trading/networking/database"

	ifxClient "github.com/influxdata/influxdb/client/v2"
)

func getOBV(from *indicator) {

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

	imobv := computeOBV(ind)
	prepareOBVPoints(ind, imobv)
}

func computeOBV(ind *indicator) map[int64]map[string]float64 {

	imohlc := getCachedLastOHLC(ind)
	if imohlc == nil {
		return nil
	}

	imobv := make(map[int64]map[string]float64, len(ind.timeIntervals))

	for _, interval := range ind.timeIntervals {

		imobv[interval] = make(map[string]float64, len(imohlc[interval]))

		for market, ohlc := range imohlc[interval] {

			if prevOhlc, ok := imohlc[interval-int64(ind.period)][market]; ok {

				obvValue := 0.0

				if ohlc.volume > prevOhlc.volume {
					obvValue = prevOhlc.volume + ohlc.volume

				} else if ohlc.volume == prevOhlc.volume {
					obvValue = ohlc.volume

				} else {
					obvValue = prevOhlc.volume - ohlc.volume
				}

				imobv[interval][market] = obvValue
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
		TypePoint: ind.exchange + "OBV",
		Points:    points,
		Callback:  ind.callback,
	}
}
