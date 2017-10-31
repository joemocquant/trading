package metrics

import (
	"fmt"
	"strconv"
	"strings"
	"time"
	"trading/networking"
	"trading/networking/database"

	ifxClient "github.com/influxdata/influxdb/client/v2"
	"github.com/sirupsen/logrus"
)

type ma struct {
	smas map[int]float64
	emas map[int]float64
}

func getMA(from *indicator) {

	ind := &indicator{
		nextRun:     from.nextRun,
		period:      conf.Metrics.OhlcPeriods[from.indexPeriod],
		indexPeriod: from.indexPeriod,
		dataSource:  from.dataSource,
		source:      from.destination,
		destination: "ma_" + conf.Metrics.OhlcPeriodsStr[from.indexPeriod],
		exchange:    from.exchange,
	}

	ind.computeTimeIntervals(conf.Metrics.LengthMax - 1)

	imma := computeMA(ind)
	setCacheLastMA(ind, imma)
	prepareMAPoints(ind, imma)
}

func computeMA(ind *indicator) map[int64]map[string]*ma {

	imohlc := getCachedLastOHLC(ind)
	if imohlc == nil {
		return nil
	}

	lastImma := getLastImma(ind)
	imma := make(map[int64]map[string]*ma, len(ind.timeIntervals))

	for i, interval := range ind.timeIntervals[conf.Metrics.LengthMax-1:] {

		if _, ok := lastImma[interval]; !ok {
			lastImma[interval] = make(map[string]*ma, len(imohlc[interval]))
		}

		imma[interval] = make(map[string]*ma, len(imohlc[interval]))

		for market, ohlc := range imohlc[interval] {

			if _, ok := lastImma[interval][market]; !ok {
				lastImma[interval][market] = &ma{
					emas: make(map[int]float64, conf.Metrics.LengthMax),
				}
			}

			imma[interval][market] = &ma{
				smas: make(map[int]float64, conf.Metrics.LengthMax),
				emas: make(map[int]float64, conf.Metrics.LengthMax),
			}
			cumulativeSum := 0.0

			for maLength := 1; maLength <= conf.Metrics.LengthMax; maLength++ {

				intervalOffset := ind.timeIntervals[i+conf.Metrics.LengthMax-maLength]

				if ohlc, ok := imohlc[intervalOffset][market]; ok {

					cumulativeSum += ohlc.close
					sma := cumulativeSum / float64(maLength)
					imma[interval][market].smas[maLength] = sma

				} else {
					break
				}
			}

			for maLength := 1; maLength <= conf.Metrics.LengthMax; maLength++ {

				emaSeed, emaFound := 0.0, false
				if ma, ok := lastImma[interval-int64(ind.period)][market]; ok {
					if emaSeed, ok = ma.emas[maLength]; ok {
						emaFound = true
					}
				}

				if !emaFound {
					if ohlc, ok := imohlc[interval-int64(ind.period)][market]; ok {
						emaSeed = ohlc.close
					} else {
						continue
					}
				}

				multiplier := 2.0 / float64(maLength+1)
				ema := multiplier*ohlc.close + (1-multiplier)*emaSeed
				imma[interval][market].emas[maLength] = ema
				lastImma[interval][market].emas[maLength] = ema
			}
		}
	}

	return imma
}

func getLastImma(ind *indicator) map[int64]map[string]*ma {

	lastImma := getCachedLastMA(ind)
	if lastImma != nil {
		return lastImma
	}

	fields := make([]string, conf.Metrics.LengthMax)
	for i := 0; i < conf.Metrics.LengthMax; i++ {
		fields[i] = "ema_" + strconv.Itoa(i+1)
	}
	selectedFields := strings.Join(fields, ", ")

	query := fmt.Sprintf(
		`SELECT %s
    FROM %s
    WHERE time = %d AND exchange = '%s'
    GROUP BY market`,
		selectedFields,
		ind.destination,
		ind.timeIntervals[conf.Metrics.LengthMax-2],
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

	imma := make(map[int64]map[string]*ma, 1)

	for _, serie := range res[0].Series {

		market := serie.Tags["market"]
		emasRec := serie.Values[0]

		if emasRec[0] == nil {
			continue
		}

		timestamp, err := networking.ConvertJsonValueToTime(emasRec[0])
		if err != nil {
			logger.WithFields(logrus.Fields{
				"error":    err,
				"exchange": ind.exchange,
				"market":   market,
			}).Error("formatOHLC: networking.ConvertJsonValueToTime")
			continue
		}

		if _, ok := imma[timestamp.UnixNano()]; !ok {
			imma[timestamp.UnixNano()] = make(map[string]*ma, len(res[0].Series))
		}

		imma[timestamp.UnixNano()][market] = &ma{
			emas: make(map[int]float64, conf.Metrics.LengthMax),
		}

		for i := 1; i <= conf.Metrics.LengthMax; i++ {
			if emasRec[i] == nil {
				continue
			}

			ema, err := networking.ConvertJsonValueToFloat64(emasRec[1])
			if err != nil {
				logger.WithFields(logrus.Fields{
					"error":    err,
					"exchange": ind.exchange,
					"market":   market,
				}).Error("getPreviousEMA: networking.ConvertJsonValueToFloat64")
				continue
			}

			imma[timestamp.UnixNano()][market].emas[i] = ema
		}
	}

	return imma
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
		TypePoint: ind.exchange + "MA",
		Points:    points,
		Callback:  ind.callback,
	}
}
