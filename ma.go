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

	offset := conf.Metrics.LengthMax - 1
	if offset < 1 {
		logger.WithField("error", "conf: ma_max must be greater than 1").Fatal(
			"ComputeMA")
	}

	ind.computeTimeIntervals(offset)

	imma := getMA(ind)
	prepareMAPoints(ind, imma)
}

func getMA(ind *indicator) map[int64]map[string]*ma {

	fields := make([]string, conf.Metrics.LengthMax)
	for i := 0; i < conf.Metrics.LengthMax; i++ {
		fields[i] = "ema_" + strconv.Itoa(i+1)
	}
	selectedFields := strings.Join(fields, ", ")

	query := fmt.Sprintf(
		`SELECT %s
    FROM %s
    WHERE time = %d AND exchange = '%s'
    GROUP BY market;`,
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

	return formatMA(ind, res)
}

func formatMA(ind *indicator,
	res []ifxClient.Result) map[int64]map[string]*ma {

	imohlc := getCachedLastOHLC(ind)
	if imohlc == nil {
		return nil
	}

	imma := make(map[int64]map[string]*ma, len(ind.timeIntervals))

	memas := getPreviousEMA(ind, res)

	for i, interval := range ind.timeIntervals[conf.Metrics.LengthMax-1:] {

		imma[interval] = make(map[string]*ma, len(imohlc[interval]))

		for market, ohlc := range imohlc[interval] {

			if _, ok := memas[market]; !ok {
				memas[market] = make(map[int]float64, conf.Metrics.LengthMax)
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

				var emaSeed float64
				if ema, ok := memas[market][maLength]; ok {
					emaSeed = ema
				} else if ohlc, ok := imohlc[interval-int64(ind.period)][market]; ok {
					emaSeed = ohlc.close
				} else {
					continue
				}

				multiplier := 2.0 / float64(maLength+1)
				ema := (ohlc.close-emaSeed)*multiplier + emaSeed
				imma[interval][market].emas[maLength] = ema
				memas[market][maLength] = ema
			}
		}
	}

	return imma
}

func getPreviousEMA(ind *indicator,
	res []ifxClient.Result) map[string]map[int]float64 {

	memas := make(map[string]map[int]float64, len(res[0].Series))

	for _, serie := range res[0].Series {

		market := serie.Tags["market"]

		for _, emasRec := range serie.Values {

			memas[market] = make(map[int]float64, conf.Metrics.LengthMax)

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
