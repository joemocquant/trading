package metrics

import (
	"fmt"
	"sync"
	"time"
	"trading/networking"
	"trading/networking/database"

	"github.com/Sirupsen/logrus"
	ifxClient "github.com/influxdata/influxdb/client/v2"
)

type dataSourceCachedMetrics map[string]periodsCachedMetrics

type periodsCachedMetrics map[time.Duration]*marketsCachedMetrics

type marketsCachedMetrics struct {
	sync.RWMutex
	imohlc     map[int64]map[string]*ohlc
	imobv      map[int64]map[string]float64
	lastImohlc map[int64]map[string]*ohlc
}

func initCachedMetrics() {

	cm = make(dataSourceCachedMetrics, len(conf.Metrics.Sources))

	lenPeriods := len(conf.Metrics.OhlcPeriods)

	for _, dataSource := range conf.Metrics.Sources {

		exchange := dataSource.Schema["database"]
		cm[exchange] = make(periodsCachedMetrics, lenPeriods)

		for _, period := range conf.Metrics.OhlcPeriods {
			cm[exchange][period] = &marketsCachedMetrics{}
		}
	}
}

func setCachedOHLC(ind *indicator, imohlc map[int64]map[string]*ohlc) {

	cm[ind.exchange][ind.period].Lock()
	defer cm[ind.exchange][ind.period].Unlock()

	cm[ind.exchange][ind.period].imohlc = imohlc
}

func getCachedOHLC(ind *indicator) map[int64]map[string]*ohlc {

	cm[ind.exchange][ind.period].RLock()
	defer cm[ind.exchange][ind.period].RUnlock()

	return cm[ind.exchange][ind.period].imohlc
}

func setCachedOBV(ind *indicator, imobv map[int64]map[string]float64) {

	cm[ind.exchange][ind.period].Lock()
	defer cm[ind.exchange][ind.period].Unlock()

	cm[ind.exchange][ind.period].imobv = imobv
}

func getCachedOBV(ind *indicator) map[int64]map[string]float64 {

	cm[ind.exchange][ind.period].RLock()
	defer cm[ind.exchange][ind.period].RUnlock()

	return cm[ind.exchange][ind.period].imobv
}

func cacheLastOHLC(ind *indicator, length int) {

	ind.computeTimeIntervals(length - 1)
	imohlc := getLastOHLC(ind)

	cm[ind.exchange][ind.period].Lock()
	defer cm[ind.exchange][ind.period].Unlock()

	cm[ind.exchange][ind.period].lastImohlc = imohlc
}

func getCachedLastOHLC(ind *indicator) map[int64]map[string]*ohlc {

	cm[ind.exchange][ind.period].RLock()
	defer cm[ind.exchange][ind.period].RUnlock()

	return cm[ind.exchange][ind.period].lastImohlc
}

func getLastOHLC(ind *indicator) map[int64]map[string]*ohlc {

	query := fmt.Sprintf(
		`SELECT close, volume, change
    FROM %s
    WHERE time >= %d AND time < %d AND exchange = '%s'
    GROUP BY market`,
		ind.destination,
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

	imohlc := formatLastOHLC(ind, res)
	return imohlc
}

func formatLastOHLC(ind *indicator,
	res []ifxClient.Result) map[int64]map[string]*ohlc {

	imohlc := make(map[int64]map[string]*ohlc, len(ind.timeIntervals))
	for _, interval := range ind.timeIntervals {
		imohlc[interval] = make(map[string]*ohlc, len(res[0].Series))
	}

	for _, serie := range res[0].Series {

		market := serie.Tags["market"]

		for _, rec := range serie.Values {

			if rec[0] == nil || rec[1] == nil || rec[2] == nil || rec[3] == nil {
				continue
			}

			timestamp, err := networking.ConvertJsonValueToTime(rec[0])
			if err != nil {
				logger.WithFields(logrus.Fields{
					"error":    err,
					"exchange": ind.exchange,
					"market":   market,
				}).Error("formatLastOHLC: networking.ConvertJsonValueToTime")
				continue
			}

			close, err := networking.ConvertJsonValueToFloat64(rec[1])
			if err != nil {
				logger.WithFields(logrus.Fields{
					"error":    err,
					"exchange": ind.exchange,
					"market":   market,
				}).Error("formatLastOHLC: networking.ConvertJsonValueToFloat64")
				continue
			}

			volume, err := networking.ConvertJsonValueToFloat64(rec[2])
			if err != nil {
				logger.WithFields(logrus.Fields{
					"error":    err,
					"exchange": ind.exchange,
					"market":   market,
				}).Error("formatLastOHLC: networking.ConvertJsonValueToFloat64")
				continue
			}

			change, err := networking.ConvertJsonValueToFloat64(rec[2])
			if err != nil {
				logger.WithFields(logrus.Fields{
					"error":    err,
					"exchange": ind.exchange,
					"market":   market,
				}).Error("formatLastOHLC: networking.ConvertJsonValueToFloat64")
				continue
			}

			if mohlc, ok := imohlc[timestamp.UnixNano()]; !ok {

				logger.WithFields(logrus.Fields{
					"error":    "timestamp not matching",
					"exchange": ind.exchange,
					"market":   market,
				}).Error("formatLastOHLC")

			} else {
				mohlc[market] = &ohlc{
					close:  close,
					volume: volume,
					change: change,
				}
			}
		}
	}

	return imohlc
}
