package metrics

import (
	"fmt"
	"sync"
	"time"
	"trading/networking"
	"trading/networking/database"

	ifxClient "github.com/influxdata/influxdb/client/v2"
)

type dataSourceCachedMetrics map[string]periodsCachedMetrics

type periodsCachedMetrics map[time.Duration]*marketsCachedMetrics

type marketsCachedMetrics struct {
	sync.RWMutex
	lastImohlc map[int64]map[string]*ohlc
	lastImma   map[int64]map[string]*ma
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

func resetCachedMetrics() {

	for _, dataSource := range conf.Metrics.Sources {

		exchange := dataSource.Schema["database"]

		for _, period := range conf.Metrics.OhlcPeriods {
			cm[exchange][period].Lock()
			cm[exchange][period] = &marketsCachedMetrics{}
			cm[exchange][period].Unlock()
		}
	}
}

func resetCachedLastOHLC(ind *indicator) {

	for _, period := range conf.Metrics.OhlcPeriods {
		cm[ind.exchange][period].Lock()
		cm[ind.exchange][period].lastImohlc = nil
		cm[ind.exchange][period].Unlock()
	}
}

func getCachedLastMA(ind *indicator) map[int64]map[string]*ma {

	cm[ind.exchange][ind.period].RLock()
	defer cm[ind.exchange][ind.period].RUnlock()

	return cm[ind.exchange][ind.period].lastImma
}

func setCacheLastMA(ind *indicator, imma map[int64]map[string]*ma) {

	cm[ind.exchange][ind.period].Lock()
	defer cm[ind.exchange][ind.period].Unlock()

	cm[ind.exchange][ind.period].lastImma = imma
}

func getCachedLastOHLC(ind *indicator) map[int64]map[string]*ohlc {

	cm[ind.exchange][ind.period].RLock()
	defer cm[ind.exchange][ind.period].RUnlock()

	return cm[ind.exchange][ind.period].lastImohlc
}

func updateCacheLastOHLC(ind *indicator, imohlc map[int64]map[string]*ohlc) {

	cachedImohlc := getCachedLastOHLC(ind)
	ind.computeTimeIntervals(conf.Metrics.LengthMax - 1)

	if len(cachedImohlc) > len(ind.timeIntervals) {
		cachedImohlc = nil
	}

	if cachedImohlc == nil {
		cacheLastOHLC(ind)
		cachedImohlc = getCachedLastOHLC(ind)
	}

	if cachedImohlc == nil {
		return
	}

	cm[ind.exchange][ind.period].Lock()
	defer cm[ind.exchange][ind.period].Unlock()

	for interval, mohlc := range imohlc {
		cachedImohlc[interval] = mohlc
	}

	interval := ind.timeIntervals[0]
	for len(cachedImohlc) > len(ind.timeIntervals) {
		interval -= int64(ind.period)
		delete(cachedImohlc, interval)
	}
}

func cacheLastOHLC(ind *indicator) {

	imohlc := getLastOHLC(ind)

	cm[ind.exchange][ind.period].Lock()
	defer cm[ind.exchange][ind.period].Unlock()

	cm[ind.exchange][ind.period].lastImohlc = imohlc
}

func getLastOHLC(ind *indicator) map[int64]map[string]*ohlc {

	query := fmt.Sprintf(
		`SELECT volume, quantity, open, high, low, close
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

	imohlc := formatOHLC(ind, res)
	return imohlc
}
