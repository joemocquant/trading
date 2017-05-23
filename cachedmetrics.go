package metrics

import (
	"sync"
	"time"
)

type dataSourceCachedMetrics map[string]periodsCachedMetrics

type periodsCachedMetrics map[time.Duration]*marketsCachedMetrics

type marketsCachedMetrics struct {
	sync.RWMutex
	imohlc map[int64]map[string]*ohlc
	imobv  map[int64]map[string]float64
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
