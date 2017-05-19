package metrics

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"math"
	"time"
	"trading/networking/database"

	"github.com/Sirupsen/logrus"
	ifxClient "github.com/influxdata/influxdb/client/v2"
	prefixed "github.com/x-cray/logrus-prefixed-formatter"
)

var (
	conf          *configuration
	logger        *logrus.Entry
	dbClient      ifxClient.Client
	batchsToWrite chan *database.BatchPoints
)

type configuration struct {
	Metrics *metricsConf `json:"metrics"`
}

type metricsConf struct {
	LogLevel            string            `json:"log_level"`
	Schema              map[string]string `json:"schema"`
	FlushBatchsPeriodMs int               `json:"flush_batchs_period_ms"`
	FlushCapacity       int               `json:"flush_capacity"`
	Frequency           time.Duration
	PeriodsStr          []string `json:"periods"`
	Periods             []time.Duration
	MarketDepths        *marketDepthsConf `json:"market_depths"`
	Sources             *sources          `json:"sources"`
}

type marketDepthsConf struct {
	Intervals                  []float64 `json:"intervals"`
	Frequency                  string    `json:"frequency"`
	PoloniexHardFetchFrequency int       `json:"poloniex_hard_fetch_frequency"`
}

type sources struct {
	Poloniex *exchangeConf `json:"poloniex"`
	Bittrex  *exchangeConf `json:"bittrex"`
}

type exchangeConf struct {
	Schema    map[string]string `json:"schema"`
	UpdateLag time.Duration
}

type indicators map[string]*indicator

type indicator struct {
	nextRun       int64
	period        time.Duration
	indexPeriod   int
	dataSource    *exchangeConf
	source        string
	destination   string
	callback      func()
	timeIntervals []time.Time
	exchange      string
}

func init() {

	customFormatter := new(prefixed.TextFormatter)
	customFormatter.FullTimestamp = true
	customFormatter.ForceColors = true
	customFormatter.ForceFormatting = true
	logrus.SetFormatter(customFormatter)

	logger = logrus.WithField("prefix", "[metrics]")

	content, err := ioutil.ReadFile("conf.json")

	if err != nil {
		logger.WithField("error", err).Fatal("loading configuration")
	}

	if err := json.Unmarshal(content, &conf); err != nil {
		logger.WithField("error", err).Fatal("loading configuration")
	}

	switch conf.Metrics.LogLevel {
	case "debug":
		logrus.SetLevel(logrus.DebugLevel)
	case "info":
		logrus.SetLevel(logrus.InfoLevel)
	case "warn":
		logrus.SetLevel(logrus.WarnLevel)
	case "error":
		logrus.SetLevel(logrus.ErrorLevel)
	case "fatal":
		logrus.SetLevel(logrus.FatalLevel)
	case "panic":
		logrus.SetLevel(logrus.PanicLevel)
	default:
		logrus.SetLevel(logrus.WarnLevel)
	}

	if dbClient, err = database.NewdbClient(); err != nil {
		logger.WithField("error", err).Fatal("database.NewdbClient")
	}

	batchsToWrite = make(chan *database.BatchPoints, conf.Metrics.FlushCapacity)
}

func ComputeMetrics() {

	// flushing batchs periodically
	period := time.Duration(conf.Metrics.FlushBatchsPeriodMs) * time.Millisecond

	go database.FlushEvery(period, &database.FlushInfo{
		batchsToWrite,
		conf.Metrics.Schema["database"],
		dbClient,
	})

	go computeMarketDepths()
	go computeBaseOHLC()
}

func (e *exchangeConf) UnmarshalJSON(data []byte) error {

	type alias exchangeConf
	aux := struct {
		UpdateLag string `json:"update_lag"`
		*alias
	}{
		alias: (*alias)(e),
	}

	if err := json.Unmarshal(data, &aux); err != nil {
		return fmt.Errorf("json.Unmarshal: %v", err)
	}

	var err error

	if e.UpdateLag, err = time.ParseDuration(aux.UpdateLag); err != nil {
		return fmt.Errorf("time.ParseDuration: %v", err)
	}

	return nil
}

func (m *metricsConf) UnmarshalJSON(data []byte) error {

	type alias metricsConf
	aux := struct {
		Frequency string `json:"frequency"`
		*alias
	}{
		alias: (*alias)(m),
	}

	if err := json.Unmarshal(data, &aux); err != nil {
		return fmt.Errorf("json.Unmarshal: %v", err)
	}

	var err error

	if m.Frequency, err = time.ParseDuration(aux.Frequency); err != nil {
		return fmt.Errorf("time.ParseDuration: %v", err)
	}

	m.Periods = make([]time.Duration, len(m.PeriodsStr))
	for i, p := range m.PeriodsStr {

		period, err := time.ParseDuration(p)
		if err != nil {
			return fmt.Errorf("time.ParseDuration: %v", err)
		}
		m.Periods[i] = period
	}

	return nil
}

func (ind *indicator) computeTimeIntervals() {

	var periodCount int64 = 0
	delta := ind.nextRun % int64(ind.period)
	lag := int64(ind.dataSource.UpdateLag)

	if delta >= lag {
		periodCount = 0

	} else {
		periodCount = int64(math.Ceil(float64(lag-delta) / float64(ind.period)))
	}

	start := int64(ind.nextRun) - delta - periodCount*int64(ind.period)
	end := ind.nextRun

	var timeIntervals []time.Time
	for s := start; s <= end; s += int64(ind.period) {
		timeIntervals = append(timeIntervals, time.Unix(0, s))
	}

	ind.timeIntervals = timeIntervals
}

func applyMetrics(from, to time.Time, dataSources []*exchangeConf,
	apply func(ind *indicator)) {

	period := conf.Metrics.Periods[0]
	fromDuration := from.UnixNano()
	firstRun := fromDuration - (fromDuration % int64(period)) + int64(period)
	sleep := 200 * time.Millisecond

	for start := firstRun; start < to.UnixNano(); start += int64(period) {

		for _, dataSource := range dataSources {

			indFrom := &indicator{
				nextRun:     start,
				period:      period,
				indexPeriod: 0,
				dataSource:  dataSource,
				destination: "ohlc_" + conf.Metrics.PeriodsStr[0],
				exchange:    dataSource.Schema["database"],
			}

			apply(indFrom)
			time.Sleep(sleep)
		}

	}
}
