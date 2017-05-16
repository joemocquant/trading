package metrics

import (
	"encoding/json"
	"io/ioutil"
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
	metricsConf `json:"metrics"`
}

type metricsConf struct {
	LogLevel            string            `json:"log_level"`
	Schema              map[string]string `json:"schema"`
	FlushBatchsPeriodMs int               `json:"flush_batchs_period_ms"`
	FlushCapacity       int               `json:"flush_capacity"`
	MarketDepths        *marketDepthsConf `json:"market_depths"`
	Ohlc                *ohlcConf         `json:"ohlc"`
	Sma                 *smaConf          `json:"sma"`
	Sources             *sources          `json:"sources"`
}

type marketDepthsConf struct {
	Intervals                  []float64 `json:"intervals"`
	Frequency                  string    `json:"frequency"`
	PoloniexHardFetchFrequency int       `json:"poloniex_hard_fetch_frequency"`
}

type ohlcConf struct {
	Periods   []string `json:"periods"`
	Frequency string   `json:"frequency"`
}

type smaConf struct {
	Periods   []string `json:"periods"`
	Frequency string   `json:"frequency"`
}

type sources struct {
	Poloniex *exchangeConf `json:"poloniex"`
	Bittrex  *exchangeConf `json:"bittrex"`
}

type exchangeConf struct {
	Schema map[string]string `json:"schema"`
}

type indicators map[string]*indicator

type indicator struct {
	period      time.Duration
	indexPeriod int
	dataSource  *exchangeConf
	source      string
	destination string
	nextRun     int64
	callback    func()
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

	switch conf.LogLevel {
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

	batchsToWrite = make(chan *database.BatchPoints, conf.FlushCapacity)
}

func ComputeMetrics() {

	// flushing batchs periodically
	period := time.Duration(conf.FlushBatchsPeriodMs) * time.Millisecond
	go database.FlushEvery(period, &database.FlushInfo{
		batchsToWrite,
		conf.Schema["database"],
		dbClient,
	})

	go computeMarketDepths()
	go computeBaseOHLC()
}
