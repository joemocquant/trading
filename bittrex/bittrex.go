package bittrex

import (
	"encoding/json"
	"io/ioutil"
	"sync"
	"time"
	"trading/api/bittrex/publicapi"
	"trading/ingestion"

	"github.com/Sirupsen/logrus"
	influxDBClient "github.com/influxdata/influxdb/client/v2"
	prefixed "github.com/x-cray/logrus-prefixed-formatter"
)

var (
	conf          *configuration
	logger        *logrus.Entry
	dbClient      *influxDBClient.Client
	publicClient  *publicapi.Client
	am            *allMarkets
	batchsToWrite chan *ingestion.BatchPoints
)

type configuration struct {
	ingestionConf `json:"ingestion"`
}

type ingestionConf struct {
	bittrexConf `json:"bittrex"`
}

type bittrexConf struct {
	Schema                        map[string]string `json:"schema"`
	MarketSummariesCheckPeriodSec int               `json:"market_summaries_check_period_sec"`
	MarketsCheckPeriodMin         int               `json:"markets_check_period_min"`
	OrderBooksCheckPeriodSec      int               `json:"order_books_check_period_sec"`
	FlushBatchsPeriodSec          int               `json:"flush_batchs_period_sec"`
	FlushCapacity                 int               `json:"flush_capacity"`
	LogLevel                      string            `json:"log_level"`
}

type allMarkets struct {
	sync.Mutex
	markets map[string]*publicapi.Market
}

func init() {

	customFormatter := new(prefixed.TextFormatter)
	customFormatter.FullTimestamp = true
	customFormatter.ForceColors = true
	customFormatter.ForceFormatting = true
	logrus.SetFormatter(customFormatter)

	logger = logrus.WithField("prefix", "[ingestion:bittrex]")

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

	if dbClient, err = ingestion.NewdbClient(); err != nil {
		logger.WithField("error", err).Fatal("ingestion.NewdbClient")
	}

	publicClient = publicapi.NewClient()
	am = &allMarkets{sync.Mutex{}, make(map[string]*publicapi.Market)}
	batchsToWrite = make(chan *ingestion.BatchPoints, conf.FlushCapacity)
}

func Ingest() {

	// flushing batchs periodically
	period := time.Duration(conf.FlushBatchsPeriodSec) * time.Second
	go ingestion.FlushEvery(period, &ingestion.FlushInfo{
		batchsToWrite,
		conf.Schema["database"],
		dbClient,
	})

	// check new markets periodically
	go checkMarkets()

	// ingest market summaries periodically
	go ingestMarketSummaries()

	// checking order books periodically
	go ingestOrderBooks()
}
