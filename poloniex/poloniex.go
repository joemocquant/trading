package poloniex

import (
	"encoding/json"
	"io/ioutil"
	"time"
	"trading/api/poloniex/publicapi"
	"trading/api/poloniex/pushapi"
	"trading/ingestion"

	"github.com/Sirupsen/logrus"
	influxDBClient "github.com/influxdata/influxdb/client/v2"
)

var (
	conf           *configuration
	logger         *logrus.Entry
	dbClient       *influxDBClient.Client
	publicClient   *publicapi.PublicClient
	pushClient     *pushapi.PushClient
	marketUpdaters map[string]pushapi.MarketUpdater
	batchsToWrite  chan *ingestion.BatchPoints
)

type configuration struct {
	ingestionConf `json:"ingestion"`
}

type ingestionConf struct {
	poloniexConf `json:"poloniex"`
}

type poloniexConf struct {
	Schema                    map[string]string `json:"schema"`
	PublicTicksCheckPeriodSec int               `json:"public_ticks_check_period_sec"`
	MarketCheckPeriodMin      int               `json:"market_check_period_min"`
	OrderBooksCheckPeriodSec  int               `json:"order_books_check_period_sec"`
	FlushBatchsPeriodMs       int               `json:"flush_batchs_period_ms"`
	LogLevel                  string            `json:"log_level"`
}

func init() {

	customFormatter := new(logrus.TextFormatter)
	customFormatter.FullTimestamp = true
	logrus.SetFormatter(customFormatter)

	logger = logrus.WithField("context", "[ingestion:poloniex]")

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

	publicClient = publicapi.NewPublicClient()

	pushClient, err = pushapi.NewPushClient()
	if err != nil {
		logger.WithField("error", err).Fatal("pushapi.NewPushClient")
	}

	marketUpdaters = make(map[string]pushapi.MarketUpdater)
	batchsToWrite = make(chan *ingestion.BatchPoints, 2000)
}

func Ingest() {

	// flushing batchs periodically
	period := time.Duration(conf.FlushBatchsPeriodMs) * time.Millisecond
	go ingestion.FlushEvery(period, &ingestion.FlushInfo{
		batchsToWrite,
		conf.Schema["database"],
		dbClient,
	})

	//-- Ticks

	// Ingest ticks
	go ingestTicks()

	//-- OrderBooks

	// Init and checking order books periodically
	go ingestOrderBooks(100000, time.Duration(conf.OrderBooksCheckPeriodSec)*time.Second)

	//-- Market

	// Ingest and checking new markets periodically
	go func() {
		for {
			ingestNewMarkets()
			<-time.After(time.Duration(conf.MarketCheckPeriodMin) * time.Minute)
		}
	}()
}
