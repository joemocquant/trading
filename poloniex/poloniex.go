package poloniex

import (
	"encoding/json"
	"io/ioutil"
	"sync"
	"time"
	"trading/api/poloniex/publicapi"
	"trading/api/poloniex/pushapi"
	"trading/networking/database"

	"github.com/Sirupsen/logrus"
	ifxClient "github.com/influxdata/influxdb/client/v2"
	prefixed "github.com/x-cray/logrus-prefixed-formatter"
)

var (
	conf          *configuration
	logger        *logrus.Entry
	dbClient      ifxClient.Client
	publicClient  *publicapi.Client
	pushClient    *pushapi.Client
	updaters      *marketUpdaters
	batchsToWrite chan *database.BatchPoints
)

type configuration struct {
	ingestionConf `json:"ingestion"`
}

type ingestionConf struct {
	LogLevel     string `json:"log_level"`
	poloniexConf `json:"poloniex"`
}

type poloniexConf struct {
	Schema                      map[string]string `json:"schema"`
	PublicTicksCheckPeriodSec   int               `json:"public_ticks_check_period_sec"`
	MarketCheckPeriodMin        int               `json:"market_check_period_min"`
	MissingTradesCheckPeriodSec int               `json:"missing_trades_check_period_sec"`
	OrderBooksCheckPeriodSec    int               `json:"order_books_check_period_sec"`
	FlushBatchsPeriodMs         int               `json:"flush_batchs_period_ms"`
	FlushCapacity               int               `json:"flush_capacity"`
}

type marketUpdaters struct {
	sync.RWMutex
	mus map[string]pushapi.MarketUpdater
}

func init() {

	customFormatter := new(prefixed.TextFormatter)
	customFormatter.FullTimestamp = true
	customFormatter.ForceColors = true
	customFormatter.ForceFormatting = true
	logrus.SetFormatter(customFormatter)

	logger = logrus.WithField("prefix", "[ingestion:poloniex]")

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

	publicClient = publicapi.NewClient()

	pushClient, err = pushapi.NewClient()
	if err != nil {
		logger.WithField("error", err).Fatal("pushapi.NewPushClient")
	}

	updaters = &marketUpdaters{
		sync.RWMutex{},
		make(map[string]pushapi.MarketUpdater),
	}

	batchsToWrite = make(chan *database.BatchPoints, conf.FlushCapacity)
}

func Ingest() {

	// flushing batchs periodically
	period := time.Duration(conf.FlushBatchsPeriodMs) * time.Millisecond
	go database.FlushEvery(period, &database.FlushInfo{
		batchsToWrite,
		conf.Schema["database"],
		dbClient,
	})

	//-- Ticks (publicapi and pushapi)

	// Ingest ticks
	go ingestTicks()

	//-- Markets

	// Ingest markets (pushapi)
	go ingestMarkets()

	// Ingest missing trades (publicapi)
	go ingestMissingTrades()

	//-- OrderBooks (publicapi)

	// Init and checking order books periodically
	go ingestOrderBooks()
}
