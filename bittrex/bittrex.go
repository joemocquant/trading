package bittrex

import (
	"encoding/json"
	"io/ioutil"
	"sync"
	"time"
	"trading/networking/database"

	ifxClient "github.com/influxdata/influxdb/client/v2"
	publicapi "github.com/joemocquant/bittrex-api/publicapi"
	"github.com/sirupsen/logrus"
	prefixed "github.com/x-cray/logrus-prefixed-formatter"
)

var (
	conf          *configuration
	logger        *logrus.Entry
	dbClient      ifxClient.Client
	publicClient  *publicapi.Client
	ams           *allMarkets
	lts           *lastTrades
	batchsToWrite chan *database.BatchPoints
)

type configuration struct {
	*ingestionConf `json:"ingestion"`
}

type ingestionConf struct {
	LogLevel     string `json:"log_level"`
	*bittrexConf `json:"bittrex"`
}

type bittrexConf struct {
	Schema                        map[string]string `json:"schema"`
	MarketSummariesCheckPeriodSec int               `json:"market_summaries_check_period_sec"`
	MarketsCheckPeriodMin         int               `json:"markets_check_period_min"`
	MarketHistoriesCheckPeriodSec int               `json:"market_histories_check_period_sec"`
	OrderBooksCheckPeriodSec      int               `json:"order_books_check_period_sec"`
	FlushBatchsPeriodSec          int               `json:"flush_batchs_period_sec"`
	FlushCapacity                 int               `json:"flush_capacity"`
}

type allMarkets struct {
	sync.Mutex
	markets map[string]*publicapi.Market
}

type lastTrades struct {
	sync.Mutex
	lastTrades map[string]*publicapi.Trade
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

	if dbClient, err = database.NewdbClient(); err != nil {
		logger.WithField("error", err).Fatal("database.NewdbClient")
	}

	publicClient = publicapi.NewClient()

	ams = &allMarkets{sync.Mutex{}, make(map[string]*publicapi.Market)}
	lts = &lastTrades{sync.Mutex{}, make(map[string]*publicapi.Trade)}

	batchsToWrite = make(chan *database.BatchPoints, conf.FlushCapacity)
}

func Ingest() {

	// flushing batchs periodically
	period := time.Duration(conf.FlushBatchsPeriodSec) * time.Second
	go database.FlushEvery(period, &database.FlushInfo{
		batchsToWrite,
		conf.Schema["database"],
		dbClient,
	})

	// check new markets periodically
	go checkMarkets()

	// ingest market summaries periodically
	go ingestMarketSummaries()

	// checking market histories periodically
	go ingestMarketHistories()

	// checking order books periodically
	go ingestOrderBooks()
}
