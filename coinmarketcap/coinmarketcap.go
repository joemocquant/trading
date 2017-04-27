package coinmarketcap

import (
	"encoding/json"
	"io/ioutil"
	"trading/api/coinmarketcap"
	"trading/ingestion"

	"github.com/Sirupsen/logrus"
	influxDBClient "github.com/influxdata/influxdb/client/v2"
)

var (
	conf                *configuration
	logger              *logrus.Entry
	dbClient            *influxDBClient.Client
	coinmarketcapClient *coinmarketcap.Client
)

type configuration struct {
	ingestionConf `json:"ingestion"`
}

type ingestionConf struct {
	coinmarketcapConf `json:"coinmarketcap"`
}

type coinmarketcapConf struct {
	Schema                   map[string]string `json:"schema"`
	TicksCheckPeriodMin      int               `json:"ticks_check_period_min"`
	GlobalDataCheckPeriodMin int               `json:"global_data_check_period_min"`
	LogLevel                 string            `json:"log_level"`
}

func init() {

	customFormatter := new(logrus.TextFormatter)
	customFormatter.FullTimestamp = true
	logrus.SetFormatter(customFormatter)

	logger = logrus.WithField("context", "[ingestion:coinmarketcap]")

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

	coinmarketcapClient = coinmarketcap.NewClient()
}

func Ingest() {

	// Ingest tickers
	go ingestTicks()

	// Ingest global data
	go ingestGlobalData()
}
