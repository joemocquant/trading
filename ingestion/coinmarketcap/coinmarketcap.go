package coinmarketcap

import (
	"encoding/json"
	"io/ioutil"
	"trading/networking/database"

	ifxClient "github.com/influxdata/influxdb/client/v2"
	coinmarketcap "github.com/joemocquant/cmc-api"
	"github.com/sirupsen/logrus"
	prefixed "github.com/x-cray/logrus-prefixed-formatter"
)

var (
	conf                *configuration
	logger              *logrus.Entry
	dbClient            ifxClient.Client
	coinmarketcapClient *coinmarketcap.Client
)

type configuration struct {
	*ingestionConf `json:"ingestion"`
}

type ingestionConf struct {
	LogLevel           string `json:"log_level"`
	*coinmarketcapConf `json:"coinmarketcap"`
}

type coinmarketcapConf struct {
	Schema                   map[string]string `json:"schema"`
	TicksCheckPeriodMin      int               `json:"ticks_check_period_min"`
	GlobalDataCheckPeriodMin int               `json:"global_data_check_period_min"`
}

func init() {

	customFormatter := new(prefixed.TextFormatter)
	customFormatter.FullTimestamp = true
	customFormatter.ForceColors = true
	customFormatter.ForceFormatting = true
	logrus.SetFormatter(customFormatter)

	logger = logrus.WithField("prefix", "[ingestion:coinmarketcap]")

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

	coinmarketcapClient = coinmarketcap.NewClient()
}

func Ingest() {

	// Ingest tickers
	go ingestTicks()

	// Ingest global data
	go ingestGlobalData()
}
