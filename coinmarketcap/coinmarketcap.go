package coinmarketcap

import (
	"encoding/json"
	"io/ioutil"
	"trading/coinmarketcap"
	"trading/ingestion"

	influxDBClient "github.com/influxdata/influxdb/client/v2"
	log "github.com/sirupsen/logrus"
)

var (
	conf                *configuration
	dbClient            influxDBClient.Client
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

	customFormatter := new(log.TextFormatter)
	customFormatter.FullTimestamp = true
	log.SetFormatter(customFormatter)

	log.WithField("context", "coinmarketcap")

	content, err := ioutil.ReadFile("conf.json")

	if err != nil {
		log.WithField("error", err).Fatal("loading configuration")
	}

	if err := json.Unmarshal(content, &conf); err != nil {
		log.WithField("error", err).Fatal("loading configuration")
	}

	switch conf.LogLevel {
	case "debug":
		log.SetLevel(log.DebugLevel)
	case "info":
		log.SetLevel(log.InfoLevel)
	case "warn":
		log.SetLevel(log.WarnLevel)
	case "error":
		log.SetLevel(log.ErrorLevel)
	case "fatal":
		log.SetLevel(log.FatalLevel)
	case "panic":
		log.SetLevel(log.PanicLevel)
	default:
		log.SetLevel(log.WarnLevel)
	}

	if dbClient, err = ingestion.NewdbClient(); err != nil {
		log.WithField("error", err).Fatal("ingestion.NewdbClient")
	}

	coinmarketcapClient = coinmarketcap.NewClient()
}

func Ingest() {

	// Ingest tickers
	go ingestTicks()

	// Ingest global data
	go ingestGlobalData()
}
