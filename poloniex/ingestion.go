package poloniex

import (
	"encoding/json"
	"io/ioutil"
	"time"
	"trading/poloniex/publicapi"
	"trading/poloniex/pushapi"

	influxDBClient "github.com/influxdata/influxdb/client/v2"
	log "github.com/sirupsen/logrus"
)

var (
	conf           *configuration
	dbClient       influxDBClient.Client
	publicClient   *publicapi.PublicClient
	pushClient     *pushapi.PushClient
	marketUpdaters map[string]pushapi.MarketUpdater
	pointsToWrite  chan *influxDBClient.Point
)

type configuration struct {
	Ingestion struct {
		Host                     string            `json:"host"`
		Auth                     map[string]string `json:"auth"`
		TlsCertificatePath       string            `json:"tls_certificate_path"`
		Schema                   map[string]string `json:"schema"`
		OrderBooksCheckPeriodMin int               `json:"order_books_check_period_min"`
		MarketCheckPeriodMin     int               `json:"market_check_period_min"`
		FlushPointsPeriodMs      int               `json:"flush_points_period_ms"`
		LogLevel                 string            `json:"log_level"`
	} `json:"ingestion"`
}

func init() {

	customFormatter := new(log.TextFormatter)
	customFormatter.FullTimestamp = true
	log.SetFormatter(customFormatter)

	content, err := ioutil.ReadFile("conf.json")

	if err != nil {
		log.WithField("error", err).Fatal("loading configuration")
	}

	if err := json.Unmarshal(content, &conf); err != nil {
		log.WithField("error", err).Fatal("loading configuration")
	}

	switch conf.Ingestion.LogLevel {
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

	initDb()

	publicClient = publicapi.NewPublicClient()

	pushClient, err = pushapi.NewPushClient()
	if err != nil {
		log.WithField("error", err).Fatal("pushapi.NewPushClient")
	}

	marketUpdaters = make(map[string]pushapi.MarketUpdater)
	pointsToWrite = make(chan *influxDBClient.Point, 500)
}

func Ingest() {

	// Init and checking order books periodically
	go func() {
		for {
			ingestOrderBooks()
			<-time.After(time.Duration(conf.Ingestion.OrderBooksCheckPeriodMin) *
				time.Second)
		}
	}()

	// Flushing market points periodiocally
	go func() {
		for {
			<-time.After(time.Duration(conf.Ingestion.FlushPointsPeriodMs) *
				time.Millisecond)
			if len(pointsToWrite) != 0 {
				flushMarketPoints(len(pointsToWrite))
			}
		}
	}()

	// Checking new markets periodically
	go func() {
		for {
			ingestNewMarkets()
			<-time.After(time.Duration(conf.Ingestion.MarketCheckPeriodMin) *
				time.Minute)
		}
	}()

	select {}
}
