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
	publicClient   *publicapi.PublicClient
	pushClient     *pushapi.PushClient
	marketUpdaters map[string]pushapi.MarketUpdater
	pointsToWrite  chan *influxDBClient.Point
)

type configuration struct {
	Ingestion struct {
		Host                 string            `json:"host"`
		Auth                 map[string]string `json:"auth"`
		TlsCertificatePath   string            `json:"tls_certificate_path"`
		Schema               map[string]string `json:"schema"`
		MarketCheckPeriodMin int               `json:"market_check_period_min"`
		FlushPointsPeriodMs  int               `json:"flush_points_period_ms"`
		LogLevel             string            `json:"log_level"`
	} `json:"ingestion"`
}

func init() {

	customFormatter := new(log.TextFormatter)
	customFormatter.TimestampFormat = "2006-01-02 15:04:05"
	log.SetFormatter(customFormatter)
	customFormatter.FullTimestamp = true

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

	// Flushing periodiocally
	go func() {
		for {
			<-time.After(time.Duration(conf.Ingestion.FlushPointsPeriodMs) *
				time.Millisecond)
			if len(pointsToWrite) != 0 {
				flushPoints(len(pointsToWrite))
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

func ingestNewMarkets() {

	tickers, err := publicClient.GetTickers()

	for err != nil {
		log.WithField("error", err).Error("ingestion.ingestNewMarkets: publicClient.GetTickers")
		time.Sleep(5 * time.Second)
		tickers, err = publicClient.GetTickers()
	}

	for currencyPair, _ := range tickers {

		if _, ok := marketUpdaters[currencyPair]; !ok {

			marketUpdater, err := pushClient.SubscribeMarket(currencyPair)

			if err != nil {

				log.WithFields(log.Fields{
					"currencyPair": currencyPair,
					"error":        err,
				}).Error("ingestion.ingestNewMarkets: pushClient.SubscribeMarket")
				continue
			}

			log.Infof("Subscribed to: %s", currencyPair)

			marketUpdaters[currencyPair] = marketUpdater
			go getNewPoints(marketUpdater, currencyPair)
		}
	}
}

func getNewPoints(marketUpdater pushapi.MarketUpdater, currencyPair string) {

	for {
		marketUpdates := <-marketUpdater

		go func(marketUpdates *pushapi.MarketUpdates) {

			for _, marketUpdate := range marketUpdates.Updates {

				pt, err := preparePoint(marketUpdate, currencyPair, marketUpdates.Sequence)
				if err != nil {
					log.WithField("error", err).Error("ingestion.dbWriter: ingestion.preparePoint")
					continue
				}

				pointsToWrite <- pt
			}

		}(marketUpdates)

	}
}
