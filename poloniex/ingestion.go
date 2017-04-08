package poloniex

import (
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"io/ioutil"
	"sync"
	"time"
	"trading/poloniex/publicapi"
	"trading/poloniex/pushapi"

	influxDBClient "github.com/influxdata/influxdb/client/v2"
	log "github.com/sirupsen/logrus"
)

var (
	conf         *configuration
	publicClient *publicapi.PublicClient
	pushClient   *pushapi.PushClient
	dbClient     influxDBClient.Client

	mapMutex       sync.Mutex
	marketUpdaters map[string]pushapi.MarketUpdater
)

type configuration struct {
	Ingestion struct {
		Host                 string            `json:"host"`
		Auth                 map[string]string `json:"auth"`
		TlsCertificatePath   string            `json:"tls_certificate_path"`
		Schema               map[string]string `json:"schema"`
		MarketCheckPeriodMin int               `json:"market_check_period_min"`
		LogLevel             string            `json:"log_level"`
	} `json:"ingestion"`
}

func init() {

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

	publicClient = publicapi.NewPublicClient()

	tlsConfig := &tls.Config{RootCAs: x509.NewCertPool()}
	certPath := conf.Ingestion.TlsCertificatePath

	if crt, err := ioutil.ReadFile(certPath); err != nil {
		log.WithField("error", err).Fatal("reading certificate")
	} else {
		if ok := tlsConfig.RootCAs.AppendCertsFromPEM(crt); !ok {
			log.Fatal("cannot append certificate")
		}
	}

	dbClient, err = influxDBClient.NewHTTPClient(influxDBClient.HTTPConfig{
		Addr:      conf.Ingestion.Host,
		Username:  conf.Ingestion.Auth["username"],
		Password:  conf.Ingestion.Auth["password"],
		TLSConfig: tlsConfig,
	})

	if err != nil {
		log.WithField("error", err).Fatal("influxDBClient.NewHTTPClient")
	}

	pushClient, err = pushapi.NewPushClient()

	if err != nil {
		log.WithField("error", err).Fatal("pushapi.NewPushClient")
	}

	marketUpdaters = make(map[string]pushapi.MarketUpdater)
}

func Ingest() {

	for {
		updateMarkets()
		<-time.After(time.Duration(conf.Ingestion.MarketCheckPeriodMin) *
			time.Minute)
	}
}

func updateMarkets() {

	tickers, err := publicClient.GetTickers()

	for err != nil {
		log.WithField("error", err).Error("ingestion.updateMarkets: publicClient.GetTickers")
		tickers, err = publicClient.GetTickers()
	}

	mapMutex.Lock()
	defer mapMutex.Unlock()

	for currencyPair, _ := range tickers {

		if _, ok := marketUpdaters[currencyPair]; !ok {

			marketUpdater, err := pushClient.SubscribeMarket(currencyPair)

			if err != nil {

				log.WithFields(log.Fields{
					"currencyPair": currencyPair,
					"error":        err,
				}).Error("ingestion.updateMarkets: pushClient.SubscribeMarket")
				continue
			}

			log.Infof("Subscribed to: %s", currencyPair)

			marketUpdaters[currencyPair] = marketUpdater
			go dbWriter(marketUpdater, currencyPair)
		}
	}
}

func dbWriter(marketUpdater pushapi.MarketUpdater, currencyPair string) {

	for {
		marketUpdates := <-marketUpdater

		go func(marketUpdates *pushapi.MarketUpdates) {

			bp, err := influxDBClient.NewBatchPoints(influxDBClient.BatchPointsConfig{
				Database:  conf.Ingestion.Schema["database"],
				Precision: "ns",
			})
			if err != nil {
				log.WithField("error", err).Error("ingestion.dbWriter: dbClient.NewBatchPoints")
				return
			}

			for _, marketUpdate := range marketUpdates.Updates {

				pt, err := preparePoint(marketUpdate, currencyPair, marketUpdates.Sequence)
				if err != nil {
					log.WithField("error", err).Error("ingestion.dbWriter: ingestion.preparePoint")
					continue
				}
				bp.AddPoint(pt)
			}

			if err := dbClient.Write(bp); err != nil {
				log.WithFields(log.Fields{
					"batchPoints": bp,
					"error":       err,
				}).Error("ingestion.dbWriter: ingestion.dbClient.Write")
			}

		}(marketUpdates)

	}
}

func preparePoint(marketUpdate *pushapi.MarketUpdate,
	currencyPair string, sequence int64) (*influxDBClient.Point, error) {

	tags := make(map[string]string)
	fields := make(map[string]interface{})
	var measurement string
	var timestamp time.Time

	switch marketUpdate.TypeUpdate {

	case "orderBookModify":

		obm := marketUpdate.Data.(pushapi.OrderBookModify)

		tags = map[string]string{
			"order_type": obm.TypeOrder,
			"market":     currencyPair,
		}
		fields = map[string]interface{}{
			"sequence": sequence,
			"rate":     obm.Rate,
			"amount":   obm.Amount,
		}
		measurement = conf.Ingestion.Schema["book_updates_measurement"]
		timestamp = time.Now()

	case "orderBookRemove":

		obr := marketUpdate.Data.(pushapi.OrderBookRemove)

		tags = map[string]string{
			"order_type": obr.TypeOrder,
			"market":     currencyPair,
		}
		fields = map[string]interface{}{
			"sequence": sequence,
			"rate":     obr.Rate,
			"amount":   0.0,
		}
		measurement = conf.Ingestion.Schema["book_updates_measurement"]
		timestamp = time.Now()

	case "newTrade":

		nt := marketUpdate.Data.(pushapi.NewTrade)

		tags = map[string]string{
			"order_type": nt.TypeOrder,
			"market":     currencyPair,
		}
		fields = map[string]interface{}{
			"sequence": sequence,
			"rate":     nt.Rate,
			"amount":   nt.Amount,
		}
		measurement = conf.Ingestion.Schema["trade_updates_measurement"]

		nano := time.Now().UnixNano() % int64(time.Second)
		timestamp = time.Unix(nt.Date, nano)
	}

	pt, err := influxDBClient.NewPoint(measurement, tags, fields, timestamp)
	if err != nil {
		return nil, err
	}
	return pt, nil
}
