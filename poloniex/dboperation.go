package poloniex

import (
	"crypto/tls"
	"crypto/x509"
	"io/ioutil"
	"time"
	"trading/poloniex/pushapi"

	influxDBClient "github.com/influxdata/influxdb/client/v2"
	log "github.com/sirupsen/logrus"
)

var dbClient influxDBClient.Client

func initDb() {

	tlsConfig := &tls.Config{RootCAs: x509.NewCertPool()}
	certPath := conf.Ingestion.TlsCertificatePath

	if crt, err := ioutil.ReadFile(certPath); err != nil {
		log.WithField("error", err).Fatal("reading certificate")

	} else {

		if ok := tlsConfig.RootCAs.AppendCertsFromPEM(crt); !ok {
			log.Fatal("cannot append certificate")
		}
	}

	var err error
	dbClient, err = influxDBClient.NewHTTPClient(influxDBClient.HTTPConfig{
		Addr:      conf.Ingestion.Host,
		Username:  conf.Ingestion.Auth["username"],
		Password:  conf.Ingestion.Auth["password"],
		TLSConfig: tlsConfig,
	})

	if err != nil {
		log.WithField("error", err).Fatal("influxDBClient.NewHTTPClient")
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

func flushPoints(number int) {

	bp, err := influxDBClient.NewBatchPoints(influxDBClient.BatchPointsConfig{
		Database:  conf.Ingestion.Schema["database"],
		Precision: "ns",
	})
	if err != nil {
		log.WithField("error", err).Error("ingestion.flushPoints: dbClient.NewBatchPoints")
		return
	}

	for i := 0; i < number; i++ {
		bp.AddPoint(<-pointsToWrite)
	}

	if err := dbClient.Write(bp); err != nil {
		log.WithFields(log.Fields{
			"batchPoints": bp,
			"error":       err,
		}).Error("ingestion.flushPoints: ingestion.dbClient.Write")
	}
}
