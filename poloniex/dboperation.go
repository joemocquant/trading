package poloniex

import (
	"crypto/tls"
	"crypto/x509"
	"io/ioutil"

	influxDBClient "github.com/influxdata/influxdb/client/v2"
	log "github.com/sirupsen/logrus"
)

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
