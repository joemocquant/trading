package ingestion

import (
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"io/ioutil"

	influxDBClient "github.com/influxdata/influxdb/client/v2"
	log "github.com/sirupsen/logrus"
)

var (
	conf      *configuration
	tlsConfig *tls.Config
)

type configuration struct {
	ingestionConf `json:"ingestion"`
}

type ingestionConf struct {
	influxdbConf `json:"influxdb"`
}

type influxdbConf struct {
	Host               string            `json:"host"`
	Auth               map[string]string `json:"auth"`
	TlsCertificatePath string            `json:"tls_certificate_path"`
	LogLevel           string            `json:"log_level"`
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

	tlsConfig = &tls.Config{RootCAs: x509.NewCertPool()}
	certPath := conf.TlsCertificatePath

	if crt, err := ioutil.ReadFile(certPath); err != nil {
		log.WithField("error", err).Fatal("reading certificate")

	} else {

		if ok := tlsConfig.RootCAs.AppendCertsFromPEM(crt); !ok {
			log.Fatal("cannot append certificate")
		}
	}
}

func NewdbClient() (influxDBClient.Client, error) {

	dbClient, err := influxDBClient.NewHTTPClient(influxDBClient.HTTPConfig{
		Addr:      conf.Host,
		Username:  conf.Auth["username"],
		Password:  conf.Auth["password"],
		TLSConfig: tlsConfig,
	})

	if err != nil {
		return nil, err
	}

	return dbClient, nil
}
