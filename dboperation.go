package ingestion

import (
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"io/ioutil"

	"github.com/Sirupsen/logrus"
	influxDBClient "github.com/influxdata/influxdb/client/v2"
)

var (
	conf      *configuration
	logger    *logrus.Entry
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

	customFormatter := new(logrus.TextFormatter)
	customFormatter.FullTimestamp = true
	logrus.SetFormatter(customFormatter)

	logger = logrus.WithField("context", "[ingestion]")

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

	tlsConfig = &tls.Config{RootCAs: x509.NewCertPool()}
	certPath := conf.TlsCertificatePath

	if crt, err := ioutil.ReadFile(certPath); err != nil {
		logger.WithField("error", err).Fatal("reading certificate")

	} else {

		if ok := tlsConfig.RootCAs.AppendCertsFromPEM(crt); !ok {
			logger.Fatal("cannot append certificate")
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
