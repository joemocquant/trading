package database

import (
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"io/ioutil"

	"github.com/Sirupsen/logrus"
	ifxClient "github.com/influxdata/influxdb/client/v2"
	prefixed "github.com/x-cray/logrus-prefixed-formatter"
)

var (
	conf      *configuration
	logger    *logrus.Entry
	tlsConfig *tls.Config
)

type configuration struct {
	influxdbConf `json:"influxdb"`
}

type influxdbConf struct {
	Host               string            `json:"host"`
	Auth               map[string]string `json:"auth"`
	TlsCertificatePath string            `json:"tls_certificate_path"`
	LogLevel           string            `json:"log_level"`
}

type BatchPoints struct {
	TypePoint string
	Points    []*ifxClient.Point
}

type FlushInfo struct {
	BatchsToWrite <-chan *BatchPoints
	Database      string
	DbClient      ifxClient.Client
}

func init() {

	customFormatter := new(prefixed.TextFormatter)
	customFormatter.FullTimestamp = true
	customFormatter.ForceColors = true
	customFormatter.ForceFormatting = true
	logrus.SetFormatter(customFormatter)

	logger = logrus.WithField("prefix", "[database]")

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

func NewdbClient() (ifxClient.Client, error) {

	dbClient, err := ifxClient.NewHTTPClient(ifxClient.HTTPConfig{
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

func QueryDB(dbClient ifxClient.Client, cmd, db string) ([]ifxClient.Result, error) {

	q := ifxClient.Query{
		Command:  cmd,
		Database: db,
	}

	if response, err := dbClient.Query(q); err != nil {
		return nil, err

	} else {

		if response.Error() != nil {
			return nil, response.Error()
		}

		return response.Results, nil
	}
}
