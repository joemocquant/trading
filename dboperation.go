package ingestion

import (
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"time"

	"github.com/Sirupsen/logrus"
	influxDBClient "github.com/influxdata/influxdb/client/v2"
	prefixed "github.com/x-cray/logrus-prefixed-formatter"
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

type FlushInfo struct {
	BatchsToWrite <-chan *BatchPoints
	Database      string
	DbClient      *influxDBClient.Client
}

type BatchPoints struct {
	TypePoint string
	Points    []*influxDBClient.Point
}

func init() {

	customFormatter := new(prefixed.TextFormatter)
	customFormatter.FullTimestamp = true
	customFormatter.ForceColors = true
	customFormatter.ForceFormatting = true
	logrus.SetFormatter(customFormatter)

	logger = logrus.WithField("prefix", "[ingestion]")

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

func NewdbClient() (*influxDBClient.Client, error) {

	dbClient, err := influxDBClient.NewHTTPClient(influxDBClient.HTTPConfig{
		Addr:      conf.Host,
		Username:  conf.Auth["username"],
		Password:  conf.Auth["password"],
		TLSConfig: tlsConfig,
	})

	if err != nil {
		return nil, err
	}

	return &dbClient, nil
}

func FlushEvery(period time.Duration, fi *FlushInfo) {

	for {
		<-time.After(period)

		if len(fi.BatchsToWrite) != 0 {
			flushBatchs(fi, len(fi.BatchsToWrite))
		}
	}
}

func flushBatchs(fi *FlushInfo, count int) {

	bp, err := influxDBClient.NewBatchPoints(influxDBClient.BatchPointsConfig{
		Database:  fi.Database,
		Precision: "ns",
	})
	if err != nil {
		logger.WithField("error", err).Error("flushBatchs: dbClient.NewBatchPoints")
		return
	}

	batchPointsArr := []*BatchPoints{}

	for i := 0; i < count; i++ {
		batchPointsArr = append(batchPointsArr, <-fi.BatchsToWrite)
	}

	for _, batchPoints := range batchPointsArr {
		bp.AddPoints(batchPoints.Points)
	}

	if err := (*fi.DbClient).Write(bp); err != nil {
		logger.WithFields(logrus.Fields{
			"batchPoints": bp,
			"error":       err,
		}).Error("flushBatchs: dbClient.Write")
	}

	if logrus.GetLevel() >= logrus.DebugLevel {

		if fi.Database == "poloniex" {
			flushPoloniexDebug(batchPointsArr)
		} else if fi.Database == "bittrex" {
			flushBittrexDebug(batchPointsArr)
		}
	}
}

func flushPoloniexDebug(batchPointsArr []*BatchPoints) {

	orderBookBatchCount, orderBookLastCheckBatchCount, marketBatchCount, tickBatchCount := 0, 0, 0, 0
	orderBookPointCount, orderBookLastCheckPointCount, marketPointCount, tickPointCount := 0, 0, 0, 0

	for _, batchPoints := range batchPointsArr {
		switch batchPoints.TypePoint {

		case "orderBook":
			orderBookBatchCount++
			orderBookPointCount += len(batchPoints.Points)

		case "orderBookLastCheck":
			orderBookLastCheckBatchCount++
			orderBookLastCheckPointCount += len(batchPoints.Points)
		case "market":
			marketBatchCount++
			marketPointCount += len(batchPoints.Points)

		case "tick":
			tickBatchCount++
			tickPointCount += len(batchPoints.Points)
		}
	}

	toPrint := fmt.Sprintf("[Poloniex flush]: %d batchs (%d points)",
		orderBookBatchCount+orderBookLastCheckBatchCount+marketBatchCount+tickBatchCount,
		orderBookPointCount+orderBookLastCheckPointCount+marketPointCount+tickPointCount)

	if orderBookBatchCount > 0 {
		toPrint += fmt.Sprintf(" %d orderBooks (%d)",
			orderBookBatchCount, orderBookPointCount)
	}

	if orderBookLastCheckBatchCount > 0 {
		toPrint += fmt.Sprintf(" %d orderBookLastChecks (%d)",
			orderBookLastCheckBatchCount, orderBookLastCheckPointCount)
	}

	if marketBatchCount > 0 {
		toPrint += fmt.Sprintf(" %d markets (%d)",
			marketBatchCount, marketPointCount)
	}

	if tickBatchCount > 0 {
		toPrint += fmt.Sprintf(" %d ticks (%d)",
			tickBatchCount, tickPointCount)
	}

	logger.Debug(toPrint)
}

func flushBittrexDebug(batchPointsArr []*BatchPoints) {

	marketSummaryBatchCount, marketSummaryPointCount := 0, 0
	marketHistoryBatchCount, marketHistoryPointCount := 0, 0
	orderBookBatchCount, orderBookPointCount := 0, 0

	for _, batchPoints := range batchPointsArr {
		switch batchPoints.TypePoint {

		case "marketSummary":
			marketSummaryBatchCount++
			marketSummaryPointCount += len(batchPoints.Points)

		case "marketHistory":
			marketHistoryBatchCount++
			marketHistoryPointCount += len(batchPoints.Points)

		case "orderBook":
			orderBookBatchCount++
			orderBookPointCount += len(batchPoints.Points)
		}
	}

	toPrint := fmt.Sprintf("[Bittrex Flush]: %d batchs (%d points)",
		marketSummaryBatchCount+marketHistoryBatchCount+orderBookBatchCount,
		marketSummaryPointCount+marketHistoryPointCount+orderBookPointCount)

	if marketSummaryBatchCount > 0 {
		toPrint += fmt.Sprintf(" %d marketSummaries (%d)",
			marketSummaryBatchCount, marketSummaryPointCount)
	}

	if marketHistoryBatchCount > 0 {
		toPrint += fmt.Sprintf(" %d marketHistories (%d)",
			marketHistoryBatchCount, marketHistoryPointCount)
	}

	if orderBookBatchCount > 0 {
		toPrint += fmt.Sprintf(" %d orderBooks (%d)",
			orderBookBatchCount, orderBookPointCount)
	}

	logger.Debug(toPrint)
}
