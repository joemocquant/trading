package ingestion

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"time"

	"github.com/Sirupsen/logrus"
	influxDBClient "github.com/influxdata/influxdb/client/v2"
	prefixed "github.com/x-cray/logrus-prefixed-formatter"
)

var (
	conf   *configuration
	logger *logrus.Entry
)

type configuration struct {
	ingestionConf `json:"ingestion"`
}

type ingestionConf struct {
	LogLevel string `json:"log_level"`
}

type FlushInfo struct {
	BatchsToWrite <-chan *BatchPoints
	Database      string
	DbClient      influxDBClient.Client
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

	if err := fi.DbClient.Write(bp); err != nil {
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

	tickBatchCount, tickPointCount := 0, 0
	marketBatchCount, marketPointCount := 0, 0
	missingTradeBatchCount, missingTradePointCount := 0, 0
	orderBookBatchCount, orderBookPointCount := 0, 0
	orderBookLastCheckBatchCount, orderBookLastCheckPointCount := 0, 0

	for _, batchPoints := range batchPointsArr {
		switch batchPoints.TypePoint {

		case "tick":
			tickBatchCount++
			tickPointCount += len(batchPoints.Points)

		case "market":
			marketBatchCount++
			marketPointCount += len(batchPoints.Points)

		case "missingTrade":
			missingTradeBatchCount++
			missingTradePointCount += len(batchPoints.Points)

		case "orderBook":
			orderBookBatchCount++
			orderBookPointCount += len(batchPoints.Points)

		case "orderBookLastCheck":
			orderBookLastCheckBatchCount++
			orderBookLastCheckPointCount += len(batchPoints.Points)

		}
	}

	toPrint := fmt.Sprintf("[Poloniex flush]: %d batchs (%d points)",
		tickBatchCount+marketBatchCount+missingTradeBatchCount+orderBookBatchCount+orderBookLastCheckBatchCount,
		tickPointCount+marketPointCount+missingTradePointCount+orderBookPointCount+orderBookLastCheckPointCount)

	if tickBatchCount > 0 {
		toPrint += fmt.Sprintf(" %d ticks (%d)",
			tickBatchCount, tickPointCount)
	}

	if marketBatchCount > 0 {
		toPrint += fmt.Sprintf(" %d markets (%d)",
			marketBatchCount, marketPointCount)
	}

	if missingTradeBatchCount > 0 {
		toPrint += fmt.Sprintf(" %d missingTrades (%d)",
			missingTradeBatchCount, missingTradePointCount)
	}

	if orderBookBatchCount > 0 {
		toPrint += fmt.Sprintf(" %d orderBooks (%d)",
			orderBookBatchCount, orderBookPointCount)
	}

	if orderBookLastCheckBatchCount > 0 {
		toPrint += fmt.Sprintf(" %d orderBookLastChecks (%d)",
			orderBookLastCheckBatchCount, orderBookLastCheckPointCount)
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
