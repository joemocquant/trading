package poloniex

import (
	"encoding/json"
	"io/ioutil"
	"time"
	"trading/ingestion"
	"trading/poloniex/publicapi"
	"trading/poloniex/pushapi"

	"github.com/Sirupsen/logrus"
	influxDBClient "github.com/influxdata/influxdb/client/v2"
)

var (
	conf           *configuration
	logger         *logrus.Entry
	dbClient       influxDBClient.Client
	publicClient   *publicapi.PublicClient
	pushClient     *pushapi.PushClient
	marketUpdaters map[string]pushapi.MarketUpdater
	pointsToWrite  chan *batchPoints
)

type configuration struct {
	ingestionConf `json:"ingestion"`
}

type ingestionConf struct {
	poloniexConf `json:"poloniex"`
}

type poloniexConf struct {
	Schema                    map[string]string `json:"schema"`
	PublicTicksCheckPeriodSec int               `json:"public_ticks_check_period_sec"`
	OrderBooksCheckPeriodSec  int               `json:"order_books_check_period_sec"`
	MarketCheckPeriodMin      int               `json:"market_check_period_min"`
	FlushPointsPeriodMs       int               `json:"flush_points_period_ms"`
	LogLevel                  string            `json:"log_level"`
}

type batchPoints struct {
	typePoint string
	points    []*influxDBClient.Point
}

func init() {

	customFormatter := new(logrus.TextFormatter)
	customFormatter.FullTimestamp = true
	logrus.SetFormatter(customFormatter)

	logger = logrus.WithField("context", "[ingestion:poloniex]")

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

	if dbClient, err = ingestion.NewdbClient(); err != nil {
		logger.WithField("error", err).Fatal("ingestion.NewdbClient")
	}

	publicClient = publicapi.NewPublicClient()

	pushClient, err = pushapi.NewPushClient()
	if err != nil {
		logger.WithField("error", err).Fatal("pushapi.NewPushClient")
	}

	marketUpdaters = make(map[string]pushapi.MarketUpdater)
	pointsToWrite = make(chan *batchPoints, 5000)
}

func Ingest() {

	// flushing points periodically
	go func() {
		for {
			<-time.After(time.Duration(conf.FlushPointsPeriodMs) * time.Millisecond)
			if len(pointsToWrite) != 0 {
				flushPoints(len(pointsToWrite))
			}
		}
	}()

	//-- Ticks

	// Ingest ticks
	go ingestTicks()

	//-- OrderBooks

	// Init and checking order books periodically
	go ingestOrderBooks(100000, time.Duration(conf.OrderBooksCheckPeriodSec)*time.Second)

	//-- Market

	// Ingest and checking new markets periodically
	go func() {
		for {
			ingestNewMarkets()
			<-time.After(time.Duration(conf.MarketCheckPeriodMin) * time.Minute)
		}
	}()
}

func flushPoints(batchCount int) {

	bp, err := influxDBClient.NewBatchPoints(influxDBClient.BatchPointsConfig{
		Database:  conf.Schema["database"],
		Precision: "ns",
	})
	if err != nil {
		logger.WithField("error", err).Error("flushPoints: dbClient.NewBatchPoints")
		return
	}

	batchPointsArr := []*batchPoints{}

	for i := 0; i < batchCount; i++ {
		batchPointsArr = append(batchPointsArr, <-pointsToWrite)
	}

	for _, batchPoints := range batchPointsArr {
		bp.AddPoints(batchPoints.points)
	}

	if err := dbClient.Write(bp); err != nil {
		logger.WithFields(logrus.Fields{
			"batchPoints": bp,
			"error":       err,
		}).Error("flushPoints: dbClient.Write")
	}

	if logrus.GetLevel() >= logrus.DebugLevel {
		flushDebug(batchPointsArr)
	}
}

func flushDebug(batchPointsArr []*batchPoints) {

	orderBookBatchCount, orderBookLastCheckBatchCount, marketBatchCount, tickBatchCount := 0, 0, 0, 0
	orderBookPointCount, orderBookLastCheckPointCount, marketPointCount, tickPointCount := 0, 0, 0, 0

	for _, batchPoints := range batchPointsArr {
		switch batchPoints.typePoint {

		case "orderBook":
			orderBookBatchCount++
			orderBookPointCount += len(batchPoints.points)

		case "orderBookLastCheck":
			orderBookLastCheckBatchCount++
			orderBookLastCheckPointCount += len(batchPoints.points)
		case "markets":
			marketBatchCount++
			marketPointCount += len(batchPoints.points)

		case "ticks":
			tickBatchCount++
			tickPointCount += len(batchPoints.points)
		}
	}

	logger.Debugf("Flushed: %d batchs (%d points)\n"+
		"\t%d orderBook batchs (%d points)\n"+
		"\t%d orderBookLastCheck batchs (%d points)\n"+
		"\t%d market batchs (%d points)\n"+
		"\t%d tick batchs (%d points))",
		orderBookBatchCount+orderBookLastCheckBatchCount+marketBatchCount+tickBatchCount,
		orderBookPointCount+orderBookLastCheckPointCount+marketPointCount+tickPointCount,
		orderBookBatchCount, orderBookPointCount,
		orderBookLastCheckBatchCount, orderBookLastCheckPointCount,
		marketBatchCount, marketPointCount,
		tickBatchCount, tickPointCount)
}
