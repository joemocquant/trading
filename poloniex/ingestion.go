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
	dbClient       influxDBClient.Client
	publicClient   *publicapi.PublicClient
	pushClient     *pushapi.PushClient
	marketUpdaters map[string]pushapi.MarketUpdater
	pointsToWrite  chan *batchPoints
)

type configuration struct {
	Ingestion struct {
		Host                     string            `json:"host"`
		Auth                     map[string]string `json:"auth"`
		TlsCertificatePath       string            `json:"tls_certificate_path"`
		Schema                   map[string]string `json:"schema"`
		OrderBooksCheckPeriodSec int               `json:"order_books_check_period_sec"`
		MarketCheckPeriodMin     int               `json:"market_check_period_min"`
		FlushPointsPeriodMs      int               `json:"flush_points_period_ms"`
		LogLevel                 string            `json:"log_level"`
	} `json:"ingestion"`
}

type batchPoints struct {
	typePoint string
	points    []*influxDBClient.Point
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
	pointsToWrite = make(chan *batchPoints, 5000)
}

func Ingest() {

	// flushing points periodically
	go func() {
		for {
			<-time.After(time.Duration(conf.Ingestion.FlushPointsPeriodMs) * time.Millisecond)
			if len(pointsToWrite) != 0 {
				flushPoints(len(pointsToWrite))
			}
		}
	}()

	//-- Ticks

	// Ingest tickers
	go ingestTicks()

	//-- OrderBooks

	// Init and checking order books periodically
	go ingestOrderBooks(100000, time.Duration(conf.Ingestion.OrderBooksCheckPeriodSec)*time.Second)

	//-- Market

	// Ingest and checking new markets periodically
	go func() {
		for {
			ingestNewMarkets()
			<-time.After(time.Duration(conf.Ingestion.MarketCheckPeriodMin) * time.Minute)
		}
	}()

	select {}
}

func flushPoints(batchCount int) {

	bp, err := influxDBClient.NewBatchPoints(influxDBClient.BatchPointsConfig{
		Database:  conf.Ingestion.Schema["database"],
		Precision: "ns",
	})
	if err != nil {
		log.WithField("error", err).Error("ingestion.flushPoints: dbClient.NewBatchPoints")
		return
	}

	batchPointsArr := []*batchPoints{}

	for i := 0; i < batchCount; i++ {
		batchPointsArr = append(batchPointsArr, <-pointsToWrite)
	}

	for _, batchPoints := range batchPointsArr {
		for _, pt := range batchPoints.points {
			bp.AddPoint(pt)
		}
	}

	if err := dbClient.Write(bp); err != nil {
		log.WithFields(log.Fields{
			"batchPoints": bp,
			"error":       err,
		}).Error("ingestion.flushPoints: ingestion.dbClient.Write")
	}

	if log.GetLevel() >= log.DebugLevel {
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

	log.Debugf("Flushed: %d batchs (%d points)\n"+
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
