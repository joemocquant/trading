package database

import (
	"fmt"
	"time"

	"github.com/Sirupsen/logrus"
	ifxClient "github.com/influxdata/influxdb/client/v2"
)

func FlushEvery(period time.Duration, fi *FlushInfo) {

	for {
		<-time.After(period)

		if len(fi.BatchsToWrite) != 0 {
			flushBatchs(fi, len(fi.BatchsToWrite))
		}
	}
}

func flushBatchs(fi *FlushInfo, count int) {

	bp, err := ifxClient.NewBatchPoints(ifxClient.BatchPointsConfig{
		Database:  fi.Database,
		Precision: "ns",
	})

	if err != nil {
		logger.WithField("error", err).Error(
			"flushBatchs: dbClient.NewBatchPoints")
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
		return
	}

	if logrus.GetLevel() >= logrus.DebugLevel {

		switch fi.Database {
		case "poloniex":
			flushPoloniexDebug(batchPointsArr)
		case "bittrex":
			flushBittrexDebug(batchPointsArr)
		case "metrics":
			flushMetricsDebug(batchPointsArr)
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
		tickBatchCount+marketBatchCount+missingTradeBatchCount+
			orderBookBatchCount+orderBookLastCheckBatchCount,
		tickPointCount+marketPointCount+missingTradePointCount+
			orderBookPointCount+orderBookLastCheckPointCount)

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
	orderBookLastCheckBatchCount, orderBookLastCheckPointCount := 0, 0

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

		case "orderBookLastCheck":
			orderBookLastCheckBatchCount++
			orderBookLastCheckPointCount += len(batchPoints.Points)
		}
	}

	toPrint := fmt.Sprintf("[Bittrex Flush]: %d batchs (%d points)",
		marketSummaryBatchCount+marketHistoryBatchCount+
			orderBookBatchCount+orderBookLastCheckBatchCount,
		marketSummaryPointCount+marketHistoryPointCount+
			orderBookPointCount+orderBookLastCheckPointCount)

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

	if orderBookLastCheckBatchCount > 0 {
		toPrint += fmt.Sprintf(" %d orderBookLastChecks (%d)",
			orderBookLastCheckBatchCount, orderBookLastCheckPointCount)
	}

	logger.Debug(toPrint)
}

func flushMetricsDebug(batchPointsArr []*BatchPoints) {

	poloniexMarketDepthBatchCount, poloniexMarketDepthPointCount := 0, 0
	bittrexMarketDepthBatchCount, bittrexMarketDepthPointCount := 0, 0

	for _, batchPoints := range batchPointsArr {
		switch batchPoints.TypePoint {

		case "poloniexMarketDepth":
			poloniexMarketDepthBatchCount++
			poloniexMarketDepthPointCount += len(batchPoints.Points)

		case "bittrexMarketDepth":
			bittrexMarketDepthBatchCount++
			bittrexMarketDepthPointCount += len(batchPoints.Points)
		}
	}

	toPrint := fmt.Sprintf("[Metrics Flush]: %d batchs (%d points)",
		poloniexMarketDepthBatchCount+bittrexMarketDepthBatchCount,
		poloniexMarketDepthPointCount+bittrexMarketDepthPointCount)

	if poloniexMarketDepthBatchCount > 0 {
		toPrint += fmt.Sprintf(" %d poloniexMarketDepths (%d)",
			poloniexMarketDepthBatchCount, poloniexMarketDepthPointCount)
	}

	if bittrexMarketDepthBatchCount > 0 {
		toPrint += fmt.Sprintf(" %d bittrexMarketDepths (%d)",
			bittrexMarketDepthBatchCount, bittrexMarketDepthPointCount)
	}

	logger.Debug(toPrint)
}
