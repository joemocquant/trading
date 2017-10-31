package database

import (
	"fmt"
	"time"

	ifxClient "github.com/influxdata/influxdb/client/v2"
	"github.com/sirupsen/logrus"
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
		logger.WithField("error", err).Error("flushBatchs: dbClient.Write")
		return
	}

	for _, batchPoints := range batchPointsArr {
		if batchPoints.Callback != nil {
			go batchPoints.Callback()
		}
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

	bittrexMarketDepthBatchCount, bittrexMarketDepthPointCount := 0, 0
	poloniexMarketDepthBatchCount, poloniexMarketDepthPointCount := 0, 0
	bittrexOHLCBatchCount, bittrexOHLCPointCount := 0, 0
	poloniexOHLCBatchCount, poloniexOHLCPointCount := 0, 0
	bittrexOBVBatchCount, bittrexOBVPointCount := 0, 0
	poloniexOBVBatchCount, poloniexOBVPointCount := 0, 0
	bittrexMABatchCount, bittrexMAPointCount := 0, 0
	poloniexMABatchCount, poloniexMAPointCount := 0, 0
	bittrexRSIBatchCount, bittrexRSIPointCount := 0, 0
	poloniexRSIBatchCount, poloniexRSIPointCount := 0, 0

	for _, batchPoints := range batchPointsArr {
		switch batchPoints.TypePoint {

		case "bittrexMarketDepth":
			bittrexMarketDepthBatchCount++
			bittrexMarketDepthPointCount += len(batchPoints.Points)

		case "poloniexMarketDepth":
			poloniexMarketDepthBatchCount++
			poloniexMarketDepthPointCount += len(batchPoints.Points)

		case "bittrexOHLC":
			bittrexOHLCBatchCount++
			bittrexOHLCPointCount += len(batchPoints.Points)

		case "poloniexOHLC":
			poloniexOHLCBatchCount++
			poloniexOHLCPointCount += len(batchPoints.Points)

		case "bittrexOBV":
			bittrexOBVBatchCount++
			bittrexOBVPointCount += len(batchPoints.Points)

		case "poloniexOBV":
			poloniexOBVBatchCount++
			poloniexOBVPointCount += len(batchPoints.Points)

		case "bittrexMA":
			bittrexMABatchCount++
			bittrexMAPointCount += len(batchPoints.Points)

		case "poloniexMA":
			poloniexMABatchCount++
			poloniexMAPointCount += len(batchPoints.Points)

		case "bittrexRSI":
			bittrexRSIBatchCount++
			bittrexRSIPointCount += len(batchPoints.Points)

		case "poloniexRSI":
			poloniexRSIBatchCount++
			poloniexRSIPointCount += len(batchPoints.Points)
		}
	}

	toPrint := fmt.Sprintf("[Metrics Flush]: %d batchs (%d points)",
		bittrexMarketDepthBatchCount+poloniexMarketDepthBatchCount+
			bittrexOHLCBatchCount+poloniexOHLCBatchCount+
			bittrexOBVBatchCount+poloniexOBVBatchCount+
			bittrexMABatchCount+poloniexMABatchCount+
			bittrexRSIBatchCount+poloniexRSIBatchCount,
		bittrexMarketDepthPointCount+poloniexMarketDepthPointCount+
			bittrexOHLCPointCount+poloniexOHLCPointCount+
			bittrexOBVPointCount+poloniexOBVPointCount+
			bittrexMAPointCount+poloniexMAPointCount+
			bittrexRSIPointCount+poloniexRSIPointCount)

	if bittrexMarketDepthBatchCount > 0 {
		toPrint += fmt.Sprintf(" %d bittrexMarketDepths (%d)",
			bittrexMarketDepthBatchCount, bittrexMarketDepthPointCount)
	}

	if poloniexMarketDepthBatchCount > 0 {
		toPrint += fmt.Sprintf(" %d poloniexMarketDepths (%d)",
			poloniexMarketDepthBatchCount, poloniexMarketDepthPointCount)
	}

	if bittrexOHLCBatchCount > 0 {
		toPrint += fmt.Sprintf(" %d bittrexOHLCs (%d)",
			bittrexOHLCBatchCount, bittrexOHLCPointCount)
	}

	if poloniexOHLCBatchCount > 0 {
		toPrint += fmt.Sprintf(" %d poloniexOHLCs (%d)",
			poloniexOHLCBatchCount, poloniexOHLCPointCount)
	}

	if bittrexOBVBatchCount > 0 {
		toPrint += fmt.Sprintf(" %d bittrexOBVs (%d)",
			bittrexOBVBatchCount, bittrexOBVPointCount)
	}

	if poloniexOBVBatchCount > 0 {
		toPrint += fmt.Sprintf(" %d poloniexOBVs (%d)",
			poloniexOBVBatchCount, poloniexOBVPointCount)
	}

	if bittrexMABatchCount > 0 {
		toPrint += fmt.Sprintf(" %d bittrexMAs (%d)",
			bittrexMABatchCount, bittrexMAPointCount)
	}

	if poloniexMABatchCount > 0 {
		toPrint += fmt.Sprintf(" %d poloniexMAs (%d)",
			poloniexMABatchCount, poloniexMAPointCount)
	}

	if bittrexRSIBatchCount > 0 {
		toPrint += fmt.Sprintf(" %d bittrexRSIs (%d)",
			bittrexRSIBatchCount, bittrexRSIPointCount)
	}

	if poloniexRSIBatchCount > 0 {
		toPrint += fmt.Sprintf(" %d poloniexRSIs (%d)",
			poloniexRSIBatchCount, poloniexRSIPointCount)
	}

	logger.Debug(toPrint)
}
