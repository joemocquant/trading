package bittrex

import (
	"time"
	"trading/api/bittrex/publicapi"
	"trading/ingestion"

	influxDBClient "github.com/influxdata/influxdb/client/v2"
)

func ingestMarketHistories() {

	for {

		markets := getActiveMarkets()

		for _, marketName := range markets {
			go ingestMarketHistory(marketName)
		}

		<-time.After(time.Duration(conf.MarketHistoriesCheckPeriodSec) * time.Second)
	}
}

func ingestMarketHistory(marketName string) {

	marketHistory, err := publicClient.GetMarketHistory(marketName)

	for err != nil {
		logger.WithField("error", err).Error("ingestMarketHistory: publicClient.GetMarketHistory")
		time.Sleep(5 * time.Second)
		marketHistory, err = publicClient.GetMarketHistory(marketName)
	}

	prepareMarketHistoryPoints(marketName, marketHistory)
}

func prepareMarketHistoryPoints(marketName string, mh publicapi.MarketHistory) {

	measurement := conf.Schema["market_histories_measurement"]
	points := make([]*influxDBClient.Point, 0, len(mh))

	tags := map[string]string{
		"source": "publicapi",
		"market": marketName,
	}

	lastTrade := getLastTrade(marketName)

	for _, trade := range mh {

		if lastTrade != nil && trade.Id <= lastTrade.Id {
			break
		}

		timestamp := time.Unix(trade.TimeStamp, 0)

		fields := map[string]interface{}{
			"id":         trade.Id,
			"quantity":   trade.Quantity,
			"price":      trade.Price,
			"total":      trade.Total,
			"fill_type":  trade.FillType,
			"order_type": trade.OrderType,
		}

		pt, err := influxDBClient.NewPoint(measurement, tags, fields, timestamp)
		if err != nil {
			logger.WithField("error", err).Error("prepareMarketHistoryPoints: influxDBClient.NewPoint")
			continue
		}
		points = append(points, pt)
	}

	if len(mh) != 0 && lastTrade != nil && mh[len(mh)-1].Id > lastTrade.Id {
		logger.Warnf("Possibly missing trades for market %s", marketName)
	}

	if len(mh) == 0 || !setLastTrade(marketName, mh[0]) {
		return
	}

	batchsToWrite <- &ingestion.BatchPoints{"marketHistory", points}
}

func getLastTrade(marketName string) *publicapi.Trade {

	lt.Lock()
	defer lt.Unlock()

	return lt.lastTrades[marketName]
}

func setLastTrade(marketName string, newTrade *publicapi.Trade) bool {

	lt.Lock()
	defer lt.Unlock()

	lastTrade, ok := lt.lastTrades[marketName]
	if !ok || newTrade.Id > lastTrade.Id {
		lt.lastTrades[marketName] = newTrade
		return true
	}

	return false
}
