package bittrex

import (
	"time"
	"trading/api/bittrex/publicapi"
	"trading/database"

	ifxClient "github.com/influxdata/influxdb/client/v2"
)

func ingestMarketHistories() {

	period := time.Duration(conf.MarketHistoriesCheckPeriodSec) * time.Second

	for {

		markets := getActiveMarketNames()

		for _, marketName := range markets {
			go ingestMarketHistory(marketName)
		}

		<-time.After(period)
	}
}

func ingestMarketHistory(marketName string) {

	marketHistory, err := publicClient.GetMarketHistory(marketName)

	for err != nil {
		logger.WithField("error", err).Error(
			"ingestMarketHistory: publicClient.GetMarketHistory")

		time.Sleep(5 * time.Second)
		marketHistory, err = publicClient.GetMarketHistory(marketName)
	}

	prepareMarketHistoryPoints(marketName, marketHistory)
}

func prepareMarketHistoryPoints(marketName string,
	mh publicapi.MarketHistory) {

	measurement := conf.Schema["market_histories_measurement"]
	points := make([]*ifxClient.Point, 0, len(mh))

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

		pt, err := ifxClient.NewPoint(measurement, tags, fields, timestamp)
		if err != nil {
			logger.WithField("error", err).Error(
				"prepareMarketHistoryPoints: ifxClient.NewPoint")
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

	batchsToWrite <- &database.BatchPoints{"marketHistory", points}
}

func getLastTrade(marketName string) *publicapi.Trade {

	lts.Lock()
	defer lts.Unlock()

	return lts.lastTrades[marketName]
}

func setLastTrade(marketName string, newTrade *publicapi.Trade) bool {

	lts.Lock()
	defer lts.Unlock()

	lastTrade, ok := lts.lastTrades[marketName]
	if !ok || newTrade.Id > lastTrade.Id {
		lts.lastTrades[marketName] = newTrade
		return true
	}

	return false
}
