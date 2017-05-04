package poloniex

import (
	"encoding/json"
	"fmt"
	"time"
	"trading/api/poloniex/publicapi"
	"trading/ingestion"

	influxDBClient "github.com/influxdata/influxdb/client/v2"
)

func ingestMissingTrades() {

	// checking missing trades periodically
	period := time.Duration(conf.MissingTradesCheckPeriodSec) * time.Second

	for {
		end := time.Now()
		start := end.Add(-2 * period)

		go func(start, end time.Time) {
			res := getLastIngestedTrades(start, end)
			missingTradeIds := getMissingTradeIds(res)
			updateMissingTrades(missingTradeIds, start, end)
		}(start, end)

		<-time.After(period)
	}
}

func getLastIngestedTrades(start, end time.Time) []influxDBClient.Result {

	cmd := fmt.Sprintf(
		"SELECT trade_id FROM trade_updates WHERE time >= %d and time < %d GROUP BY market",
		start.UnixNano(), end.UnixNano())

	res, err := ingestion.QueryDB(dbClient, cmd, conf.Schema["database"])

	for err != nil {
		logger.WithField("error", err).Error("getLastTrades: ingestion.QueryDB")
		time.Sleep(5 * time.Second)
		res, err = ingestion.QueryDB(dbClient, cmd, conf.Schema["database"])
	}

	return res
}

func getMissingTradeIds(res []influxDBClient.Result) map[string]map[int64]struct{} {

	missingTradeIds := make(map[string]map[int64]struct{})

	for _, serie := range res[0].Series {

		currencyPair := serie.Tags["market"]
		var prevId int64 = 0

		for _, record := range serie.Values {

			tradeId, ok := record[1].(json.Number)
			if !ok {
				logger.Errorf("Wrong tradeId type: %v (%s)", record[1], currencyPair)
				continue
			}

			id, err := tradeId.Int64()
			if err != nil {
				logger.Errorf("Wrong tradeId type: %v (%s)", tradeId, currencyPair)
				continue
			}

			if prevId != 0 && prevId+1 != id {

				if _, ok := missingTradeIds[currencyPair]; !ok {
					missingTradeIds[currencyPair] = make(map[int64]struct{})
				}

				for i := prevId + 1; i < id; i++ {
					missingTradeIds[currencyPair][i] = struct{}{}
				}

			}
			prevId = id
		}
	}

	return missingTradeIds
}

func updateMissingTrades(missingTradeIds map[string]map[int64]struct{}, start, end time.Time) {

	for currencyPair, _ := range missingTradeIds {

		go func(currencyPair string) {

			th, err := publicClient.GetTradeHistory(currencyPair, start, end)

			for err != nil {
				logger.WithField("error", err).Error("getLastTrades: publicClient.GetTradeHistory")
				time.Sleep(5 * time.Second)
				th, err = publicClient.GetTradeHistory(currencyPair, start, end)
			}

			missingTrades := make([]*publicapi.Trade, 0, len(missingTradeIds[currencyPair]))
			for _, trade := range th {
				if _, ok := missingTradeIds[currencyPair][trade.TradeId]; ok {
					missingTrades = append(missingTrades, trade)
				}
			}

			prepareMissingTradePoints(currencyPair, missingTrades)

		}(currencyPair)
	}
}

func prepareMissingTradePoints(currencyPair string, mt []*publicapi.Trade) {

	measurement := conf.Schema["trade_updates_measurement"]
	points := make([]*influxDBClient.Point, 0, len(mt))

	for _, trade := range mt {

		tags := map[string]string{
			"source":     "publicapi",
			"order_type": trade.TypeOrder,
			"market":     currencyPair,
		}

		fields := map[string]interface{}{
			"trade_id": trade.TradeId,
			"rate":     trade.Rate,
			"quantity": trade.Amount,
			"total":    trade.Total,
		}

		timestamp := time.Unix(trade.Date, trade.TradeId%1000000000)

		pt, err := influxDBClient.NewPoint(measurement, tags, fields, timestamp)
		if err != nil {
			logger.WithField("error", err).Error("prepareMissingTradePoints: influxDBClient.NewPoint")
			continue
		}
		points = append(points, pt)
	}

	batchsToWrite <- &ingestion.BatchPoints{"missingTrade", points}
}
