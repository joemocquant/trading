package poloniex

import (
	"fmt"
	"time"
	"trading/api/poloniex/publicapi"
	"trading/networking"
	"trading/networking/database"

	"github.com/Sirupsen/logrus"
	ifxClient "github.com/influxdata/influxdb/client/v2"
)

func ingestMissingTrades() {

	// checking missing trades periodically
	period := time.Duration(conf.MissingTradesCheckPeriodSec) * time.Second

	go networking.RunEvery(period, func(nextRun int64) {

		start := time.Unix(0, nextRun)
		end := time.Unix(0, nextRun-int64(period))

		res := getLastIngestedTrades(start, end)
		if res == nil {
			return
		}

		missingTradeIds := getMissingTradeIds(res)
		updateMissingTrades(missingTradeIds, start, end)

	})
}

func getLastIngestedTrades(start, end time.Time) []ifxClient.Result {

	cmd := fmt.Sprintf(
		"SELECT trade_id FROM %s WHERE time >= %d AND time < %d GROUP BY market",
		conf.Schema["trades_measurement"], start.UnixNano(), end.UnixNano())

	var res []ifxClient.Result

	request := func() (err error) {
		res, err = database.QueryDB(dbClient, cmd, conf.Schema["database"])
		return err
	}

	success := networking.ExecuteRequest(&networking.RequestInfo{
		Logger:   logger,
		Period:   0,
		ErrorMsg: "getLastIngestedTrades: database.QueryDB",
		Request:  request,
	})

	if !success {
		return nil
	}

	return res
}

func getMissingTradeIds(
	trades []ifxClient.Result) map[string]map[int64]struct{} {

	missingTradeIds := make(map[string]map[int64]struct{})

	for _, serie := range trades[0].Series {

		market := serie.Tags["market"]
		var prevId int64 = 0

		for _, record := range serie.Values {

			tradeId, err := networking.ConvertJsonValueToInt64(record[1])
			if err != nil {
				logger.WithFields(logrus.Fields{
					"error":  err,
					"market": market,
				}).Error("getMissingTradeIds: networking.ConvertJsonValueToInt64")
				continue
			}

			if prevId != 0 && prevId+1 != tradeId {

				if _, ok := missingTradeIds[market]; !ok {
					missingTradeIds[market] = make(map[int64]struct{})
				}

				for i := prevId + 1; i < tradeId; i++ {
					missingTradeIds[market][i] = struct{}{}
				}

			}
			prevId = tradeId
		}
	}

	return missingTradeIds
}

func updateMissingTrades(missingTradeIds map[string]map[int64]struct{},
	start, end time.Time) {

	for market, _ := range missingTradeIds {

		go func(market string) {

			var th publicapi.TradeHistory

			request := func() (err error) {
				th, err = publicClient.GetTradeHistory(market, start, end)
				return err
			}

			success := networking.ExecuteRequest(&networking.RequestInfo{
				Logger:   logger,
				Period:   0,
				ErrorMsg: "updateMissingTrades: publicClient.GetTradeHistory",
				Request:  request,
			})

			if !success {
				return
			}

			mts := make([]*publicapi.Trade, 0, len(missingTradeIds[market]))
			for _, trade := range th {
				if _, ok := missingTradeIds[market][trade.TradeId]; ok {
					mts = append(mts, trade)
				}
			}

			prepareMissingTradePoints(market, mts)

		}(market)
	}
}

func prepareMissingTradePoints(market string, mt []*publicapi.Trade) {

	measurement := conf.Schema["trades_measurement"]
	points := make([]*ifxClient.Point, 0, len(mt))

	for _, trade := range mt {

		tags := map[string]string{
			"source":     "publicapi",
			"order_type": trade.TypeOrder,
			"market":     market,
		}

		fields := map[string]interface{}{
			"trade_id": trade.TradeId,
			"rate":     trade.Rate,
			"quantity": trade.Amount,
			"total":    trade.Total,
		}

		timestamp := time.Unix(trade.Date, trade.TradeId%1000000000)

		pt, err := ifxClient.NewPoint(measurement, tags, fields, timestamp)
		if err != nil {
			logger.WithField("error", err).Error(
				"prepareMissingTradePoints: ifxClient.NewPoint")
			continue
		}
		points = append(points, pt)
	}

	batchsToWrite <- &database.BatchPoints{
		TypePoint: "missingTrade",
		Points:    points,
	}
}
