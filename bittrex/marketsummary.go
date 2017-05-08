package bittrex

import (
	"time"
	"trading/api/bittrex/publicapi"
	"trading/database"

	ifxClient "github.com/influxdata/influxdb/client/v2"
)

func ingestMarketSummaries() {

	period := time.Duration(conf.MarketSummariesCheckPeriodSec) * time.Second

	for {
		marketSummaries, err := publicClient.GetMarketSummaries()

		for err != nil {
			logger.WithField("error", err).Error(
				"ingestMarketSummaries: publicClient.GetMarketSummaries")

			time.Sleep(5 * time.Second)
			marketSummaries, err = publicClient.GetMarketSummaries()
		}

		points := make([]*ifxClient.Point, 0, len(marketSummaries))

		for _, marketSummary := range marketSummaries {

			pt, err := prepareMarketSummaryPoint(marketSummary)
			if err != nil {
				logger.WithField("error", err).Error(
					"ingestMarketSummaries: prepareMarketSummaryPoint")
				continue
			}
			points = append(points, pt)
		}

		batchsToWrite <- &database.BatchPoints{"marketSummary", points}

		<-time.After(period)
	}
}

func prepareMarketSummaryPoint(
	ms *publicapi.MarketSummary) (*ifxClient.Point, error) {

	measurement := conf.Schema["market_summaries_measurement"]
	timestamp := time.Unix(ms.TimeStamp, 0)

	tags := map[string]string{
		"source": "publicapi",
		"market": ms.MarketName,
	}

	fields := map[string]interface{}{
		"high":             ms.High,
		"low":              ms.Low,
		"volume":           ms.Volume,
		"last":             ms.Last,
		"base_volume":      ms.BaseVolume,
		"bid":              ms.Bid,
		"ask":              ms.Ask,
		"open_buy_orders":  ms.OpenBuyOrders,
		"open_sell_orders": ms.OpenSellOrders,
		"prev_day":         ms.PrevDay,
		"created":          ms.Created,
	}

	pt, err := ifxClient.NewPoint(measurement, tags, fields, timestamp)
	if err != nil {
		return nil, err
	}
	return pt, nil
}
