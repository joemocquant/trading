package bittrex

import (
	"time"
	"trading/api/bittrex/publicapi"
	"trading/ingestion"

	influxDBClient "github.com/influxdata/influxdb/client/v2"
)

func ingestMarketSummaries() {

	for {
		marketSummaries, err := publicClient.GetMarketSummaries()

		for err != nil {
			logger.WithField("error", err).Error("ingestMarketSummaries: publicClient.GetMarketSummaries")
			time.Sleep(5 * time.Second)
			marketSummaries, err = publicClient.GetMarketSummaries()
		}

		points := make([]*influxDBClient.Point, 0, len(marketSummaries))
		for _, marketSummary := range marketSummaries {
			pt, err := prepareMarketSummaryPoint(marketSummary)
			if err != nil {
				logger.WithField("error", err).Error("ingestMarketSummaries: prepareMarketSummaryPoint")
				continue
			}
			points = append(points, pt)
		}

		batchsToWrite <- &ingestion.BatchPoints{"marketSummary", points}

		<-time.After(time.Duration(conf.MarketSummariesCheckPeriodSec) * time.Second)
	}
}

func prepareMarketSummaryPoint(ms *publicapi.MarketSummary) (*influxDBClient.Point, error) {

	measurement := conf.Schema["market_summaries_measurement"]
	timestamp := time.Unix(ms.TimeStamp, 0)

	tags := map[string]string{
		"source": "publicapi",
		"market": ms.MarketName,
	}

	fields := map[string]interface{}{
		"market_name":      ms.MarketName,
		"high":             ms.High,
		"low":              ms.Low,
		"volume":           ms.Volume,
		"last":             ms.Last,
		"base_volume":      ms.BaseVolume,
		"timestamp":        ms.TimeStamp,
		"bid":              ms.Bid,
		"ask":              ms.Ask,
		"open_buy_orders":  ms.OpenBuyOrders,
		"open_sell_orders": ms.OpenSellOrders,
		"prev_day":         ms.PrevDay,
		"created":          ms.Created,
	}

	pt, err := influxDBClient.NewPoint(measurement, tags, fields, timestamp)
	if err != nil {
		return nil, err
	}
	return pt, nil
}
