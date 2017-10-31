package bittrex

import (
	"time"
	"trading/networking"
	"trading/networking/database"

	ifxClient "github.com/influxdata/influxdb/client/v2"
	publicapi "github.com/joemocquant/bittrex-api/publicapi"
)

func ingestMarketSummaries() {

	period := time.Duration(conf.MarketSummariesCheckPeriodSec) * time.Second

	for {

		go func() {
			var marketSummaries publicapi.MarketSummaries

			request := func() (err error) {
				marketSummaries, err = publicClient.GetMarketSummaries()
				return err
			}

			success := networking.ExecuteRequest(&networking.RequestInfo{
				Logger:   logger,
				Period:   period,
				ErrorMsg: "ingestMarketSummaries: publicClient.GetMarketSummaries",
				Request:  request,
			})

			if !success {
				return
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

			batchsToWrite <- &database.BatchPoints{
				TypePoint: "marketSummary",
				Points:    points,
			}
		}()

		<-time.After(period)
	}
}

func prepareMarketSummaryPoint(
	ms *publicapi.MarketSummary) (*ifxClient.Point, error) {

	measurement := conf.Schema["ticks_measurement"]
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
