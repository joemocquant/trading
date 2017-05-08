package poloniex

import (
	"time"
	"trading/api/poloniex/publicapi"
	"trading/api/poloniex/pushapi"
	"trading/database"

	ifxClient "github.com/influxdata/influxdb/client/v2"
)

func ingestTicks() {

	go ingestPushTicks()

	for {
		go ingestPublicTicks()
		<-time.After(time.Duration(conf.PublicTicksCheckPeriodSec) * time.Second)
	}
}

func ingestPublicTicks() {

	ticks, err := publicClient.GetTickers()

	for err != nil {

		logger.WithField("error", err).Error(
			"ingestPublicTicks: publicClient.GetTickers")

		time.Sleep(5 * time.Second)
		ticks, err = publicClient.GetTickers()
	}

	points := make([]*ifxClient.Point, 0, len(ticks))

	for market, tick := range ticks {

		pt, err := preparePublicTickPoint(market, tick)

		if err != nil {
			logger.WithField("error", err).Error(
				"ingestPublicTicks: preparePublicTickPoint")
			continue
		}

		points = append(points, pt)
	}

	batchsToWrite <- &database.BatchPoints{"tick", points}
}

func preparePublicTickPoint(market string,
	tick *publicapi.Tick) (*ifxClient.Point, error) {

	measurement := conf.Schema["ticks_measurement"]
	timestamp := time.Now()

	tags := map[string]string{
		"source": "publicapi",
		"market": market,
	}

	fields := map[string]interface{}{
		"last":           tick.Last,
		"lowest_ask":     tick.LowestAsk,
		"highest_bid":    tick.HighestBid,
		"percent_change": tick.PercentChange,
		"base_volume":    tick.BaseVolume,
		"quote_volume":   tick.QuoteVolume,
		"is_frozen":      tick.IsFrozen,
		"high_24hr":      tick.High24hr,
		"low_24hr":       tick.Low24hr,
	}

	pt, err := ifxClient.NewPoint(measurement, tags, fields, timestamp)
	if err != nil {
		return nil, err
	}
	return pt, nil
}

func ingestPushTicks() {

	ticker, err := pushClient.SubscribeTicker()

	for err != nil {
		logger.WithField("error", err).Error(
			"ingestPushTicks: pushClient.SubscribeTicker")
		time.Sleep(5 * time.Second)
		ticker, err = pushClient.SubscribeTicker()
	}

	for {
		tick := <-ticker

		go func(tick *pushapi.Tick) {

			pt, err := preparePushTickPoint(tick)
			if err != nil {
				logger.WithField("error", err).Error(
					"ingestPushTicks: poloniex.prepareTickPoint")
				return
			}

			batchsToWrite <- &database.BatchPoints{
				"tick",
				[]*ifxClient.Point{pt},
			}

		}(tick)

	}
}

func preparePushTickPoint(tick *pushapi.Tick) (*ifxClient.Point, error) {

	measurement := conf.Schema["ticks_measurement"]
	timestamp := time.Now()

	tags := map[string]string{
		"source": "pushapi",
		"market": tick.CurrencyPair,
	}

	fields := map[string]interface{}{
		"last":           tick.Last,
		"lowest_ask":     tick.LowestAsk,
		"highest_bid":    tick.HighestBid,
		"percent_change": tick.PercentChange,
		"base_volume":    tick.BaseVolume,
		"quote_volume":   tick.QuoteVolume,
		"is_frozen":      tick.IsFrozen,
		"high_24hr":      tick.High24hr,
		"low_24hr":       tick.Low24hr,
	}

	pt, err := ifxClient.NewPoint(measurement, tags, fields, timestamp)
	if err != nil {
		return nil, err
	}
	return pt, nil
}
