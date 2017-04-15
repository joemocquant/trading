package poloniex

import (
	"time"
	"trading/poloniex/pushapi"

	influxDBClient "github.com/influxdata/influxdb/client/v2"
	log "github.com/sirupsen/logrus"
)

func ingestTicks() {

	ticker, err := pushClient.SubscribeTicker()

	for err != nil {
		log.WithField("error", err).Error("ingestion.ingestTickers: pushClient.SubscribeTicker")
		time.Sleep(5 * time.Second)
		ticker, err = pushClient.SubscribeTicker()
	}

	log.Infof("Subscribed to ticker")

	go getNewTicks(ticker)
}

func getNewTicks(ticker pushapi.Ticker) {

	for {
		tick := <-ticker

		go func(tick *pushapi.Tick) {

			pt, err := prepareTickPoint(tick)
			if err != nil {
				log.WithField("error", err).Error("ingestion.getNewTicks: ingestion.prepareTickPoint")
				return
			}
			pointsToWrite <- &batchPoints{"ticks", []*influxDBClient.Point{pt}}

		}(tick)

	}
}

func prepareTickPoint(tick *pushapi.Tick) (*influxDBClient.Point, error) {

	measurement := conf.Ingestion.Schema["ticks_measurement"]
	timestamp := time.Now()

	tags := map[string]string{
		"market": tick.CurrencyPair,
	}

	fields := map[string]interface{}{
		"market":         tick.CurrencyPair,
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

	pt, err := influxDBClient.NewPoint(measurement, tags, fields, timestamp)
	if err != nil {
		return nil, err
	}
	return pt, nil
}
