package poloniex

import (
	"time"
	"trading/poloniex/pushapi"

	influxDBClient "github.com/influxdata/influxdb/client/v2"
	log "github.com/sirupsen/logrus"
)

func ingestNewMarkets() {

	tickers, err := publicClient.GetTickers()

	for err != nil {
		log.WithField("error", err).Error("poloniex.ingestNewMarkets: publicClient.GetTickers")
		time.Sleep(5 * time.Second)
		tickers, err = publicClient.GetTickers()
	}

	// Might need to filter out frozen markets

	newMarkets := make([]string, 0)
	for currencyPair, _ := range tickers {
		if _, ok := marketUpdaters[currencyPair]; !ok {
			newMarkets = append(newMarkets, currencyPair)
		}
	}

	if len(newMarkets) > 0 {
		log.WithField("newMarkets", newMarkets).Infof("Ingesting %d new markets", len(newMarkets))
	}

	for _, currencyPair := range newMarkets {

		marketUpdater, err := pushClient.SubscribeMarket(currencyPair)

		if err != nil {

			log.WithFields(log.Fields{
				"currencyPair": currencyPair,
				"error":        err,
			}).Error("poloniex.ingestNewMarkets: pushClient.SubscribeMarket")
			continue
		}

		marketUpdaters[currencyPair] = marketUpdater
		go getMarketNewPoints(marketUpdater, currencyPair)
	}
}

func getMarketNewPoints(marketUpdater pushapi.MarketUpdater, currencyPair string) {

	for {
		marketUpdates := <-marketUpdater

		go func(marketUpdates *pushapi.MarketUpdates) {

			points := make([]*influxDBClient.Point, 0, len(marketUpdates.Updates))

			for _, marketUpdate := range marketUpdates.Updates {

				pt, err := prepareMarketPoint(marketUpdate, currencyPair, marketUpdates.Sequence)
				if err != nil {
					log.WithField("error", err).Error("poloniex.getMarketNewPoints: poloniex.prepareMarketPoint")
					continue
				}

				points = append(points, pt)
			}
			pointsToWrite <- &batchPoints{"markets", points}

		}(marketUpdates)

	}
}

func prepareMarketPoint(marketUpdate *pushapi.MarketUpdate,
	currencyPair string, sequence int64) (*influxDBClient.Point, error) {

	tags := make(map[string]string)
	fields := make(map[string]interface{})
	var measurement string
	var timestamp time.Time

	switch marketUpdate.TypeUpdate {

	case "orderBookModify":

		obm := marketUpdate.Data.(*pushapi.OrderBookModify)

		tags = map[string]string{
			"source":     "pushapi",
			"order_type": obm.TypeOrder,
			"market":     currencyPair,
		}
		fields = map[string]interface{}{
			"sequence": sequence,
			"rate":     obm.Rate,
			"amount":   obm.Amount,
		}
		measurement = conf.Schema["book_updates_measurement"]
		timestamp = time.Now()

	case "orderBookRemove":

		obr := marketUpdate.Data.(*pushapi.OrderBookRemove)

		tags = map[string]string{
			"source":     "pushapi",
			"order_type": obr.TypeOrder,
			"market":     currencyPair,
		}
		fields = map[string]interface{}{
			"sequence": sequence,
			"rate":     obr.Rate,
			"amount":   0.0,
		}
		measurement = conf.Schema["book_updates_measurement"]
		timestamp = time.Now()

	case "newTrade":

		nt := marketUpdate.Data.(*pushapi.NewTrade)

		tags = map[string]string{
			"source":     "pushapi",
			"order_type": nt.TypeOrder,
			"market":     currencyPair,
		}
		fields = map[string]interface{}{
			"sequence": sequence,
			"rate":     nt.Rate,
			"amount":   nt.Amount,
		}
		measurement = conf.Schema["trade_updates_measurement"]

		nano := time.Now().UnixNano() % int64(time.Second)
		timestamp = time.Unix(nt.Date, nano)
	}

	pt, err := influxDBClient.NewPoint(measurement, tags, fields, timestamp)
	if err != nil {
		return nil, err
	}
	return pt, nil
}
