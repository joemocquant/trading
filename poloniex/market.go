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
		log.WithField("error", err).Error("ingestion.ingestNewMarkets: publicClient.GetTickers")
		time.Sleep(5 * time.Second)
		tickers, err = publicClient.GetTickers()
	}

	for currencyPair, _ := range tickers {

		if _, ok := marketUpdaters[currencyPair]; !ok {

			marketUpdater, err := pushClient.SubscribeMarket(currencyPair)

			if err != nil {

				log.WithFields(log.Fields{
					"currencyPair": currencyPair,
					"error":        err,
				}).Error("ingestion.ingestNewMarkets: pushClient.SubscribeMarket")
				continue
			}

			log.Infof("Subscribed to: %s", currencyPair)

			marketUpdaters[currencyPair] = marketUpdater
			go getMarketNewPoints(marketUpdater, currencyPair)
		}
	}
}

func getMarketNewPoints(marketUpdater pushapi.MarketUpdater, currencyPair string) {

	for {
		marketUpdates := <-marketUpdater

		go func(marketUpdates *pushapi.MarketUpdates) {

			for _, marketUpdate := range marketUpdates.Updates {

				pt, err := prepareMarketPoint(marketUpdate, currencyPair, marketUpdates.Sequence)
				if err != nil {
					log.WithField("error", err).Error("ingestion.getMarketNewPoints: ingestion.prepareMarketPoint")
					continue
				}

				pointsToWrite <- pt
			}

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

		obm := marketUpdate.Data.(pushapi.OrderBookModify)

		tags = map[string]string{
			"order_type": obm.TypeOrder,
			"market":     currencyPair,
		}
		fields = map[string]interface{}{
			"sequence": sequence,
			"rate":     obm.Rate,
			"amount":   obm.Amount,
		}
		measurement = conf.Ingestion.Schema["book_updates_measurement"]
		timestamp = time.Now()

	case "orderBookRemove":

		obr := marketUpdate.Data.(pushapi.OrderBookRemove)

		tags = map[string]string{
			"order_type": obr.TypeOrder,
			"market":     currencyPair,
		}
		fields = map[string]interface{}{
			"sequence": sequence,
			"rate":     obr.Rate,
			"amount":   0.0,
		}
		measurement = conf.Ingestion.Schema["book_updates_measurement"]
		timestamp = time.Now()

	case "newTrade":

		nt := marketUpdate.Data.(pushapi.NewTrade)

		tags = map[string]string{
			"order_type": nt.TypeOrder,
			"market":     currencyPair,
		}
		fields = map[string]interface{}{
			"sequence": sequence,
			"rate":     nt.Rate,
			"amount":   nt.Amount,
		}
		measurement = conf.Ingestion.Schema["trade_updates_measurement"]

		nano := time.Now().UnixNano() % int64(time.Second)
		timestamp = time.Unix(nt.Date, nano)
	}

	pt, err := influxDBClient.NewPoint(measurement, tags, fields, timestamp)
	if err != nil {
		return nil, err
	}
	return pt, nil
}

func flushMarketPoints(number int) {

	bp, err := influxDBClient.NewBatchPoints(influxDBClient.BatchPointsConfig{
		Database:  conf.Ingestion.Schema["database"],
		Precision: "ns",
	})
	if err != nil {
		log.WithField("error", err).Error("ingestion.flushMarketPoints: dbClient.NewBatchPoints")
		return
	}

	for i := 0; i < number; i++ {
		bp.AddPoint(<-pointsToWrite)
	}

	if err := dbClient.Write(bp); err != nil {
		log.WithFields(log.Fields{
			"batchPoints": bp,
			"error":       err,
		}).Error("ingestion.flushMarketPoints: ingestion.dbClient.Write")
	}
}
