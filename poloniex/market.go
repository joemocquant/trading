package poloniex

import (
	"time"
	"trading/api/poloniex/publicapi"
	"trading/api/poloniex/pushapi"
	"trading/networking"
	"trading/networking/database"

	ifxClient "github.com/influxdata/influxdb/client/v2"
)

func ingestMarkets() {

	// checking new markets periodically
	for {
		go ingestNewMarkets()
		<-time.After(time.Duration(conf.MarketCheckPeriodMin) * time.Minute)
	}
}

func ingestNewMarkets() {

	var tickers publicapi.Ticks

	request := func() (err error) {
		tickers, err = publicClient.GetTickers()
		return err
	}

	success := networking.ExecuteRequest(&networking.RequestInfo{
		Logger:   logger,
		Period:   time.Duration(conf.MarketCheckPeriodMin) * time.Minute,
		ErrorMsg: "ingestNewMarkets: publicClient.GetTickers",
		Request:  request,
	})

	if !success {
		return
	}

	newMarkets := make([]string, 0, len(tickers))

	updaters.RLock()
	for market, _ := range tickers {
		if _, ok := updaters.mus[market]; !ok {
			newMarkets = append(newMarkets, market)
		}
	}
	updaters.RUnlock()

	if len(newMarkets) > 0 {
		logger.WithField("newMarkets", newMarkets).Infof(
			"Ingesting %d new markets", len(newMarkets))
	}

	for _, market := range newMarkets {

		go func(market string) {
			var marketUpdater pushapi.MarketUpdater

			request := func() (err error) {
				marketUpdater, err = pushClient.SubscribeMarket(market)
				return err
			}

			success := networking.ExecuteRequest(&networking.RequestInfo{
				Logger:   logger.WithField("market", market),
				Period:   time.Duration(conf.MarketCheckPeriodMin) * time.Minute,
				ErrorMsg: "ingestNewMarkets: pushClient.SubscribeMarket",
				Request:  request,
			})

			if !success {
				return
			}

			updaters.Lock()
			updaters.mus[market] = marketUpdater
			updaters.Unlock()

			go getMarketNewPoints(marketUpdater, market)
		}(market)
	}
}

func getMarketNewPoints(marketUpdater pushapi.MarketUpdater, market string) {

	for {
		marketUpdates := <-marketUpdater

		go func(marketUpdates *pushapi.MarketUpdates) {

			points := make([]*ifxClient.Point, 0, len(marketUpdates.Updates))

			for _, marketUpdate := range marketUpdates.Updates {

				pt, err := prepareMarketPoint(
					marketUpdate, market, marketUpdates.Sequence)

				if err != nil {
					logger.WithField("error", err).Error(
						"getMarketNewPoints: prepareMarketPoint")
					continue
				}

				points = append(points, pt)
			}
			batchsToWrite <- &database.BatchPoints{"market", points}

		}(marketUpdates)

	}
}

func prepareMarketPoint(marketUpdate *pushapi.MarketUpdate,
	market string, sequence int64) (*ifxClient.Point, error) {

	tags := make(map[string]string, 3)
	fields := make(map[string]interface{}, 3)
	var measurement string
	var timestamp time.Time

	switch marketUpdate.TypeUpdate {

	case "orderBookModify":

		obm := marketUpdate.Data.(*pushapi.OrderBookModify)

		tags = map[string]string{
			"source":     "pushapi",
			"order_type": obm.TypeOrder,
			"market":     market,
		}
		fields = map[string]interface{}{
			"sequence": sequence,
			"rate":     obm.Rate,
			"quantity": obm.Amount,
			"total":    obm.Rate * obm.Amount,
		}
		measurement = conf.Schema["book_updates_measurement"]
		timestamp = time.Now()

	case "orderBookRemove":

		obr := marketUpdate.Data.(*pushapi.OrderBookRemove)

		tags = map[string]string{
			"source":     "pushapi",
			"order_type": obr.TypeOrder,
			"market":     market,
		}
		fields = map[string]interface{}{
			"sequence": sequence,
			"rate":     obr.Rate,
			"quantity": 0.0,
			"total":    0.0,
		}
		measurement = conf.Schema["book_updates_measurement"]
		timestamp = time.Now()

	case "newTrade":

		nt := marketUpdate.Data.(*pushapi.NewTrade)

		tags = map[string]string{
			"source":     "pushapi",
			"order_type": nt.TypeOrder,
			"market":     market,
		}
		fields = map[string]interface{}{
			"sequence": sequence,
			"trade_id": nt.TradeId,
			"rate":     nt.Rate,
			"quantity": nt.Amount,
			"total":    nt.Total,
		}
		measurement = conf.Schema["trade_updates_measurement"]
		timestamp = time.Unix(nt.Date, nt.TradeId%1000000000)
	}

	pt, err := ifxClient.NewPoint(measurement, tags, fields, timestamp)
	if err != nil {
		return nil, err
	}
	return pt, nil
}
