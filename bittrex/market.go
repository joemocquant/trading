package bittrex

import (
	"time"
	"trading/api/bittrex/publicapi"
)

func checkMarkets() {

	for {

		markets, err := publicClient.GetMarkets()

		for err != nil {
			logger.WithField("error", err).Error(
				"ingestNewMarkets: publicClient.GetMarkets")

			time.Sleep(5 * time.Second)
			markets, err = publicClient.GetMarkets()
		}

		setMarkets(markets)

		<-time.After(time.Duration(conf.MarketsCheckPeriodMin) * time.Minute)
	}
}

func getActiveMarketNames() []string {

	ams.Lock()
	defer ams.Unlock()

	res := make([]string, 0, len(ams.markets))

	for name, market := range ams.markets {

		if market.IsActive {
			res = append(res, name)
		}
	}

	return res
}

func setMarkets(markets []*publicapi.Market) {

	ams.Lock()
	defer ams.Unlock()

	for _, market := range markets {
		ams.markets[market.MarketName] = market
	}
}
