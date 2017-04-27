package bittrex

import (
	"time"
	"trading/api/bittrex/publicapi"
)

func checkMarkets() {

	for {

		markets, err := publicClient.GetMarkets()

		for err != nil {

			logger.WithField("error", err).Error("ingestNewMarkets: publicClient.GetMarkets")
			time.Sleep(5 * time.Second)
			markets, err = publicClient.GetMarkets()
		}

		setMarkets(markets)

		<-time.After(time.Duration(conf.MarketsCheckPeriodMin) * time.Minute)
	}
}

func getMarkets() []string {

	am.Lock()
	defer am.Unlock()

	res := make([]string, 0, len(am.markets))

	for name, market := range am.markets {

		if market.IsActive {
			res = append(res, name)
		}
	}

	return res
}

func setMarkets(markets []*publicapi.Market) {

	am.Lock()
	defer am.Unlock()

	for _, market := range markets {
		am.markets[market.MarketName] = market
	}
}
