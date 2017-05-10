package bittrex

import (
	"time"
	"trading/api/bittrex/publicapi"
	"trading/networking"
)

func checkMarkets() {

	period := time.Duration(conf.MarketsCheckPeriodMin) * time.Minute

	for {

		go func() {
			var markets publicapi.Markets

			request := func() (err error) {
				markets, err = publicClient.GetMarkets()
				return err
			}

			success := networking.ExecuteRequest(&networking.RequestInfo{
				Logger:   logger,
				Period:   period,
				ErrorMsg: "ingestNewMarkets: publicClient.GetMarkets",
				Request:  request,
			})

			if !success {
				return
			}

			setMarkets(markets)
		}()

		<-time.After(period)
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
