package metrics

import (
	"fmt"
	"strconv"
	"time"
	"trading/networking"
	"trading/networking/database"

	"github.com/Sirupsen/logrus"
	ifxClient "github.com/influxdata/influxdb/client/v2"
)

type orderBooks map[string]*orderBook

type orderBook struct {
	sequence int64
	asks     []*order
	bids     []*order
}

type order struct {
	rate          float64
	quantity      float64
	total         float64
	cumulativeSum float64
	orderType     string
}

type marketDepths map[string]*marketDepth

type marketDepth struct {
	bidsDepth map[float64]float64
	asksDepth map[float64]float64
}

func computeMarketDepths() {

	f, err := time.ParseDuration(conf.Metrics.MarketDepths.Frequency)
	if err != nil {
		logger.WithField("error", err).Fatal(
			"computeMarketDepths: time.ParseDuration")
	}

	computeMarketDepthsBittrex(&indicator{
		period:   f,
		exchange: "bittrex",
	})

	computeMarketDepthsPoloniex(&indicator{
		period:   f,
		exchange: "poloniex",
	})
}

func computeMarketDepthsBittrex(ind *indicator) {

	ind.dataSource = conf.Metrics.Sources[ind.exchange]

	go networking.RunEvery(ind.period, func(nextRun int64) {

		ind.nextRun = nextRun

		obs := getLastOrderBooksBittrex(ind, nil)
		// printOrderBook(obs["BTC-STRAT"], 10)
		mds := getMarketDepths(obs)
		prepareMarketDepthsPoints(ind, mds)
	})
}

func computeMarketDepthsPoloniex(ind *indicator) {

	ind.dataSource = conf.Metrics.Sources[ind.exchange]

	var obs orderBooks
	i := 0

	go networking.RunEvery(ind.period, func(nextRun int64) {

		ind.nextRun = nextRun

		if i%conf.Metrics.MarketDepths.PoloniexHardFetchFrequency == 0 {
			obs = getLastOrderBooksPoloniex(ind, nil)
			i = 0
		}
		i++

		bookUpdates := getBookUpdates(obs, ind)
		mergeOrderBooksWithBookUpdates(obs, bookUpdates)

		// printOrderBook(obs["BTC_STRAT"], 10)
		// getLossStatus(obs["BTC_STRAT"].sequence, bookUpdates["BTC_STRAT"])

		mds := getMarketDepths(obs)
		prepareMarketDepthsPoints(ind, mds)
	})

}

func formatOrderBooks(res []ifxClient.Result) orderBooks {

	obs := make(orderBooks)

	for _, result := range res {
		for _, serie := range result.Series {

			market := serie.Tags["market"]
			ob := orderBook{}

			for _, o := range serie.Values {

				rate, err := networking.ConvertJsonValueToFloat64(o[1])
				if err != nil {
					logger.WithFields(logrus.Fields{
						"error":  err,
						"market": market,
					}).Error("formatOrderBooks: networking.ConvertJsonValueToFloat64")
					continue
				}

				quantity, err := networking.ConvertJsonValueToFloat64(o[2])
				if err != nil {
					logger.WithFields(logrus.Fields{
						"error":  err,
						"market": market,
					}).Error("formatOrderBooks: networking.ConvertJsonValueToFloat64")
					continue
				}

				total, err := networking.ConvertJsonValueToFloat64(o[3])
				if err != nil {
					logger.WithFields(logrus.Fields{
						"error":  err,
						"market": market,
					}).Error("formatOrderBooks: networking.ConvertJsonValueToFloat64")
					continue
				}

				cumulativeSum, err := networking.ConvertJsonValueToFloat64(o[4])
				if err != nil {
					logger.WithFields(logrus.Fields{
						"error":  err,
						"market": market,
					}).Error("formatOrderBooks: networking.ConvertJsonValueToFloat64")
					continue
				}

				orderType, err := networking.ConvertJsonValueToString(o[5])
				if err != nil {
					logger.WithFields(logrus.Fields{
						"error":  err,
						"market": market,
					}).Error("formatOrderBooks: networking.ConvertJsonValueToString")
					continue
				}

				order := &order{
					rate:          rate,
					quantity:      quantity,
					total:         total,
					cumulativeSum: cumulativeSum,
					orderType:     orderType,
				}

				if orderType == "ask" {
					ob.asks = append(ob.asks, order)
				} else {
					ob.bids = append(ob.bids, order)
				}
			}

			if len(ob.bids) != 0 && len(ob.asks) != 0 {
				obs[market] = &ob
			}
		}
	}

	return obs
}

func getMarketDepths(obs orderBooks) marketDepths {

	intervals := conf.Metrics.MarketDepths.Intervals

	mds := make(marketDepths, len(obs))

	for market, _ := range obs {

		md := marketDepth{make(map[float64]float64, len(intervals)),
			make(map[float64]float64, len(intervals)),
		}
		mds[market] = &md
	}

	for market, ob := range obs {

		baseRate := (ob.bids[0].rate + ob.asks[0].rate) / 2
		cs := 0.0
		lastI := 0

		for _, interval := range intervals {

			bound := baseRate - (baseRate / 100 * interval)

			for i, bid := range ob.bids[lastI:] {

				if bid.rate < bound {
					mds[market].bidsDepth[interval] = cs
					lastI = i
					break
				}
				cs = bid.cumulativeSum
			}

			if cs == ob.bids[len(ob.bids)-1].cumulativeSum {
				break
			}
		}
	}

	for market, ob := range obs {

		baseRate := (ob.bids[0].rate + ob.asks[0].rate) / 2
		cs := 0.0
		lastI := 0

		for _, interval := range intervals {

			bound := baseRate + (baseRate / 100 * interval)

			for i, ask := range ob.asks[lastI:] {

				if ask.rate > bound {
					mds[market].asksDepth[interval] = cs
					lastI = i
					break
				}
				cs = ask.cumulativeSum
			}

			if cs == ob.asks[len(ob.asks)-1].cumulativeSum {
				break
			}
		}
	}

	return mds
}

func prepareMarketDepthsPoints(ind *indicator, mds marketDepths) {

	measurement := conf.Metrics.Schema["market_depths_measurement"]
	timestamp := time.Unix(0, ind.nextRun)
	points := make([]*ifxClient.Point, 0, len(mds)*2)

	for market, md := range mds {

		tags := map[string]string{
			"market":   market,
			"exchange": ind.exchange,
		}

		for _, interval := range conf.Metrics.MarketDepths.Intervals {

			tags["interval"] = strconv.FormatFloat(interval, 'f', 2, 64)

			fields := map[string]interface{}{
				"bid_depth": md.bidsDepth[interval],
				"ask_depth": md.asksDepth[interval],
			}

			pt, err := ifxClient.NewPoint(measurement, tags, fields, timestamp)
			if err != nil {
				logger.WithField("error", err).Error(
					"prepareMarketDepthsPoints: ifxClient.NewPoint")
			}
			points = append(points, pt)
		}
	}

	if len(points) == 0 {
		return
	}

	batchsToWrite <- &database.BatchPoints{
		TypePoint: ind.dataSource.Schema["database"] + "MarketDepth",
		Points:    points,
	}
}

func printOrderBook(orderBook *orderBook, limit int) {

	line := "\nBids\t\t\t\tAsks\n"
	line += "Rate\t\tAmount\t\tRate\t\tAmount\n"

	for i := 0; (i < len(orderBook.bids) || i < len(orderBook.asks)) &&
		i < limit; i++ {

		if i < len(orderBook.bids) {
			line += fmt.Sprintf("%.8f\t%.8f",
				orderBook.bids[i].rate, orderBook.bids[i].quantity)
		} else {
			line += "\t\t\t"
		}

		if i < len(orderBook.asks) {
			line += fmt.Sprintf("\t%.8f\t%.8f",
				orderBook.asks[i].rate, orderBook.asks[i].quantity)
		}
		line += "\n"
	}

	logger.Warn(line)
}
