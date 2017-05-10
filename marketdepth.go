package metrics

import (
	"encoding/json"
	"fmt"
	"strconv"
	"time"
	"trading/networking"
	"trading/networking/database"

	ifxClient "github.com/influxdata/influxdb/client/v2"
)

type orderBooks map[string]*orderBook

type orderBook struct {
	asks []*order
	bids []*order
}

type order struct {
	rate          float64
	cumulativeSum float64
}

type marketDepths map[string]*marketDepth

type marketDepth struct {
	bidsDepth map[float64]float64
	asksDepth map[float64]float64
}

type getLastOrderBooks func(map[string]time.Time) orderBooks

func computeMarketDepth() {

	cmd := func(period time.Duration, globs getLastOrderBooks, exchange string) {

		previousStarts := make(map[string]time.Time)

		for {
			obs := globs(previousStarts)
			mds := getMarketDepths(obs)
			prepareMarketDepthsPoints(mds, previousStarts, exchange)

			<-time.After(period)
		}
	}

	p := time.Duration(conf.Poloniex.MarketDepthPeriodCheckSec) * time.Second
	go cmd(p, getLastOrderBooksPoloniex, "poloniex")

	p = time.Duration(conf.Bittrex.MarketDepthPeriodCheckSec) * time.Second
	go cmd(p, getLastOrderBooksBittrex, "bittrex")
}

func getLastOrderBooksPoloniex(
	previousStarts map[string]time.Time) orderBooks {

	query := fmt.Sprintf("SELECT ask_depth FROM %s ORDER BY time DESC LIMIT 1",
		conf.Poloniex.Schema["book_orders_last_check_measurement"])

	var res []ifxClient.Result

	request := func() (err error) {
		res, err = database.QueryDB(
			dbClient, query, conf.Poloniex.Schema["database"])
		return err
	}

	success := networking.ExecuteRequest(&networking.RequestInfo{
		Logger:   logger.WithField("query", query),
		Period:   time.Duration(conf.Poloniex.MarketDepthPeriodCheckSec) * time.Second,
		ErrorMsg: "getLastOrderBooksPoloniex: database.QueryDB",
		Request:  request,
	})

	if !success {
		return nil
	}

	if len(res[0].Series) == 0 || len(res[0].Series[0].Values) != 1 {
		logger.Info("getLastOrderBooksPoloniex: no orderBooks found")
		return nil
	}

	start, err := time.Parse(
		time.RFC3339, res[0].Series[0].Values[0][0].(string))

	if err != nil {
		logger.WithField("error", err).Error(
			"getLastOrderBooksPoloniex: time.Parse")
		return nil
	}

	prevousStart, ok := previousStarts["allMarkets"]

	if ok && prevousStart.Unix() == start.Unix() {
		return nil
	} else {
		previousStarts["allMarkets"] = start
	}

	end := start.Add(1 * time.Second)

	query = fmt.Sprintf(
		`SELECT rate, cumulative_sum, order_type FROM %s
    WHERE time >= %d AND time < %d GROUP BY market`,
		conf.Poloniex.Schema["book_orders_measurement"],
		start.UnixNano(), end.UnixNano())

	success = networking.ExecuteRequest(&networking.RequestInfo{
		Logger:   logger.WithField("query", query),
		Period:   0,
		ErrorMsg: "getLastOrderBooksPoloniex: database.QueryDB",
		Request:  request,
	})

	if !success {
		return nil
	}

	return formatOrderBooks(res)
}

func getLastOrderBooksBittrex(previousStarts map[string]time.Time) orderBooks {

	query := fmt.Sprintf(`SELECT ask_depth FROM %s
    GROUP BY market ORDER BY time DESC LIMIT 1`,
		conf.Bittrex.Schema["book_orders_last_check_measurement"])

	var res []ifxClient.Result

	request := func() (err error) {
		res, err = database.QueryDB(
			dbClient, query, conf.Bittrex.Schema["database"])
		return err
	}

	success := networking.ExecuteRequest(&networking.RequestInfo{
		Logger:   logger.WithField("query", query),
		Period:   time.Duration(conf.Bittrex.MarketDepthPeriodCheckSec) * time.Second,
		ErrorMsg: "getLastOrderBooksBittrex: database.QueryDB",
		Request:  request,
	})

	if !success {
		return nil
	}

	if len(res[0].Series) == 0 {
		logger.Info("getLastOrderBooksBittrex: no orderBooks found")
		return nil
	}

	query = ""
	for _, serie := range res[0].Series {

		market := serie.Tags["market"]

		start, err := time.Parse(
			time.RFC3339, serie.Values[0][0].(string))

		if err != nil {
			logger.WithField("error", err).Error(
				"getLastOrderBooksBittrex: time.Parse")
			continue
		}

		prevousStart, ok := previousStarts[market]
		if ok && prevousStart.Unix() == start.Unix() {
			continue
		} else {
			previousStarts[market] = start
			start := previousStarts[market]
			end := start.Add(1 * time.Second)

			query += fmt.Sprintf(
				`SELECT rate, cumulative_sum, order_type FROM %s
      WHERE market = '%s' AND time >= %d AND time < %d GROUP BY market;`,
				conf.Bittrex.Schema["book_orders_measurement"],
				market, start.UnixNano(), end.UnixNano())
		}
	}

	if query == "" {
		return nil
	}

	success = networking.ExecuteRequest(&networking.RequestInfo{
		Logger:   logger.WithField("query", query),
		Period:   0,
		ErrorMsg: "getLastOrderBooksBittrex: database.QueryDB",
		Request:  request,
	})

	if !success {
		return nil
	}

	obs := formatOrderBooks(res)
	return obs
}

func formatOrderBooks(res []ifxClient.Result) orderBooks {

	obs := make(orderBooks)

	for _, result := range res {
		for _, serie := range result.Series {

			market := serie.Tags["market"]
			ob := orderBook{}

			for _, o := range serie.Values {

				rate, err := o[1].(json.Number).Float64()

				if err != nil {
					logger.Errorf("formatOrderBooks: Wrong rate type: %v (%s)",
						rate, market)
					continue
				}

				cumulativeSum, err := o[2].(json.Number).Float64()

				if err != nil {
					logger.Errorf("formatOrderBooks: Wrong cumulativeSum type: %v (%s)",
						cumulativeSum, market)
					continue
				}

				orderType := o[3].(string)

				if orderType == "ask" {
					ob.asks = append(ob.asks, &order{rate, cumulativeSum})
				} else {
					ob.bids = append(ob.bids, &order{rate, cumulativeSum})
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

	intervals := conf.MarketDepthIntervals

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

func prepareMarketDepthsPoints(mds marketDepths,
	previousStarts map[string]time.Time, exchange string) {

	measurement := conf.Schema["market_depths_measurement"]

	for market, md := range mds {

		baseTimestamp, ok := previousStarts["allMarkets"]
		if !ok {
			baseTimestamp = previousStarts[market]
		}

		points := make([]*ifxClient.Point, 0, len(mds)*2)

		tags := map[string]string{
			"market":   market,
			"exchange": exchange,
		}

		for _, interval := range conf.MarketDepthIntervals {

			tags["interval"] = strconv.FormatFloat(interval, 'f', 2, 64)

			fields := map[string]interface{}{
				"bid_depth": md.bidsDepth[interval],
				"ask_depth": md.asksDepth[interval],
			}

			pt, err := ifxClient.NewPoint(measurement, tags, fields, baseTimestamp)
			if err != nil {
				logger.WithField("error", err).Error(
					"prepareMarketDepthsPoints: ifxClient.NewPoint")
			}
			points = append(points, pt)
		}

		batchsToWrite <- &database.BatchPoints{
			exchange + "MarketDepth",
			points,
		}
	}
}
