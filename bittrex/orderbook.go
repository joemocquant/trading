package bittrex

import (
	"time"
	"trading/api/bittrex/publicapi"
	"trading/networking"
	"trading/networking/database"

	ifxClient "github.com/influxdata/influxdb/client/v2"
)

func ingestOrderBooks() {

	for {

		markets := getActiveMarketNames()

		for _, market := range markets {
			go ingestOrderBook(market)
		}

		<-time.After(time.Duration(conf.OrderBooksCheckPeriodSec) * time.Second)
	}
}

func ingestOrderBook(market string) {

	var orderBook *publicapi.OrderBook

	request := func() (err error) {
		orderBook, err = publicClient.GetOrderBook(market, "both")
		return err
	}

	success := networking.ExecuteRequest(&networking.RequestInfo{
		Logger:   logger,
		Period:   time.Duration(conf.OrderBooksCheckPeriodSec) * time.Second,
		ErrorMsg: "ingestOrderBook: publicClient.GetOrderBook",
		Request:  request,
	})

	if !success {
		return
	}

	baseTimestamp := time.Now().Unix()
	prepareOrderBookPoints(market, orderBook, baseTimestamp)
	prepareLastOrderBookCheckPoint(market, orderBook, baseTimestamp)
}

func prepareOrderBookPoints(market string, orderBook *publicapi.OrderBook,
	baseTimestamp int64) {

	measurement := conf.Schema["book_orders_measurement"]
	index := 0

	size := len(orderBook.Sell) + len(orderBook.Buy)
	points := make([]*ifxClient.Point, 0, size)

	processOrderBookPoints := func(typeOrder string, orders []*publicapi.Order) {

		tags := map[string]string{
			"source":     "publicapi",
			"order_type": typeOrder,
			"market":     market,
		}

		cumulativeSum := 0.0
		for _, order := range orders {

			cumulativeSum += order.Rate * order.Quantity

			fields := map[string]interface{}{
				"rate":           order.Rate,
				"quantity":       order.Quantity,
				"total":          order.Rate * order.Quantity,
				"cumulative_sum": cumulativeSum,
			}

			timestamp := time.Unix(int64(baseTimestamp), int64(index))
			index++

			pt, err := ifxClient.NewPoint(measurement, tags, fields, timestamp)
			if err != nil {
				logger.WithField("error", err).Error(
					"prepareOrderBookPoints: ifxClient.NewPoint")
				continue
			}
			points = append(points, pt)
		}
	}

	processOrderBookPoints("ask", orderBook.Sell)
	processOrderBookPoints("bid", orderBook.Buy)
	batchsToWrite <- &database.BatchPoints{"orderBook", points}
}

func prepareLastOrderBookCheckPoint(market string,
	orderBook *publicapi.OrderBook, baseTimestamp int64) {

	measurement := conf.Schema["book_orders_last_check_measurement"]
	timestamp := time.Unix(int64(baseTimestamp), 0)

	tags := map[string]string{
		"source": "publicapi",
		"market": market,
	}

	fields := map[string]interface{}{
		"bid_depth": len(orderBook.Buy),
		"ask_depth": len(orderBook.Sell),
	}

	point, err := ifxClient.NewPoint(measurement, tags, fields, timestamp)
	if err != nil {
		logger.WithField("error", err).Error(
			"prepareLastOrderBookCheckPoint: ifxClient.NewPoint")
	}

	batchsToWrite <- &database.BatchPoints{
		"orderBookLastCheck",
		[]*ifxClient.Point{point},
	}
}
