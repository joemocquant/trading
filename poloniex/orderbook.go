package poloniex

import (
	"time"
	"trading/api/poloniex/publicapi"
	"trading/networking"
	"trading/networking/database"

	ifxClient "github.com/influxdata/influxdb/client/v2"
)

func ingestOrderBooks() {

	depth := 100000
	period := time.Duration(conf.OrderBooksCheckPeriodSec) * time.Second

	for {

		go func() {
			var orderBooks publicapi.OrderBooks

			request := func() (err error) {
				orderBooks, err = publicClient.GetOrderBooks(depth)
				return err
			}

			success := networking.ExecuteRequest(&networking.RequestInfo{
				Logger:   logger,
				Period:   period,
				ErrorMsg: "ingestOrderBooks: publicClient.GetOrderBooks",
				Request:  request,
			})

			if !success {
				return
			}

			baseTimestamp := time.Now().Unix()
			for market, ob := range orderBooks {
				prepareOrderBookPoints(market, ob, depth, baseTimestamp)
			}

			prepareLastOrderBookCheckPoints(orderBooks, depth, baseTimestamp)
		}()

		<-time.After(period)
	}
}

func prepareOrderBookPoints(market string,
	orderBook *publicapi.OrderBook, depth int, baseTimestamp int64) {

	measurement := conf.Schema["book_orders_measurement"]
	index := 0

	size := len(orderBook.Asks) + len(orderBook.Bids)
	points := make([]*ifxClient.Point, 0, size)

	processOrderBookPoints := func(typeOrder string, orders []*publicapi.Order,
		sequence int64) {

		tags := map[string]string{
			"source":     "publicapi",
			"order_type": typeOrder,
			"market":     market,
		}

		cumulativeSum := 0.0
		for _, order := range orders {

			cumulativeSum += order.Rate * order.Quantity

			fields := map[string]interface{}{
				"sequence":       sequence,
				"rate":           order.Rate,
				"quantity":       order.Quantity,
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

	processOrderBookPoints("ask", orderBook.Asks, orderBook.Seq)
	processOrderBookPoints("bid", orderBook.Bids, orderBook.Seq)
	batchsToWrite <- &database.BatchPoints{"orderBook", points}
}

func prepareLastOrderBookCheckPoints(orderBooks publicapi.OrderBooks,
	depth int, baseTimestamp int64) {

	measurement := conf.Schema["book_orders_last_check_measurement"]
	timestamp := time.Unix(int64(baseTimestamp), 0)

	points := make([]*ifxClient.Point, 0, len(orderBooks))

	for market, ob := range orderBooks {

		tags := map[string]string{
			"source": "publicapi",
			"market": market,
		}

		fields := map[string]interface{}{
			"sequence":  ob.Seq,
			"bid_depth": len(ob.Bids),
			"ask_depth": len(ob.Asks),
		}

		pt, err := ifxClient.NewPoint(measurement, tags, fields, timestamp)
		if err != nil {
			logger.WithField("error", err).Error(
				"prepareLastOrderBookCheckPoint: ifxClient.NewPoint")
		}
		points = append(points, pt)
	}

	batchsToWrite <- &database.BatchPoints{
		"orderBookLastCheck",
		points,
	}
}
