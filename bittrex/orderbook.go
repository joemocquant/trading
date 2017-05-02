package bittrex

import (
	"time"
	"trading/api/bittrex/publicapi"
	"trading/ingestion"

	influxDBClient "github.com/influxdata/influxdb/client/v2"
)

func ingestOrderBooks() {

	for {

		markets := getActiveMarkets()

		for _, marketName := range markets {
			go ingestOrderBook(marketName)
		}

		<-time.After(time.Duration(conf.OrderBooksCheckPeriodSec) * time.Second)
	}
}

func ingestOrderBook(marketName string) {

	orderBook, err := publicClient.GetOrderBook(marketName, "both")

	for err != nil {
		logger.WithField("error", err).Error("ingestOrderBook: publicClient.GetOrderBook")
		time.Sleep(5 * time.Second)
		orderBook, err = publicClient.GetOrderBook(marketName, "both")
	}

	prepareOrderBookPoints(marketName, orderBook)
}

func prepareOrderBookPoints(marketName string, orderBook *publicapi.OrderBook) {

	measurement := conf.Schema["book_orders_measurement"]
	baseTimestamp := time.Now().Unix()
	index := 0

	size := len(orderBook.Sell) + len(orderBook.Buy)
	points := make([]*influxDBClient.Point, 0, size)

	processOrderBookPoints := func(currencyPair, typeOrder string, orders []*publicapi.Order) {

		tags := map[string]string{
			"source":     "publicapi",
			"order_type": typeOrder,
			"market":     marketName,
		}

		for _, order := range orders {

			fields := map[string]interface{}{
				"rate":   order.Rate,
				"amount": order.Quantity,
			}

			timestamp := time.Unix(int64(baseTimestamp), int64(index))
			index++

			pt, err := influxDBClient.NewPoint(measurement, tags, fields, timestamp)
			if err != nil {
				logger.WithField("error", err).Error("prepareOrderBookPoints: influxDBClient.NewPoint")
				continue
			}
			points = append(points, pt)
		}
	}

	processOrderBookPoints(marketName, "sell", orderBook.Sell)
	processOrderBookPoints(marketName, "buy", orderBook.Buy)
	batchsToWrite <- &ingestion.BatchPoints{"orderBook", points}
}
