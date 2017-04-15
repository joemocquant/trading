package poloniex

import (
	"time"
	"trading/poloniex/publicapi"

	influxDBClient "github.com/influxdata/influxdb/client/v2"
	log "github.com/sirupsen/logrus"
)

func ingestOrderBooks() {

	orderBooks, err := publicClient.GetOrderBooks(100000)

	for err != nil {
		log.WithField("error", err).Error("ingestion.ingestOrderBooks: publicClient.GetOrderBooks")
		time.Sleep(5 * time.Second)
		orderBooks, err = publicClient.GetOrderBooks(100000)
	}

	prepareOrderBooksPoints(&orderBooks)
}

func prepareOrderBooksPoints(orderBooks *publicapi.OrderBooks) {

	measurement := conf.Ingestion.Schema["book_orders_measurement"]
	baseTimestamp := time.Now().Unix()
	index := 0

	size := 0
	for _, orderBook := range *orderBooks {
		size += len(orderBook.Asks) + len(orderBook.Bids)
	}

	points := make([]*influxDBClient.Point, 0, size)

	loop := func(currencyPair, typeOrder string, orders []publicapi.Order, sequence int64) {

		for _, order := range orders {

			tags := map[string]string{
				"order_type": typeOrder,
				"market":     currencyPair,
			}

			fields := map[string]interface{}{
				"sequence": sequence,
				"rate":     order.Rate,
				"amount":   order.Quantity,
			}

			timestamp := time.Unix(int64(baseTimestamp), int64(index))
			index++

			pt, err := influxDBClient.NewPoint(measurement, tags, fields, timestamp)
			if err != nil {
				log.WithField("error", err).Error("ingestion.writeOrderBooks: influxDBClient.NewPoint")
				continue
			}
			points = append(points, pt)
		}
	}

	for currencyPair, orderBook := range *orderBooks {
		loop(currencyPair, "ask", orderBook.Asks, orderBook.Seq)
		loop(currencyPair, "bid", orderBook.Bids, orderBook.Seq)
	}

	pointsToWrite <- &batchPoints{"orderBooks", points}
}
