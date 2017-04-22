package poloniex

import (
	"time"
	"trading/poloniex/publicapi"

	influxDBClient "github.com/influxdata/influxdb/client/v2"
	log "github.com/sirupsen/logrus"
)

func ingestOrderBooks(depth int, period time.Duration) {

	for {
		orderBooks, err := publicClient.GetOrderBooks(depth)

		for err != nil {
			log.WithField("error", err).Error("ingestion.ingestOrderBooks: publicClient.GetOrderBooks")
			time.Sleep(5 * time.Second)
			orderBooks, err = publicClient.GetOrderBooks(depth)
		}

		for currencyPair, orderBook := range orderBooks {
			prepareOrderBookPoints(currencyPair, orderBook, depth)
			prepareLastOrderBookCheckPoint(currencyPair, orderBook.Seq, depth)
		}

		<-time.After(period)
	}
}

func prepareOrderBookPoints(currencyPair string, orderBook *publicapi.OrderBook, depth int) {

	measurement := conf.Ingestion.Schema["book_orders_measurement"]
	baseTimestamp := time.Now().Unix()
	index := 0

	size := len(orderBook.Asks) + len(orderBook.Bids)
	points := make([]*influxDBClient.Point, 0, size)

	processOrderBookPoints :=
		func(currencyPair, typeOrder string, orders []*publicapi.Order, sequence int64) {

			for _, order := range orders {

				tags := map[string]string{
					"source":     "publicapi",
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

	processOrderBookPoints(currencyPair, "ask", orderBook.Asks, orderBook.Seq)
	processOrderBookPoints(currencyPair, "bid", orderBook.Bids, orderBook.Seq)
	pointsToWrite <- &batchPoints{"orderBook", points}
}

func prepareLastOrderBookCheckPoint(currencyPair string, sequence int64, depth int) {

	measurement := conf.Ingestion.Schema["book_orders_last_check_measurement"]
	timestamp := time.Now()

	tags := map[string]string{
		"source": "publicapi",
		"market": currencyPair,
	}

	fields := map[string]interface{}{
		"sequence": sequence,
		"depth":    depth,
	}

	pt, err := influxDBClient.NewPoint(measurement, tags, fields, timestamp)
	if err != nil {
		log.WithField("error", err).Error("ingestion.writeOrderBooks: influxDBClient.NewPoint")
	}
	pointsToWrite <- &batchPoints{"orderBookLastCheck", []*influxDBClient.Point{pt}}
}
