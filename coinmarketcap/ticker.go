package coinmarketcap

import (
	"time"
	"trading/api/coinmarketcap"
	"trading/networking"

	"github.com/Sirupsen/logrus"
	ifxClient "github.com/influxdata/influxdb/client/v2"
)

func ingestTicks() {

	period := time.Duration(conf.TicksCheckPeriodMin) * time.Minute

	for {

		go func() {
			var ticks coinmarketcap.Ticks

			request := func() (err error) {
				ticks, err = coinmarketcapClient.GetTickers()
				return err
			}

			success := networking.ExecuteRequest(&networking.RequestInfo{
				Logger:   logger,
				Period:   period,
				ErrorMsg: "ingestTicks: publicClient.GetTickers",
				Request:  request,
			})

			if !success {
				return
			}

			points := make([]*ifxClient.Point, 0, len(ticks))
			for _, tick := range ticks {

				pt, err := prepareTickPoint(tick)
				if err != nil {
					logger.WithField("error", err).Error(
						"ingestTicks: coinmarketcap.prepareTickPoint")
					continue
				}
				points = append(points, pt)
			}

			flushTickPoints(points)
		}()

		<-time.After(period)
	}

}

func prepareTickPoint(tick *coinmarketcap.Tick) (*ifxClient.Point, error) {

	measurement := conf.Schema["ticks_measurement"]
	timestamp := time.Unix(tick.LastUpdated, 0)

	tags := map[string]string{
		"source": "coinmarketcap",
		"symbol": tick.Symbol,
	}

	fields := map[string]interface{}{
		"id":                 tick.Id,
		"name":               tick.Name,
		"rank":               tick.Rank,
		"price_usd":          tick.PriceUSD,
		"price_btc":          tick.PriceBTC,
		"24h_volume_usd":     tick.DayVolumeUSD,
		"market_cap_usd":     tick.MarketCapUSD,
		"available_supply":   tick.AvailableSupply,
		"total_supply":       tick.TotalSupply,
		"percent_change_1h":  tick.PercentChange1h,
		"percent_change_24h": tick.PercentChange24h,
		"percent_change_7d":  tick.PercentChange7d,
	}

	pt, err := ifxClient.NewPoint(measurement, tags, fields, timestamp)
	if err != nil {
		return nil, err
	}
	return pt, nil
}

func flushTickPoints(points []*ifxClient.Point) {

	bp, err := ifxClient.NewBatchPoints(ifxClient.BatchPointsConfig{
		Database:  conf.Schema["database"],
		Precision: "ns",
	})

	if err != nil {
		logger.WithField("error", err).Error(
			"flushTickPoints: dbClient.NewBatchPoints")
		return
	}

	bp.AddPoints(points)

	if err := dbClient.Write(bp); err != nil {
		logger.WithFields(logrus.Fields{
			"batchPoints": bp,
			"error":       err,
		}).Error("flushTickPoints: dbClient.Write")
		return
	}

	if logrus.GetLevel() >= logrus.DebugLevel {
		logger.Debugf("[Coinmarketcap flush]: %d ticks", len(points))
	}
}
