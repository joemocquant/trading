package coinmarketcap

import (
	"time"
	"trading/api/coinmarketcap"

	"github.com/Sirupsen/logrus"
	influxDBClient "github.com/influxdata/influxdb/client/v2"
)

func ingestGlobalData() {

	for {
		globalData, err := coinmarketcapClient.GetGlobalData()

		for err != nil {
			logger.WithField("error", err).Error("ingestGlobalData: publicClient.GetGlobalData")
			time.Sleep(5 * time.Second)
			globalData, err = coinmarketcapClient.GetGlobalData()
		}

		pt, err := prepareGlobalDataPoint(globalData)
		if err != nil {
			logger.WithField("error", err).Error("ingestGlobalData: prepareGlobalDataPoint")
			continue
		}

		flushGlobalDataPoint(pt)

		<-time.After(time.Duration(conf.GlobalDataCheckPeriodMin) * time.Minute)
	}
}

func prepareGlobalDataPoint(gd *coinmarketcap.GlobalData) (*influxDBClient.Point, error) {

	measurement := conf.Schema["global_data_measurement"]
	timestamp := time.Now()

	fields := map[string]interface{}{
		"total_market_cap_usd":             gd.TotalMarketCapUSD,
		"total_24h_volume_usd":             gd.Total24hVolumeUSD,
		"bitcoin_percentage_of_market_cap": gd.BitcoinPercentageOfMarketCap,
		"active_currencies":                gd.ActiveCurrencies,
		"active_assets":                    gd.ActiveAsset,
		"active_markets":                   gd.ActiveMarkets,
	}

	pt, err := influxDBClient.NewPoint(measurement, nil, fields, timestamp)
	if err != nil {
		return nil, err
	}
	return pt, nil
}

func flushGlobalDataPoint(point *influxDBClient.Point) {

	bp, err := influxDBClient.NewBatchPoints(influxDBClient.BatchPointsConfig{
		Database:  conf.Schema["database"],
		Precision: "ns",
	})

	if err != nil {
		logger.WithField("error", err).Error("flushGlobalDataPoint: dbClient.NewBatchPoints")
		return
	}

	bp.AddPoint(point)

	if err := dbClient.Write(bp); err != nil {
		logger.WithFields(logrus.Fields{
			"batchPoints": bp,
			"error":       err,
		}).Error("flushGlobalDataPoint: dbClient.Write")
		return
	}

	if logrus.GetLevel() >= logrus.DebugLevel {
		logger.Debug("Flushed: global data")
	}
}
