package coinmarketcap

import (
	"time"
	"trading/coinmarketcap"

	influxDBClient "github.com/influxdata/influxdb/client/v2"
	log "github.com/sirupsen/logrus"
)

func ingestGlobalData() {

	for {
		globalData, err := coinmarketcapClient.GetGlobalData()

		for err != nil {
			log.WithField("error", err).Error("coinmarketcap.ingestGlobalData: publicClient.GetGlobalData")
			time.Sleep(5 * time.Second)
			globalData, err = coinmarketcapClient.GetGlobalData()
		}

		pt, err := prepareGlobalDataPoint(globalData)
		if err != nil {
			log.WithField("error", err).Error("coinmarketcap.ingestGlobalData: coinmarketcap.prepareGlobalDataPoint")
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
		log.WithField("error", err).Error("coinmarketcap.flushGlobalDataPoint: dbClient.NewBatchPoints")
		return
	}

	bp.AddPoint(point)

	if err := dbClient.Write(bp); err != nil {
		log.WithFields(log.Fields{
			"batchPoints": bp,
			"error":       err,
		}).Error("coinmarketcap.flushGlobalDataPoint: coinmarketcap.dbClient.Write")
		return
	}

	if log.GetLevel() >= log.DebugLevel {
		log.Debug("Flushed: global data")
	}
}
