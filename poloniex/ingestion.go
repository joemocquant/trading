package poloniex

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"poloniex/publicapi"
	"poloniex/pushapi"
	"sync"
	"time"

	influxDBClient "github.com/influxdata/influxdb/client/v2"
)

var (
	conf         *configuration
	publicClient *publicapi.PublicClient
	pushClient   *pushapi.PushClient
	dbClient     influxDBClient.Client

	mapMutex       sync.Mutex
	marketUpdaters map[string]pushapi.MarketUpdater
)

type configuration struct {
	Ingestion struct {
		Host                 string            `json:"host"`
		Schema               map[string]string `json:"schema"`
		MarketCheckPeriodMin int               `json:"market_check_period_min"`
	} `json:"ingestion"`
}

func init() {

	content, err := ioutil.ReadFile("conf.json")

	if err != nil {
		log.Fatalf("loading configuration: %v", err)
	}

	if err := json.Unmarshal(content, &conf); err != nil {
		log.Fatalf("loading configuration: %v", err)
	}

	publicClient = publicapi.NewPublicClient()

	dbClient, err = influxDBClient.NewHTTPClient(influxDBClient.HTTPConfig{
		Addr: conf.Ingestion.Host,
		// Username: username,
		// Password: password,
	})

	if err != nil {
		log.Fatal(err)
	}

	pushClient, err = pushapi.NewPushClient()

	if err != nil {
		log.Fatal(err)
	}

	marketUpdaters = make(map[string]pushapi.MarketUpdater)
}

func Ingest() {

	for {
		updateMarkets()
		<-time.After(time.Duration(conf.Ingestion.MarketCheckPeriodMin)
            * time.Minute)
	}
}

func updateMarkets() {

	tickers, err := publicClient.GetTickers()

	for err != nil {
		fmt.Printf("ingestion.updateMarkets: publicClient.GetTickers: %v\n", err)
		tickers, err = publicClient.GetTickers()
	}

	mapMutex.Lock()
	defer mapMutex.Unlock()

	for currencyPair, _ := range tickers {

		if _, ok := marketUpdaters[currencyPair]; !ok {

			marketUpdater, err := pushClient.SubscribeMarket(currencyPair)

			if err != nil {
				fmt.Printf("ingestion.updateMarkets: pushClient.SubscribeMarket (%s): %v\n",
					currencyPair, err)
				continue
			}

			marketUpdaters[currencyPair] = marketUpdater
			go dbWriter(marketUpdater, currencyPair)
		}
	}
}

func dbWriter(marketUpdater pushapi.MarketUpdater, currencyPair string) {

	for {
		marketUpdates := <-marketUpdater

		go func(marketUpdates *pushapi.MarketUpdates) {

			bp, err := influxDBClient.NewBatchPoints(influxDBClient.BatchPointsConfig{
				Database:  conf.Ingestion.Schema["database"],
				Precision: "ns",
			})
			if err != nil {
				fmt.Printf("ingestion.dbWriter: dbClient.NewBatchPoints: %v\n", err)
			}

			for _, marketUpdate := range marketUpdates.Updates {

				pt, err := preparePoint(marketUpdate, currencyPair, marketUpdates.Sequence)
				if err != nil {
					fmt.Printf("ingestion.dbWriter: ingestion.preparePoint: %v\n", err)
					continue
				}
				bp.AddPoint(pt)
			}

			if err := dbClient.Write(bp); err != nil {
				fmt.Printf("ingestion.dbWriter: ingestion.dbClient.Write %v\n", err)
			}

		}(marketUpdates)

	}
}

func preparePoint(marketUpdate *pushapi.MarketUpdate,
	currencyPair string, sequence int64) (*influxDBClient.Point, error) {

	tags := make(map[string]string)
	fields := make(map[string]interface{})
	var measurement string
	var timestamp time.Time

	switch marketUpdate.TypeUpdate {

	case "orderBookModify":

		obm := marketUpdate.Data.(pushapi.OrderBookModify)

		tags = map[string]string{
			"order_type": obm.TypeOrder,
			"market":     currencyPair,
		}
		fields = map[string]interface{}{
			"sequence": sequence,
			"rate":     obm.Rate,
			"amount":   obm.Amount,
		}
		measurement = conf.Ingestion.Schema["book_updates_measurement"]
		timestamp = time.Now()

	case "orderBookRemove":

		obr := marketUpdate.Data.(pushapi.OrderBookRemove)

		tags = map[string]string{
			"order_type": obr.TypeOrder,
			"market":     currencyPair,
		}
		fields = map[string]interface{}{
			"sequence": sequence,
			"rate":     obr.Rate,
			"amount":   0.0,
		}
		measurement = conf.Ingestion.Schema["book_updates_measurement"]
		timestamp = time.Now()

	case "newTrade":

		nt := marketUpdate.Data.(pushapi.NewTrade)

		tags = map[string]string{
			"order_type": nt.TypeOrder,
			"market":     currencyPair,
		}
		fields = map[string]interface{}{
			"sequence": sequence,
			"rate":     nt.Rate,
			"amount":   nt.Amount,
		}
		measurement = conf.Ingestion.Schema["trade_updates_measurement"]

		nano := time.Now().UnixNano() % int64(time.Second)
		timestamp = time.Unix(nt.Date, nano)
	}

	pt, err := influxDBClient.NewPoint(measurement, tags, fields, timestamp)
	if err != nil {
		return nil, err
	}
	return pt, nil
}
