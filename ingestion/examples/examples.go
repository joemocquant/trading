package main

import (
	"trading/ingestion/bittrex"
	"trading/ingestion/coinmarketcap"
	"trading/ingestion/poloniex"
)

func main() {

	go poloniex.Ingest()
	go bittrex.Ingest()
	go coinmarketcap.Ingest()

	select {}
}
