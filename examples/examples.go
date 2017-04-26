package main

import (
	"trading/ingestion/coinmarketcap"
	"trading/ingestion/poloniex"
)

func main() {

	go poloniex.Ingest()
	go coinmarketcap.Ingest()

	select {}
}
