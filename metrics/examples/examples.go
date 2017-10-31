package main

import (
	"trading/metrics"
)

func main() {

	metrics.ComputeMetrics()

	select {}

}
