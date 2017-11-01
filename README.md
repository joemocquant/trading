# Real time trading data ingestion pipeline using Go and InfluxDB time series database

Run: 

docker build -t trading .
<br>
docker run trading


Connect to the container to monitor logs: 

bash -c "clear && docker exec -it <container_name> sh"

tail -f /go/src/trading/ingestion/examples/ingestion.log | grep -e ERRO -e WARN
<br>
tail -f /go/src/trading/ingestion/examples/ingestion.log | grep -v batchs
<br>
tail -f /go/src/trading/install/db.log
