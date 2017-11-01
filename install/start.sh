#!/bin/sh

influxd -config confs/influxdb.conf > db.log 2>&1 &


#Ingesting
cd ../ingestion/examples
TZ=UTC go run examples.go 2>&1 | tee -a ingestion.log &


cd ../../metrics/examples
TZ=UTC go run examples.go 2>&1 | tee -a metrics.log