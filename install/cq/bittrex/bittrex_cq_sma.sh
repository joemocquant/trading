#!/bin/sh

cmd="influx -ssl -username admin -password password -execute"

#period is 30s

######################################## bittrex_sma_1m

read -r -d '' cq <<- EOM

CREATE CONTINUOUS QUERY bittrex_sma_1m ON metrics
RESAMPLE EVERY 10s FOR 2m
BEGIN
  SELECT SUM(close) AS sum_close,
    COUNT(close) AS count_close,
    SUM(close) / COUNT(close) AS sma
  INTO metrics.autogen_monthly_sharded.bittrex_sma_1m
  FROM bittrex_ohlc_30s
  GROUP BY time(1m), market
END

EOM

cq=$(echo $cq | tr -d '\n')
`$cmd "$cq"`


######################################## bittrex_sma_5m

read -r -d '' cq <<- EOM

CREATE CONTINUOUS QUERY bittrex_sma_5m ON metrics
RESAMPLE EVERY 10s FOR 10m
BEGIN
  SELECT SUM(sum_close) as sum_close,
    COUNT(count_close) as count_close,
    SUM(sum_close) / COUNT(count_close) AS sma
  INTO metrics.autogen_monthly_sharded.bittrex_sma_5m
  FROM bittrex_sma_1m
  GROUP BY time(5m), market
END

EOM

cq=$(echo $cq | tr -d '\n')
`$cmd "$cq"`


######################################## bittrex_sma_15m

read -r -d '' cq <<- EOM

CREATE CONTINUOUS QUERY bittrex_sma_15m ON metrics
RESAMPLE EVERY 10s FOR 30m
BEGIN
  SELECT SUM(sum_close) as sum_close,
    COUNT(count_close) as count_close,
    SUM(sum_close) / COUNT(count_close) AS sma
  INTO metrics.autogen_monthly_sharded.bittrex_sma_15m
  FROM bittrex_sma_5m
  GROUP BY time(15m), market
END

EOM

cq=$(echo $cq | tr -d '\n')
`$cmd "$cq"`


######################################## bittrex_sma_30m

read -r -d '' cq <<- EOM

CREATE CONTINUOUS QUERY bittrex_sma_30m ON metrics
RESAMPLE EVERY 10s FOR 1h
BEGIN
  SELECT SUM(sum_close) as sum_close,
    COUNT(count_close) as count_close,
    SUM(sum_close) / COUNT(count_close) AS sma
  INTO metrics.autogen_monthly_sharded.bittrex_sma_30m
  FROM bittrex_sma_15m
  GROUP BY time(30m), market
END

EOM

cq=$(echo $cq | tr -d '\n')
`$cmd "$cq"`


######################################## bittrex_sma_1h

read -r -d '' cq <<- EOM

CREATE CONTINUOUS QUERY bittrex_sma_1h ON metrics
RESAMPLE EVERY 10s FOR 2h
BEGIN
  SELECT SUM(sum_close) as sum_close,
    COUNT(count_close) as count_close,
    SUM(sum_close) / COUNT(count_close) AS sma
  INTO metrics.autogen_monthly_sharded.bittrex_sma_1h
  FROM bittrex_sma_30m
  GROUP BY time(1h), market
END

EOM

cq=$(echo $cq | tr -d '\n')
`$cmd "$cq"`


######################################## bittrex_sma_6h

read -r -d '' cq <<- EOM

CREATE CONTINUOUS QUERY bittrex_sma_6h ON metrics
RESAMPLE EVERY 10s FOR 12h
BEGIN
  SELECT SUM(sum_close) as sum_close,
    COUNT(count_close) as count_close,
    SUM(sum_close) / COUNT(count_close) AS sma
  INTO metrics.autogen_monthly_sharded.bittrex_sma_6h
  FROM bittrex_sma_1h
  GROUP BY time(6h), market
END

EOM

cq=$(echo $cq | tr -d '\n')
`$cmd "$cq"`


######################################## bittrex_sma_1d

read -r -d '' cq <<- EOM

CREATE CONTINUOUS QUERY bittrex_sma_1d ON metrics
RESAMPLE EVERY 10s FOR 2d
BEGIN
  SELECT SUM(sum_close) as sum_close,
    COUNT(count_close) as count_close,
    SUM(sum_close) / COUNT(count_close) AS sma
  INTO metrics.autogen_monthly_sharded.bittrex_sma_1d
  FROM bittrex_sma_6h
  GROUP BY time(1d), market
END

EOM

cq=$(echo $cq | tr -d '\n')
`$cmd "$cq"`


######################################## bittrex_sma_1w

read -r -d '' cq <<- EOM

CREATE CONTINUOUS QUERY bittrex_sma_1w ON metrics
RESAMPLE EVERY 10s FOR 2w
BEGIN
  SELECT SUM(sum_close) as sum_close,
    COUNT(count_close) as count_close,
    SUM(sum_close) / COUNT(count_close) AS sma
  INTO metrics.autogen_monthly_sharded.bittrex_sma_1w
  FROM bittrex_sma_1d
  GROUP BY time(1w), market
END

EOM

cq=$(echo $cq | tr -d '\n')
`$cmd "$cq"`
