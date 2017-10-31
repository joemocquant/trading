#!/bin/sh

cmd="influx -ssl -username admin -password password -execute"


######################################## poloniex_wa_30s
## FOR 1h to fill(0) (need to have data within time internval)

read -r -d '' cq <<- EOM

CREATE CONTINUOUS QUERY poloniex_wa_30s ON poloniex
RESAMPLE EVERY 10s FOR 1h
BEGIN
  SELECT SUM(total) AS volume,
    SUM(quantity) AS quantity,
    SUM(total) / SUM(quantity) AS weighted_average
  INTO metrics.autogen_monthly_sharded.poloniex_ohlc_30s
  FROM trade_updates
  GROUP BY time(30s), market fill(0)
END

EOM

cq=$(echo $cq | tr -d '\n')
`$cmd "$cq"`


######################################## poloniex_ohlc_30s
## FOR 1h to fill(previous) (need to have data within time internval)

read -r -d '' cq <<- EOM

CREATE CONTINUOUS QUERY poloniex_ohlc_30s ON poloniex
RESAMPLE EVERY 10s FOR 1h
BEGIN
  SELECT FIRST(rate) AS open,
    MAX(rate) AS high,
    MIN(rate) AS low,
    LAST(rate) AS close
  INTO metrics.autogen_monthly_sharded.poloniex_ohlc_30s
  FROM trade_updates
  GROUP BY time(30s), market fill(previous)
END

EOM

cq=$(echo $cq | tr -d '\n')
`$cmd "$cq"`


######################################## poloniex_ohlc_1m

read -r -d '' cq <<- EOM

CREATE CONTINUOUS QUERY poloniex_ohlc_1m ON metrics
RESAMPLE EVERY 10s FOR 2m
BEGIN
  SELECT SUM(volume) AS volume,
    SUM(quantity) AS quantity,
    SUM(volume) / SUM(quantity) AS weighted_average,
    FIRST(open) AS open,
    MAX(high) AS high,
    MIN(low) AS low,
    LAST(close) AS close
  INTO metrics.autogen_monthly_sharded.poloniex_ohlc_1m
  FROM poloniex_ohlc_30s
  GROUP BY time(1m), market
END

EOM

cq=$(echo $cq | tr -d '\n')
`$cmd "$cq"`



######################################## poloniex_ohlc_5m

read -r -d '' cq <<- EOM

CREATE CONTINUOUS QUERY poloniex_ohlc_5m ON metrics
RESAMPLE EVERY 10s FOR 10m
BEGIN
  SELECT SUM(volume) AS volume,
    SUM(quantity) AS quantity,
    SUM(volume) / SUM(quantity) AS weighted_average,
    FIRST(open) AS open,
    MAX(high) AS high,
    MIN(low) AS low,
    LAST(close) AS close
  INTO metrics.autogen_monthly_sharded.poloniex_ohlc_5m
  FROM poloniex_ohlc_1m
  GROUP BY time(5m), market
END

EOM

cq=$(echo $cq | tr -d '\n')
`$cmd "$cq"`


######################################## poloniex_ohlc_15m

read -r -d '' cq <<- EOM

CREATE CONTINUOUS QUERY poloniex_ohlc_15m ON metrics
RESAMPLE EVERY 10s FOR 30m
BEGIN
  SELECT SUM(volume) AS volume,
    SUM(quantity) AS quantity,
    SUM(volume) / SUM(quantity) AS weighted_average,
    FIRST(open) AS open,
    MAX(high) AS high,
    MIN(low) AS low,
    LAST(close) AS close
  INTO metrics.autogen_monthly_sharded.poloniex_ohlc_15m
  FROM poloniex_ohlc_5m
  GROUP BY time(15m), market
END

EOM

cq=$(echo $cq | tr -d '\n')
`$cmd "$cq"`


######################################## poloniex_ohlc_30m

read -r -d '' cq <<- EOM

CREATE CONTINUOUS QUERY poloniex_ohlc_30m ON metrics
RESAMPLE EVERY 10s FOR 1h
BEGIN
  SELECT SUM(volume) AS volume,
    SUM(quantity) AS quantity,
    SUM(volume) / SUM(quantity) AS weighted_average,
    FIRST(open) AS open,
    MAX(high) AS high,
    MIN(low) AS low,
    LAST(close) AS close
  INTO metrics.autogen_monthly_sharded.poloniex_ohlc_30m
  FROM poloniex_ohlc_15m
  GROUP BY time(30m), market
END

EOM

cq=$(echo $cq | tr -d '\n')
`$cmd "$cq"`

######################################## poloniex_ohlc_1h

read -r -d '' cq <<- EOM

CREATE CONTINUOUS QUERY poloniex_ohlc_1h ON metrics
RESAMPLE EVERY 10s FOR 2h
BEGIN
  SELECT SUM(volume) AS volume,
    SUM(quantity) AS quantity,
    SUM(volume) / SUM(quantity) AS weighted_average,
    FIRST(open) AS open,
    MAX(high) AS high,
    MIN(low) AS low,
    LAST(close) AS close
  INTO metrics.autogen_monthly_sharded.poloniex_ohlc_1h
  FROM poloniex_ohlc_30m
  GROUP BY time(1h), market
END

EOM

cq=$(echo $cq | tr -d '\n')
`$cmd "$cq"`


######################################## poloniex_ohlc_6h

read -r -d '' cq <<- EOM

CREATE CONTINUOUS QUERY poloniex_ohlc_6h ON metrics
RESAMPLE EVERY 10s FOR 12h
BEGIN
  SELECT SUM(volume) AS volume,
    SUM(quantity) AS quantity,
    SUM(volume) / SUM(quantity) AS weighted_average,
    FIRST(open) AS open,
    MAX(high) AS high,
    MIN(low) AS low,
    LAST(close) AS close
  INTO metrics.autogen_monthly_sharded.poloniex_ohlc_6h
  FROM poloniex_ohlc_1h
  GROUP BY time(6h), market
END

EOM

cq=$(echo $cq | tr -d '\n')
`$cmd "$cq"`


######################################## poloniex_ohlc_1d

read -r -d '' cq <<- EOM

CREATE CONTINUOUS QUERY poloniex_ohlc_1d ON metrics
RESAMPLE EVERY 10s FOR 2d
BEGIN
  SELECT SUM(volume) AS volume,
    SUM(quantity) AS quantity,
    SUM(volume) / SUM(quantity) AS weighted_average,
    FIRST(open) AS open,
    MAX(high) AS high,
    MIN(low) AS low,
    LAST(close) AS close
  INTO metrics.autogen_monthly_sharded.poloniex_ohlc_1d
  FROM poloniex_ohlc_6h
  GROUP BY time(1d), market
END

EOM

cq=$(echo $cq | tr -d '\n')
`$cmd "$cq"`


######################################## poloniex_ohlc_1w

read -r -d '' cq <<- EOM

CREATE CONTINUOUS QUERY poloniex_ohlc_1w ON metrics
RESAMPLE EVERY 10s FOR 2w
BEGIN
  SELECT SUM(volume) AS volume,
    SUM(quantity) AS quantity,
    SUM(volume) / SUM(quantity) AS weighted_average,
    FIRST(open) AS open,
    MAX(high) AS high,
    MIN(low) AS low,
    LAST(close) AS close
  INTO metrics.autogen_monthly_sharded.poloniex_ohlc_1w
  FROM poloniex_ohlc_1d
  GROUP BY time(1w), market
END

EOM

cq=$(echo $cq | tr -d '\n')
`$cmd "$cq"`