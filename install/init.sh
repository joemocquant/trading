#!/bin/sh

# influxdb reinit script
# have ssl cert in /etc/ssl
# cert in keychain/system should be trusted for ssl

pkill influxd
if [ $? = 0 ]; then
    echo "Existing influxdb server stopped"
fi

rm -rf /tmp/.influxdb
if [ $? = 0 ]; then
    echo "/tmp/.influxdb removed"
fi

influxd -config confs/influxdb.conf > /dev/null 2>&1 &

sleep 1

influx -ssl -execute "create user "admin" with password 'password' with all privileges"
if [ $? = 0 ]; then
    echo "Admin user created"
fi

influx -ssl -username admin -password password -import -path=db_init.txt
if [ $? = 0 ]; then
    echo "Database imported"
fi


# Poloniex

# ./cq/poloniex/poloniex_cq_ohlc.sh
# if [ $? = 0 ]; then
#     echo "Continuous queries (ohlc) created for Poloniex"
# fi

# ./cq/poloniex/poloniex_cq_sma.sh
# if [ $? = 0 ]; then
#     echo "Continuous queries (sma) created for Poloniex"
# fi


# Bittrex

# ./cq/bittrex/bittrex_cq_ohlc.sh
# if [ $? = 0 ]; then
#     echo "Continuous queries (ohlc) created for Bittrex"
# fi


# ./cq/bittrex/bittrex_cq_sma.sh
# if [ $? = 0 ]; then
#     echo "Continuous queries (sma) created for Bittrex"
# fi

pkill influxd
