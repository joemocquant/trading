FROM golang:1.9.2

### SSL

#Generate self-signed ssl certificate (for https)
RUN printf "\n\n\n\n\nlocalhost\n\n" | openssl req -x509 -nodes -newkey rsa:2048 -keyout /etc/ssl/influxdb-selfsigned-key.pem -out /etc/ssl/influxdb-selfsigned-cert.pem -days 1000

#Adding self-signed certificate to trusted list
RUN cp /etc/ssl/influxdb-selfsigned-cert.pem /usr/local/share/ca-certificates/influxdb-selfsigned-cert.crt
RUN update-ca-certificates


### InfluxDB isntall

RUN apt-get update && apt-get install apt-transport-https

#install influxdb
RUN curl -sL https://repos.influxdata.com/influxdb.key | apt-key add -
RUN . /etc/os-release
RUN echo "deb https://repos.influxdata.com/debian stretch stable" | tee /etc/apt/sources.list.d/influxdb.list

RUN apt-get update && apt-get install influxdb



### GO 

#Fetch go dependencies
RUN go get -v github.com/influxdata/influxdb/client/v2 \
gopkg.in/jcelliott/turnpike.v2 \
github.com/joemocquant/bittrex-api/publicapi \
github.com/joemocquant/cmc-api \
github.com/joemocquant/poloniex-api


WORKDIR src
RUN git clone https://github.com/joemocquant/trading.git


#Configure InfluxDB database
WORKDIR trading/install
RUN ./init.sh

#Launch database server + ingestion processes
CMD ./start.sh
