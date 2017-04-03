// InfluxDB Schema and Infos
//
// retention policy: autogen_montly_sharded (inf with 1month shard)
//
// table Poloniex
//
// m:book_updates:
// timestamp f:sequence f:rate f:amount t:order_type t:market
//
// m:trade_updates (date from api)
// timestamp f:sequence f:rate f:amount t:order_type t:market
//
// Notes:
//
// - order_type: "sell" or "ask"
// - market is currencyPair
// - orderbookRemove are special orderbookModify with amount set
package main
