package metrics

import (
	"fmt"
	"sort"
	"strings"
	"time"
	"trading/networking"
	"trading/networking/database"

	ifxClient "github.com/influxdata/influxdb/client/v2"
	"github.com/sirupsen/logrus"
)

type bookUpdates []*bookUpdate

type bookUpdate struct {
	orderType string
	rate      float64
	quantity  float64
	total     float64
	sequence  int64
}

type updateStatus struct {
	count   int64
	missing int64
	loss    float64
}

type bySequence []*bookUpdate

func (s bySequence) Len() int           { return len(s) }
func (s bySequence) Less(i, j int) bool { return s[i].sequence < s[j].sequence }
func (s bySequence) Swap(i, j int)      { s[i], s[j] = s[j], s[i] }

type byRate []*order

func (s byRate) Len() int           { return len(s) }
func (s byRate) Less(i, j int) bool { return s[i].rate < s[j].rate }
func (s byRate) Swap(i, j int)      { s[i], s[j] = s[j], s[i] }

func getLastOrderBooksPoloniex(ind *indicator, markets []string) orderBooks {

	res := getLastOrderBookSequencesPoloniex(ind, markets)
	if res == nil {
		return nil
	}

	query := ""
	sequences := make(map[string]int64, len(res[0].Series))

	for _, serie := range res[0].Series {

		market := serie.Tags["market"]

		seq, err := networking.ConvertJsonValueToInt64(serie.Values[0][1])
		if err != nil {
			logger.WithFields(logrus.Fields{
				"error":  err,
				"market": market,
			}).Error("getLastOrderBooksPoloniex: networking.ConvertJsonValueToInt64")
			continue
		}

		start, err := networking.ConvertJsonValueToTime(serie.Values[0][0])
		if err != nil {
			logger.WithFields(logrus.Fields{
				"error":  err,
				"market": market,
			}).Error("getLastOrderBooksPoloniex: networking.ConvertJsonValueToTime")
			continue
		}

		sequences[market] = seq
		end := start.Add(1 * time.Second)

		query += fmt.Sprintf(
			`SELECT rate, quantity, total, cumulative_sum, order_type
      FROM %s
      WHERE market = '%s' AND time >= %d AND time < %d
      GROUP BY market;`,
			ind.dataSource.Schema["book_orders_measurement"],
			market, start.UnixNano(), end.UnixNano())
	}

	request := func() (err error) {
		res, err = database.QueryDB(
			dbClient, query, ind.dataSource.Schema["database"])
		return err
	}

	success := networking.ExecuteRequest(&networking.RequestInfo{
		Logger:   logger.WithField("query", query),
		Period:   ind.period,
		ErrorMsg: "getLastOrderBooksPoloniex: database.QueryDB",
		Request:  request,
	})

	if !success {
		return nil
	}

	obs := formatOrderBooks(res)
	for market, ob := range obs {
		ob.sequence = sequences[market]
	}

	return obs
}

func getLastOrderBookSequencesPoloniex(ind *indicator,
	markets []string) []ifxClient.Result {

	where := ""
	if len(markets) != 0 {
		where = fmt.Sprintf("WHERE market = '%s'",
			strings.Join(markets, "' OR market = '"))
	}

	query := fmt.Sprintf(
		`SELECT sequence FROM %s %s GROUP BY market ORDER BY time DESC LIMIT 1`,
		ind.dataSource.Schema["book_orders_last_check_measurement"], where)

	var res []ifxClient.Result

	request := func() (err error) {
		res, err = database.QueryDB(
			dbClient, query, ind.dataSource.Schema["database"])
		return err
	}

	success := networking.ExecuteRequest(&networking.RequestInfo{
		Logger:   logger.WithField("query", query),
		Period:   ind.period,
		ErrorMsg: "getLastOrderBooksPoloniex: database.QueryDB",
		Request:  request,
	})

	if !success || len(res[0].Series) == 0 {
		return nil
	}

	return res
}

func getBookUpdates(obs orderBooks, ind *indicator) map[string]bookUpdates {

	query := ""
	for market, ob := range obs {

		query += fmt.Sprintf(
			`SELECT sequence, rate, quantity, total, order_type
      FROM %s
      WHERE market = '%s' AND sequence > %d
      GROUP BY market;`,
			ind.dataSource.Schema["book_updates_measurement"],
			market, ob.sequence)
	}

	if query == "" {
		return nil
	}

	var res []ifxClient.Result

	request := func() (err error) {
		res, err = database.QueryDB(
			dbClient, query, ind.dataSource.Schema["database"])
		return err
	}

	success := networking.ExecuteRequest(&networking.RequestInfo{
		Logger:   logger.WithField("query", query),
		Period:   ind.period,
		ErrorMsg: "getBookUpdates: database.QueryDB",
		Request:  request,
	})

	if !success {
		return nil
	}

	return formatBookUpdates(res)
}

func formatBookUpdates(res []ifxClient.Result) map[string]bookUpdates {

	bus := make(map[string]bookUpdates, len(res))

	for _, marketBus := range res {

		if marketBus.Series == nil {
			continue
		}

		serie := marketBus.Series[0]
		market := serie.Tags["market"]
		var updates bookUpdates

		for _, row := range serie.Values {

			sequence, err := networking.ConvertJsonValueToInt64(row[1])
			if err != nil {
				logger.WithFields(logrus.Fields{
					"error":  err,
					"market": market,
				}).Error("formatBookUpdates: networking.ConvertJsonValueToInt64")
				continue
			}

			rate, err := networking.ConvertJsonValueToFloat64(row[2])
			if err != nil {
				logger.WithFields(logrus.Fields{
					"error":  err,
					"market": market,
				}).Error("formatBookUpdates: networking.ConvertJsonValueToFloat64")
				continue
			}

			quantity, err := networking.ConvertJsonValueToFloat64(row[3])
			if err != nil {
				logger.WithFields(logrus.Fields{
					"error":  err,
					"market": market,
				}).Error("formatBookUpdates: networking.ConvertJsonValueToFloat64")
				continue
			}

			total, err := networking.ConvertJsonValueToFloat64(row[4])
			if err != nil {
				logger.WithFields(logrus.Fields{
					"error":  err,
					"market": market,
				}).Error("formatBookUpdates: networking.ConvertJsonValueToFloat64")
				continue
			}

			orderType, err := networking.ConvertJsonValueToString(row[5])
			if err != nil {
				logger.WithFields(logrus.Fields{
					"error":  err,
					"market": market,
				}).Error("formatBookUpdates: networking.ConvertJsonValueToString")
				continue
			}

			bu := bookUpdate{orderType, rate, quantity, total, sequence}
			updates = append(updates, &bu)
		}

		sort.Sort(bySequence(updates))
		bus[market] = updates
	}

	return bus
}

func mergeOrderBooksWithBookUpdates(obs orderBooks,
	mbus map[string]bookUpdates) {

	doUpdate := func(orders []*order, update *bookUpdate) []*order {

		if update.quantity == 0 {
			for i, order := range orders {
				if order.rate == update.rate {
					orders = append(orders[:i], orders[i+1:]...)
					break
				}
			}
			return orders
		}

		modified := false
		for _, order := range orders {

			if order.rate == update.rate {
				order.quantity = update.quantity
				modified = true
				break
			}
		}

		if !modified {
			orders = append(orders, &order{
				rate:      update.rate,
				quantity:  update.quantity,
				total:     update.total,
				orderType: update.orderType})
		}

		return orders
	}

	for market, bus := range mbus {

		ob := obs[market]

		for _, update := range bus {

			switch update.orderType {
			case "bid":
				ob.bids = doUpdate(ob.bids, update)
			case "ask":
				ob.asks = doUpdate(ob.asks, update)
			}

			sort.Sort(sort.Reverse(byRate(ob.bids)))
			sort.Sort(byRate(ob.asks))

			cs := 0.0
			for _, o := range ob.bids {
				cs += o.total
				o.cumulativeSum = cs
			}

			cs = 0.0
			for _, o := range ob.asks {
				cs += o.total
				o.cumulativeSum = cs
			}
		}
	}
}

func getLossStatus(sequence int64, newBookUpdates bookUpdates) *updateStatus {

	baseSeq := sequence
	var missing int64 = 0
	var lastSeq int64 = 0

	for _, update := range newBookUpdates {

		upSeq := update.sequence - baseSeq

		if upSeq == lastSeq {
			continue
		}

		missing += upSeq - lastSeq - 1
		lastSeq = upSeq
	}

	return &updateStatus{
		count:   lastSeq,
		missing: missing,
		loss:    100 * float64(missing) / float64(lastSeq),
	}
}
