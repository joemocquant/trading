package metrics

import (
	"fmt"
	"strings"
	"time"
	"trading/networking"
	"trading/networking/database"

	"github.com/Sirupsen/logrus"
	ifxClient "github.com/influxdata/influxdb/client/v2"
)

func getLastOrderBooksBittrex(ind *indicator, markets []string) orderBooks {

	res := getLastOrderBookTimestampsBittrex(ind, markets)
	if res == nil {
		return nil
	}

	query := ""
	for _, serie := range res[0].Series {

		market := serie.Tags["market"]

		start, err := networking.ConvertJsonValueToTime(serie.Values[0][0])
		if err != nil {
			logger.WithFields(logrus.Fields{
				"error":  err,
				"market": market,
			}).Error("getLastOrderBooksBittrex: networking.ConvertJsonValueToTime")
			continue
		}

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
		ErrorMsg: "getLastOrderBooksBittrex: database.QueryDB",
		Request:  request,
	})

	if !success {
		return nil
	}

	return formatOrderBooks(res)
}

func getLastOrderBookTimestampsBittrex(ind *indicator,
	markets []string) []ifxClient.Result {

	where := ""
	if len(markets) != 0 {
		where = fmt.Sprintf("WHERE market = '%s'",
			strings.Join(markets, "' OR market = '"))
	}

	query := fmt.Sprintf(
		`SELECT ask_depth FROM %s %s GROUP BY market ORDER BY time DESC LIMIT 1`,
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
		ErrorMsg: "getLastOrderBooksBittrex: database.QueryDB",
		Request:  request,
	})

	if !success || len(res[0].Series) == 0 {
		return nil
	}

	return res
}
