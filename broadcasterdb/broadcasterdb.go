/*
Package wraps dynamodb functionality as the broadcast database
*/
package broadcasterdb

import (
	"encoding/json"
	"fmt"
	"strconv"
	"time"

	"github.com/crowdmob/goamz/aws"
	"github.com/crowdmob/goamz/dynamodb"
	"github.com/twitchscience/aws_utils/common"
)

type prodDbConnection struct {
	db *dynamodb.Server
}

// Get a production connection to dynamo db
func connect() (*prodDbConnection, error) {
	auth, err := aws.GetAuth("", "", "", time.Time{})
	if err != nil {
		return nil, err
	}

	ret := &prodDbConnection{
		db: &dynamodb.Server{auth, aws.USWest2},
	}

	return ret, nil
}

var DynamoDbColumns = []string{"hours", "max_concurrents", "hours_broadcast", "hours_watched"}

// Return an existing table if found, else nil
func (conn *prodDbConnection) Table(name string) *dynamodb.Table {
	td, err := conn.db.DescribeTable(name)
	if err != nil {
		return nil
	}

	t, err := conn.TableFromDescription(td)
	if err != nil {
		panic(err)
	}

	return t
}

func (conn *prodDbConnection) TableFromDescription(td *dynamodb.TableDescriptionT) (*dynamodb.Table, error) {
	pk, err := td.BuildPrimaryKey()
	if err != nil {
		return nil, err
	}

	table := conn.db.NewTable(td.TableName, pk)
	return table, nil
}

func queryDataAsRowMap(resolution Resolution, metrics []string, queryResults []map[string]*dynamodb.Attribute) (map[int64][]AggregateMetric, error) {
	unsortedStats := make(map[int64][]AggregateMetric)
	for _, monthData := range queryResults {
		monthDataMap := make(map[string][]float64)
		for _, m := range DynamoDbColumns {
			var series []float64
			err := json.Unmarshal([]byte(monthData[m].Value), &series)
			if err != nil {
				return nil, err
			}
			monthDataMap[m] = series
		}

		times := aggrTimes(resolution, monthDataMap)
		for _, t := range times {
			if _, present := unsortedStats[t]; !present {
				unsortedStats[t] = make([]AggregateMetric, len(metrics))
			}
		}

		for i, name := range metrics {
			values := metricAggregates[name](monthDataMap)
			for j, t := range times {
				if unsortedStats[t][i] != nil {
					unsortedStats[t][i].Aggregate(values[j])
				} else {
					unsortedStats[t][i] = values[j]
				}
			}
		}
	}
	return unsortedStats, nil
}

func fetchRawStats(conn *prodDbConnection, tableName, channel string, startDate, endDate time.Time) ([]map[string]*dynamodb.Attribute, error) {
	table := conn.Table(tableName)
	if table == nil {
		return nil, fmt.Errorf("Table  %s is missing", tableName)
	}
	monthBetween := dynamodb.AttributeComparison{
		AttributeName:      "month",
		ComparisonOperator: dynamodb.COMPARISON_BETWEEN,
		AttributeValueList: []dynamodb.Attribute{
			*dynamodb.NewNumericAttribute("month", strconv.FormatInt(common.MonthFloor(startDate).Unix(), 10)),
			*dynamodb.NewNumericAttribute("month", strconv.FormatInt(common.MonthFloor(endDate).Unix(), 10)),
		},
	}

	attrComp := []dynamodb.AttributeComparison{
		*dynamodb.NewEqualStringAttributeComparison("channel", channel),
		monthBetween,
	}

	return table.Query(attrComp)
}

func getSparseStats(tableName, channel string, startDate time.Time, endDate time.Time, metrics []string, resolution Resolution) (map[int64][]AggregateMetric, error) {
	conn, err := connect()
	if err != nil {
		return nil, err
	}

	queryResults, err := fetchRawStats(conn, tableName, channel, startDate, endDate)
	if err != nil {
		return nil, err
	}

	statRowMap, err := queryDataAsRowMap(resolution, metrics, queryResults)
	if err != nil {
		return nil, err
	}

	return statRowMap, nil
}
