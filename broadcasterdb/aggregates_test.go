package broadcasterdb

import (
	"math"
	"testing"
)

var Inputs = map[string][]float64{
	"hours":           []float64{0, 3600, 43200},
	"hours_watched":   []float64{1, 2, 3},
	"hours_broadcast": []float64{1, 1, 0.5},
	"max_concurrents": []float64{3, 2, 5},
}

func TestAvgConcurrents(t *testing.T) {
	expectedHourly := []float64{1, 2, 6}
	expectedDaily := 2.4
	metricTester(t, metricAggregates["avg_concurrents"], expectedHourly, expectedDaily)
}

func TestMaxConcurrents(t *testing.T) {
	expectedHourly := []float64{3, 2, 5}
	expectedDaily := 5.0
	metricTester(t, metricAggregates["max_concurrents"], expectedHourly, expectedDaily)
}

func TestTimeWatched(t *testing.T) {
	expectedHourly := []float64{1, 2, 3}
	expectedDaily := 6.0
	metricTester(t, metricAggregates["hours_watched"], expectedHourly, expectedDaily)
}

func TestTimeBroadcast(t *testing.T) {
	expectedHourly := []float64{1, 1, 0.5}
	expectedDaily := 2.5
	metricTester(t, metricAggregates["hours_broadcast"], expectedHourly, expectedDaily)
}

func metricTester(t *testing.T, f func(map[string][]float64) []AggregateMetric, expectedHourly []float64, expectedDaily float64) {
	aggr := f(Inputs)
	if len(aggr) != len(expectedHourly) {
		t.Fatal("Returned an unexpected number of outputs")
	}
	for i, _ := range aggr {
		if math.Abs(aggr[i].Summarize()-expectedHourly[i]) > 0.001 {
			t.Fatal("Hourly values didn't match expectations")
		}
	}

	times := aggrTimes(Days, Inputs)
	for _, ts := range times {
		if ts != 0 {
			t.Fatal("Time conversion failed")
		}
	}

	dailyAggr := aggr[0]
	for _, x := range aggr[1:] {
		dailyAggr.Aggregate(x)
	}

	if math.Abs(dailyAggr.Summarize()-expectedDaily) > 0.001 {
		t.Fatal("Value is wrong for daily")
	}
}
