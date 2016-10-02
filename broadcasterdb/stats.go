package broadcasterdb

import (
	"fmt"
	"time"
)

type Resolution int

const (
	_                = iota // Discard the 0 value
	Hours Resolution = iota
	Days
)

var ResolutionNames = map[string]Resolution{
	"hour": Hours,
	"day":  Days,
}

type MaybeStatsResult struct {
	Result map[int64][]AggregateMetric
	Error  error
}

func buildTimes(startDate, endDate time.Time, resolution Resolution) (times []int64) {
	exclusive_end := endDate.AddDate(0, 0, 1)
	if resolution == Hours {
		t := startDate
		for t.Before(exclusive_end) {
			times = append(times, t.Unix())
			t = t.Add(time.Hour)
		}
	} else if resolution == Days {
		t := startDate
		for t.Before(exclusive_end) {
			times = append(times, t.Unix())
			t = t.AddDate(0, 0, 1)
		}
	}

	return times
}

func denseMultichannelStats(statRowMaps []map[int64][]AggregateMetric, metrics []string, resolution Resolution, startDate, endDate time.Time) [][]float64 {
	times := buildTimes(startDate, endDate, resolution)
	row_len := len(metrics) + 1
	stats := make([][]float64, len(times))
	stats_flat := make([]float64, len(times)*row_len)
	for i := range stats {
		stats[i], stats_flat = stats_flat[:row_len], stats_flat[row_len:]
	}

	for i, t := range times {
		var accumulator []AggregateMetric
		for _, m := range statRowMaps {
			statlist, present := m[t]
			if !present {
				continue
			}
			if accumulator == nil {
				accumulator = statlist
			} else {
				for j := range metrics {
					accumulator[j].Aggregate(statlist[j])
				}
			}
		}
		stats[i][0] = float64(t)
		if accumulator == nil {
			continue
		}

		for j := range metrics {
			stats[i][j+1] = accumulator[j].Summarize()
		}
	}
	return stats
}

func GetStats(tableName string, channels []string, start_date, end_date time.Time, metrics []string, resolution Resolution) ([][]float64, error) {
	if resolution != Days && resolution != Hours {
		return nil, fmt.Errorf("Unit not recognized")
	}

	results := make(chan *MaybeStatsResult, 10)
	for _, channel := range channels {
		go func(channel string) {
			s, err := getSparseStats(tableName, channel, start_date, end_date, metrics, resolution)
			if err != nil {
				results <- &MaybeStatsResult{Error: err}
				return
			}
			results <- &MaybeStatsResult{Result: s}
		}(channel)
	}

	var resultMaps []map[int64][]AggregateMetric
	for _ = range channels {
		result := <-results
		if result.Error != nil {
			return nil, result.Error
		} else {
			resultMaps = append(resultMaps, result.Result)
		}
	}
	return denseMultichannelStats(resultMaps, metrics, resolution, start_date, end_date), nil
}
