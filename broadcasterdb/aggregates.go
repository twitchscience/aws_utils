package broadcasterdb

import (
	"math"
	"time"

	"github.com/twitchscience/aws_utils/common"
)

type AggregateMetric interface {
	Aggregate(AggregateMetric)
	Summarize() float64
}

type sumMetric float64

func (s *sumMetric) Aggregate(m AggregateMetric) {
	*s += *m.(*sumMetric)
	return
}

func (s *sumMetric) Summarize() float64 {
	return float64(*s)
}

type maxMetric float64

func (s *maxMetric) Aggregate(m AggregateMetric) {
	*s = maxMetric(math.Max(float64(*s), float64(*m.(*maxMetric))))
	return
}

func (s *maxMetric) Summarize() float64 {
	return float64(*s)
}

type fractionMetric struct {
	num   float64
	denom float64
}

func (s *fractionMetric) Aggregate(m AggregateMetric) {
	rhs := *m.(*fractionMetric)
	s.num += rhs.num
	s.denom += rhs.denom
}

func (s *fractionMetric) Summarize() float64 {
	return s.num / s.denom
}

func newMaxMetric(f float64) AggregateMetric {
	tmp := maxMetric(f)
	return &tmp
}

func newSumMetric(f float64) AggregateMetric {
	tmp := sumMetric(f)
	return &tmp
}

var metricAggregates = map[string]func(map[string][]float64) []AggregateMetric{
	"avg_concurrents": aggrAvgConcurrents,
	"max_concurrents": AggregateMetricFun("max_concurrents", newMaxMetric),
	"hours_broadcast": AggregateMetricFun("hours_broadcast", newSumMetric),
	"hours_watched":   AggregateMetricFun("hours_watched", newSumMetric),
}

func aggrAvgConcurrents(values map[string][]float64) []AggregateMetric {
	var out []AggregateMetric

	hours, hoursWatched, hoursBroadcast := values["hours"], values["hours_watched"], values["hours_broadcast"]
	for i, _ := range hours {
		w, b := hoursWatched[i], hoursBroadcast[i]
		out = append(out, &fractionMetric{w, b})
	}
	return out
}

func AggregateMetricFun(metric string, aggrMaker func(float64) AggregateMetric) func(map[string][]float64) []AggregateMetric {
	return func(values map[string][]float64) []AggregateMetric {
		var outSeries []AggregateMetric
		hours, series := values["hours"], values[metric]

		for i, _ := range hours {
			outSeries = append(outSeries, aggrMaker(series[i]))
		}
		return outSeries
	}
}

func aggrTimes(resolution Resolution, values map[string][]float64) (times []int64) {
	if resolution == Hours {
		for _, t := range values["hours"] {
			times = append(times, int64(t))
		}
	} else if resolution == Days {
		for _, t := range values["hours"] {
			times = append(times, common.DayFloor(time.Unix(int64(t), 0).In(time.UTC)).Unix())
		}
	}
	return
}
