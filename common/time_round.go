// Library functions to manipulate time
package common

import (
	"log"
	"time"
)

var (
	PT *time.Location
)

func init() {
	var err error
	PT, err = time.LoadLocation("America/Los_Angeles")
	if err != nil {
		log.Fatalln(err)
	}
}

// Return the time.Time representation of the Pacific Time month t happened in
func PacificMonthFloorUnix(t int64) time.Time {
	return PacificMonthFloor(time.Unix(t, 0).In(PT))
}

func PacificMonthFloor(t time.Time) time.Time {
	y, m, _ := t.Date()
	return time.Date(y, m, 1, 0, 0, 0, 0, PT)
}

func PacificDayFloorUnix(t int64) time.Time {
	return PacificDayFloor(time.Unix(t, 0).In(PT))
}

func PacificDayFloor(t time.Time) time.Time {
	y, m, d := t.Date()
	return time.Date(y, m, d, 0, 0, 0, 0, PT)
}

func PacificHourFloorUnix(t int64) time.Time {
	return PacificHourFloor(time.Unix(t, 0).In(PT))
}

func PacificHourFloor(t time.Time) time.Time {
	y, m, d := t.Date()
	return time.Date(y, m, d, t.Hour(), 0, 0, 0, PT)
}

func MonthFloor(t time.Time) time.Time {
	y, m, _ := t.Date()
	return time.Date(y, m, 1, 0, 0, 0, 0, time.UTC)
}

func DayFloor(t time.Time) time.Time {
	y, m, d := t.Date()
	return time.Date(y, m, d, 0, 0, 0, 0, time.UTC)
}

func HourFloor(t time.Time) time.Time {
	y, m, d := t.Date()
	return time.Date(y, m, d, t.Hour(), 0, 0, 0, time.UTC)
}
