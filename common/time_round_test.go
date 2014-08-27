package common

import (
	"testing"
	"time"
)

func TestPacificMonthFloorUnix(t *testing.T) {
	expected := [][]int64{
		{time.Date(2014, 1, 1, 0, 0, 0, 0, PT).Unix(), time.Date(2014, 1, 1, 0, 0, 0, 0, PT).Unix()},
		{time.Date(2014, 5, 1, 5, 5, 5, 5, PT).Unix(), time.Date(2014, 5, 1, 0, 0, 0, 0, PT).Unix()},
		{time.Date(2014, 5, 1, 4, 0, 0, 0, PT).Unix(), time.Date(2014, 5, 1, 0, 0, 0, 0, PT).Unix()},
	}
	testIntArray(t, PacificMonthFloorUnix, "PacificMonthFloorUnix", expected)
}

func TestPacificDayFloorUnix(t *testing.T) {
	expected := [][]int64{
		{time.Date(2014, 1, 1, 0, 0, 0, 0, PT).Unix(), time.Date(2014, 1, 1, 0, 0, 0, 0, PT).Unix()},
		{time.Date(2014, 5, 1, 5, 5, 5, 5, PT).Unix(), time.Date(2014, 5, 1, 0, 0, 0, 0, PT).Unix()},
		{time.Date(2014, 5, 1, 4, 0, 0, 0, PT).Unix(), time.Date(2014, 5, 1, 0, 0, 0, 0, PT).Unix()},
	}
	testIntArray(t, PacificDayFloorUnix, "PacificDayFloorUnix", expected)
}

func TestPacificHourFloorUnix(t *testing.T) {
	expected := [][]int64{
		{time.Date(2014, 1, 1, 0, 0, 0, 0, PT).Unix(), time.Date(2014, 1, 1, 0, 0, 0, 0, PT).Unix()},
		{time.Date(2014, 5, 1, 5, 5, 5, 5, PT).Unix(), time.Date(2014, 5, 1, 5, 0, 0, 0, PT).Unix()},
		{time.Date(2014, 5, 1, 4, 0, 0, 0, PT).Unix(), time.Date(2014, 5, 1, 4, 0, 0, 0, PT).Unix()},
	}
	testIntArray(t, PacificHourFloorUnix, "PacificHourFloorUnix", expected)
}

func TestMonthFloor(t *testing.T) {
	expected := [][]time.Time{
		{time.Date(2014, 1, 1, 0, 0, 0, 0, time.UTC), time.Date(2014, 1, 1, 0, 0, 0, 0, time.UTC)},
		{time.Date(2014, 5, 4, 4, 4, 4, 0, time.UTC), time.Date(2014, 5, 1, 0, 0, 0, 0, time.UTC)},
		{time.Date(2014, 5, 5, 0, 0, 0, 0, time.UTC), time.Date(2014, 5, 1, 0, 0, 0, 0, time.UTC)},
	}
	testTimeArray(t, MonthFloor, "MonthFloor", expected)
}
func TestDayFloor(t *testing.T) {
	expected := [][]time.Time{
		{time.Date(2014, 1, 1, 0, 0, 0, 0, time.UTC), time.Date(2014, 1, 1, 0, 0, 0, 0, time.UTC)},
		{time.Date(2014, 5, 4, 4, 4, 4, 0, time.UTC), time.Date(2014, 5, 4, 0, 0, 0, 0, time.UTC)},
		{time.Date(2014, 5, 5, 0, 0, 0, 0, time.UTC), time.Date(2014, 5, 5, 0, 0, 0, 0, time.UTC)},
	}
	testTimeArray(t, DayFloor, "DayFloor", expected)
}
func TestHourFloor(t *testing.T) {
	expected := [][]time.Time{
		{time.Date(2014, 1, 1, 0, 0, 0, 0, time.UTC), time.Date(2014, 1, 1, 0, 0, 0, 0, time.UTC)},
		{time.Date(2014, 5, 4, 4, 4, 4, 0, time.UTC), time.Date(2014, 5, 4, 4, 0, 0, 0, time.UTC)},
		{time.Date(2014, 5, 5, 3, 0, 0, 0, time.UTC), time.Date(2014, 5, 5, 3, 0, 0, 0, time.UTC)},
	}
	testTimeArray(t, HourFloor, "HourFloor", expected)
}

func testIntArray(t *testing.T, f func(int64) time.Time, name string, expected [][]int64) {
	for _, pair := range expected {
		input, expected := pair[0], pair[1]
		output := f(input).Unix()
		if expected != output {
			t.Errorf("Calling %s on %d returned %d instead of expected %d",
				name, output, expected)
		}
	}
}

func testTimeArray(t *testing.T, f func(time.Time) time.Time, name string, expected [][]time.Time) {
	for _, pair := range expected {
		input, expected := pair[0], pair[1]
		output := f(input)
		if expected != output {
			t.Errorf("Calling %s on %s returned %s instead of expected %s",
				name, input, output, expected)
		}
	}
}
