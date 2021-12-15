package window_processor

import "time"

// TimeMeasure definition of a time interval for windowing
type TimeMeasure struct {
	// the size of the windows
	size int64
	// the time unit
	unit time.Duration
}

func NewTimeMeasure(size int64, unit time.Duration) *TimeMeasure {
	return &TimeMeasure{
		size: size,
		unit: unit,
	}
}

type WindowMeasure int

const (
	Time WindowMeasure = iota
	Count
)

type AggregateWindow interface {
	Window
	GetStart() int64
	GetEnd() int64
	GetAggValues() []interface{}
	HasValue() bool
}

type WindowCollector interface {
	Trigger(start, end int64, measure WindowMeasure)
}




