package window_processor

import "time"

type TimeMeasure struct {
	size int64
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




