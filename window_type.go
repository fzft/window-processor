package window_processor

type Window interface {
	GetMeasure() WindowMeasure
}

type ContextFreeWindow interface {
	Window
	AssignNextWindowStart(pos int64) int64
	TriggerWindows(windows WindowCollector, lastWatermark, currentWatermark int64)
	ClearDelay() int64
}

type ForwardContext interface {
	Window
	CreateContext() Context
}

// FixedBandWindow this window starts at the defined start timestamp and ends at start + size
type FixedBandWindow struct {
	measure WindowMeasure
	start   int64
	size    int64
}

func NewFixedBandWindow(measure WindowMeasure, start, size int64) *FixedBandWindow {
	return &FixedBandWindow{
		measure: measure,
		start:   start,
		size:    size,
	}
}

func (w *FixedBandWindow) GetMeasure() WindowMeasure {
	return w.measure
}

func (w *FixedBandWindow) AssignNextWindowStart(pos int64) int64 {
	if pos < w.start {
		return w.start
	} else if pos >= w.start && pos < w.start+w.size {
		return w.start + w.size
	} else {
		return Max_Value
	}
}

// TriggerWindows triggers the window, if it started after lastWatermark and ended before currentWatermark
func (w *FixedBandWindow) TriggerWindows(aggregateWindows WindowCollector, lastWatermark, currentWatermark int64) {
	if lastWatermark <= w.start+w.size && w.start+w.size <= currentWatermark {
		aggregateWindows.Trigger(w.start, w.start+w.size, w.measure)
	}
}

func (w *FixedBandWindow) ClearDelay() int64 {
	return w.size
}

type SessionWindow struct {
	measure WindowMeasure
	gap     int64
	context *SessionContext
}

func NewSessionWindow(measure WindowMeasure, gap int64) *SessionWindow {
	return &SessionWindow{measure: measure, gap: gap}
}

func (w *SessionWindow) GetMeasure() WindowMeasure {
	return w.measure
}

func (w *SessionWindow) CreateContext() Context {
	ctx := NewSessionContext(w.measure, w.gap)
	w.context = ctx
	return ctx
}

type SlidingWindow struct {
	measure WindowMeasure
	size    int64
	slide   int64
}

func NewSlidingWindow(measure WindowMeasure, size, slide int64) *SlidingWindow {
	return &SlidingWindow{
		measure: measure,
		size:    size,
		slide:   slide,
	}
}

func (w *SlidingWindow) GetMeasure() WindowMeasure {
	return w.measure
}

func (w *SlidingWindow) AssignNextWindowStart(recordStamp int64) int64 {
	return recordStamp + w.slide - recordStamp%w.slide
}

func (w *SlidingWindow) TriggerWindows(aggregateWindows WindowCollector, lastWatermark, currentWatermark int64) {
	lastStart := GetWindowStartWithOffset(currentWatermark, w.slide)
	for windowStart := lastStart; windowStart+w.size > lastWatermark; windowStart -= w.slide {
		if windowStart >= 0 && windowStart+w.size <= currentWatermark+1 {
			aggregateWindows.Trigger(windowStart, windowStart+w.size, w.measure)
		}
	}
}

func (w *SlidingWindow) ClearDelay() int64 {
	return w.size
}

type TumblingWindow struct {
	measure WindowMeasure
	size    int64
}

func NewTumblingWindow(measure WindowMeasure, size int64) *TumblingWindow {
	return &TumblingWindow{measure: measure, size: size}
}

func (w *TumblingWindow) GetMeasure() WindowMeasure {
	return w.measure
}

func (w *TumblingWindow) AssignNextWindowStart(recordStamp int64) int64 {
	return recordStamp + w.size - recordStamp%w.size
}

func (w *TumblingWindow) TriggerWindows(aggregateWindows WindowCollector, lastWatermark, currentWatermark int64) {
	lastStart := lastWatermark - (lastWatermark+w.size)%w.size
	for windowStart := lastStart; windowStart+w.size <= currentWatermark; windowStart += w.size {
		aggregateWindows.Trigger(windowStart, windowStart+w.size, w.measure)
	}
}

func (w *TumblingWindow) ClearDelay() int64 {
	return w.size
}

func GetWindowStartWithOffset(timestamp, windowSize int64) int64 {
	return timestamp - (timestamp+windowSize)%windowSize
}
