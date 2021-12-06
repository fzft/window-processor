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
	CreateContext() *WindowContext
}

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
		return 9999
	}
}

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
	context *WindowContext
}

func NewSessionWindow(measure WindowMeasure, gap int64) *SessionWindow {
	return &SessionWindow{measure: measure, gap: gap}
}

func (w *SessionWindow) GetMeasure() WindowMeasure {
	return w.measure
}

func (w *SessionWindow) getSession(pos int64) int {
	i := 0
	for ; i <= w.context.numberOfActiveWindows(); i++ {
		s := w.context.getWindow(i)
		if s.getStart()-w.gap <= pos && s.getEnd()+w.gap >= pos {
			return i
		} else if s.getStart()-w.gap > pos {
			return i - 1
		}
	}
	return i - 1
}

func (w *SessionWindow) CreateContext() *WindowContext {
	ctx := NewWindowContext()
	w.context = ctx
	return ctx
}

func (w *SessionWindow) UpdateContext(tuple interface{}, position int64) *ActiveWindow {
	if w.context.hasActiveWindows() {
		w.context.addNewWindow(0, position, position)
		return w.context.getWindow(0)
	}

	sessionIndex := w.getSession(position)
	if sessionIndex == -1 {
		w.context.addNewWindow(0, position, position)
	} else {
		s := w.context.getWindow(sessionIndex)
		if s.getStart()-w.gap > position {
			return w.context.addNewWindow(sessionIndex, position, position)
		} else if s.getStart() > position && s.getStart()-w.gap < position {
			w.context.shiftStart(s, position)
			if sessionIndex > 0 {
				preSession := w.context.getWindow(sessionIndex - 1)
				if preSession.getEnd()+w.gap >= s.getStart() {
					return w.context.mergeWithPre(sessionIndex)
				}
			}
			return s
		} else if s.getEnd() < position && s.getEnd()+w.gap >= position {
			w.context.shiftEnd(s, position)
			if sessionIndex < w.context.numberOfActiveWindows()-1 {
				nextSession := w.context.getWindow(sessionIndex - 1)
				if s.getEnd()+w.gap >= nextSession.getStart() {
					return w.context.mergeWithPre(sessionIndex + 1)
				}
			}
		} else if s.getEnd()+w.gap < position {
			return w.context.addNewWindow(sessionIndex+1, position, position)
		}
	}
	return nil
}

func (w *SessionWindow) AssignNextWindowStart(pos int64) int64 {
	return pos + w.gap
}

func (w *SessionWindow) TriggerWindows(aggregateWindows WindowCollector, lastWatermark, currentWatermark int64) {
	s := w.context.getWindow(0)
	for s.getEnd()+w.gap < currentWatermark {
		aggregateWindows.Trigger(s.getStart(), s.getEnd()+w.gap, w.measure)
		w.context.removeWindow(0)
		if w.context.hasActiveWindows() {
			return
		}
		s = w.context.getWindow(0)
	}
}

func (w *SessionWindow) ClearDelay() int64 {
	return -1
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
