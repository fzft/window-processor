package window_processor

import (
	"github.com/emirpasic/gods/sets/hashset"
)

type AggregationWindowCollector struct {
	aggregationStores []AggregateWindow
	stateFactory      StateFactory
	windowFunctions   []AggregateFunction
}

func NewAggregationWindowCollector(stateFactory StateFactory, windowFunctions []AggregateFunction) *AggregationWindowCollector {
	return &AggregationWindowCollector{
		stateFactory:    stateFactory,
		windowFunctions: windowFunctions,
	}
}

func (c *AggregationWindowCollector) Trigger(start, end int64, measure WindowMeasure) {
	aggWindow := NewAggregateWindowState(start, end, measure, c.stateFactory, c.windowFunctions)
	c.aggregationStores = append(c.aggregationStores, aggWindow)
}

func (c *AggregationWindowCollector) Range() []AggregateWindow {
	return c.aggregationStores
}

type WindowManager struct {
	aggregationStore       AggregationStore
	stateFactory           StateFactory
	hasFixedWindows        bool
	hasCountMeasure        bool
	hasContextAwareWindows bool
	maxLateness            int64
	maxFixedWindowSize     int64
	contextFreeWindows     []ContextFreeWindow
	contextAwareWindows    []Context
	windowFunctions        []AggregateFunction
	lastWatermark          int64
	hasTimeMeasure         bool
	currentCount           int64
	lastCount              int64
	isSessionWindowCase    bool
}

func NewWindowManager(stateFactory StateFactory, aggregationStore AggregationStore) *WindowManager {
	manager := new(WindowManager)
	manager.maxLateness = 1000
	manager.lastWatermark = -1
	manager.stateFactory = stateFactory
	manager.aggregationStore = aggregationStore
	return manager
}

func (m *WindowManager) ProcessWatermark(watermarkTs int64) []AggregateWindow {
	if m.lastWatermark == -1 {
		m.lastWatermark = Max(0, watermarkTs-m.maxLateness)
	}

	if m.aggregationStore.IsEmpty() {
		m.lastWatermark = watermarkTs
		return []AggregateWindow{}
	}

	oldestSliceStart := m.aggregationStore.GetSlice(0).GetTStart()

	if m.lastWatermark < oldestSliceStart {
		m.lastWatermark = oldestSliceStart
	}

	windows := NewAggregationWindowCollector(m.stateFactory, m.windowFunctions)
	m.assignContextFreeWindows(watermarkTs, windows)
	m.assignContextAwareWindows(watermarkTs, windows)

	minTs := Max_Value
	maxTs := Min_Value
	minCount := m.currentCount
	maxCount := int64(0)
	for _, aggregateWindow := range windows.Range() {
		if aggregateWindow.GetMeasure() == Time {
			minTs = Min(aggregateWindow.GetStart(), minTs)
			maxTs = Max(aggregateWindow.GetEnd(), maxTs)
		} else if aggregateWindow.GetMeasure() == Count {
			minCount = Min(aggregateWindow.GetStart(), minCount)
			maxCount = Max(aggregateWindow.GetEnd(), maxCount)
		}
	}

	if len(windows.Range()) > 0 {
		m.aggregationStore.Aggregate(windows, minTs, maxTs, minCount, maxCount)
	}

	m.lastWatermark = watermarkTs
	m.lastCount = m.currentCount
	m.clearAfterWatermark(watermarkTs - m.maxLateness)
	return windows.aggregationStores
}

func (m *WindowManager) assignContextAwareWindows(watermarkTs int64, windows *AggregationWindowCollector) {
	for _, context := range m.contextAwareWindows {
		context.triggerWindows(windows, m.lastWatermark, watermarkTs)
	}
}

func (m *WindowManager) assignContextFreeWindows(watermarkTs int64, windowCollector WindowCollector) {
	for _, window := range m.contextFreeWindows {
		if window.GetMeasure() == Time {
			window.TriggerWindows(windowCollector, m.lastWatermark, watermarkTs)
		} else if window.GetMeasure() == Count {
			sliceIndex := m.aggregationStore.FindSliceIndexByTimestamp(watermarkTs)
			slice := m.aggregationStore.GetSlice(sliceIndex)
			if slice.GetTLast() >= watermarkTs && sliceIndex > 0 {
				slice = m.aggregationStore.GetSlice(sliceIndex - 1)
			}
			cend := slice.GetCLast()
			window.TriggerWindows(windowCollector, m.lastCount, cend+1)
		}
	}
}

func (m *WindowManager) clearAfterWatermark(currentWatermark int64) {
	firstActiveWindowStart := currentWatermark
	for _, context := range m.contextAwareWindows {
		for _, window := range context.getActiveWindows() {
			firstActiveWindowStart = Min(firstActiveWindowStart, window.getStart())
		}
	}
	maxDelay := currentWatermark - m.maxFixedWindowSize
	removeFromTimestamp := Min(maxDelay, firstActiveWindowStart)
	m.aggregationStore.RemoveSlices(removeFromTimestamp)
}

func (m *WindowManager) addWindowAssigner(window Window) {
	if w, ok := window.(ContextFreeWindow); ok {
		m.contextFreeWindows = append(m.contextFreeWindows, w)
		m.maxFixedWindowSize = Max(m.maxFixedWindowSize, w.ClearDelay())
		m.hasFixedWindows = true
	}
	if fc, ok := window.(ForwardContext); ok {
		if _, ok := fc.(*SessionWindow); ok && (!m.hasContextAwareWindows || m.isSessionWindowCase) {
			m.isSessionWindowCase = true
		} else {
			m.isSessionWindowCase = false
		}
		m.hasContextAwareWindows = true
		context := fc.CreateContext()
		m.contextAwareWindows = append(m.contextAwareWindows, context)
	}

	if window.GetMeasure() == Count {
		m.hasCountMeasure = true
	} else {
		m.hasTimeMeasure = true
	}
}

func (m *WindowManager) addAggregation(windowFunction AggregateFunction) {
	m.windowFunctions = append(m.windowFunctions, windowFunction)
}

func (m *WindowManager) HasContextAwareWindow() bool {
	return m.hasContextAwareWindows
}

func (m *WindowManager) HasFixedWindows() bool {
	return m.hasFixedWindows
}

func (m *WindowManager) IncrementCount() {
	m.currentCount++
}

func (m *WindowManager) GetMaxLateness() int64 {
	return m.maxLateness
}

func (m *WindowManager) GetContextFreeWindows() []ContextFreeWindow {
	return m.contextFreeWindows
}

func (m *WindowManager) GetAggregations() []AggregateFunction {
	return m.windowFunctions
}

func (m *WindowManager) GetContextAwareWindows() []Context {
	return m.contextAwareWindows
}

func (m *WindowManager) HasCountMeasure() bool {
	return m.hasCountMeasure
}
func (m *WindowManager) HasTimeMeasure() bool {
	return m.hasTimeMeasure
}

func (m *WindowManager) IsSessionWindowCase() bool {
	return m.isSessionWindowCase
}

func (m *WindowManager) GetCurrentCount() int64 {
	return m.currentCount
}
func (m *WindowManager) SetMaxLateness(maxLateness int64) {
	m.maxLateness = maxLateness
}

type SliceManager struct {
	sliceFactory     SliceFactory
	aggregationStore AggregationStore
	windowManager    *WindowManager
}

func NewSliceManager(sliceFactory SliceFactory, aggregationStore AggregationStore, windowManager *WindowManager) *SliceManager {
	return &SliceManager{
		sliceFactory:     sliceFactory,
		aggregationStore: aggregationStore,
		windowManager:    windowManager,
	}
}

// AppendSlice append new slice, this method is called by StreamSlicer
// startTs: the start timestamp of new slice or end timestamp of previous slice
// t : the end measure of previous slice
func (m *SliceManager) AppendSlice(startTs int64, t Type) {

	if !m.aggregationStore.IsEmpty() {
		currentSlice := m.aggregationStore.GetCurrentSlice()
		currentSlice.SetTEnd(startTs)
		currentSlice.SetType(t)
	}

	slice := m.sliceFactory.CreateSlice(startTs, Max_Value, m.windowManager.GetCurrentCount(), m.windowManager.GetCurrentCount(), NewFlexible(1))
	m.aggregationStore.AppendSlice(slice)
}

// ProcessElement process tuple and insert it in the correct slice
func (m *SliceManager) ProcessElement(element interface{}, ts int64) {
	if m.aggregationStore.IsEmpty() {
		m.AppendSlice(0, NewFlexible(1))
	}

	currentSlice := m.aggregationStore.GetCurrentSlice()

	// is element in order
	if ts >= currentSlice.GetTLast() {
		// in order
		m.aggregationStore.InsertValueToCurrentSlice(element, ts)
		windowModifications := hashset.New()
		for _, windowContext := range m.windowManager.GetContextAwareWindows() {
			windowContext.updateContext1(element, ts, windowModifications)
		}
	} else {
		// out of order
		for _, windowContext := range m.windowManager.GetContextAwareWindows() {
			windowModifications := hashset.New()
			windowContext.updateContext1(element, ts, windowModifications)
			m.checkSliceEdges(windowModifications)
		}

		indexOfSlice := m.aggregationStore.FindSliceIndexByTimestamp(ts)
		m.aggregationStore.InsertValueToSlice(indexOfSlice, element, ts)
		if m.windowManager.hasCountMeasure {
			// shift count in slices
			for ; indexOfSlice <= m.aggregationStore.Size()-2; indexOfSlice++ {
				lazySlice := m.aggregationStore.GetSlice(indexOfSlice).(*LazySlice)
				lastElement := lazySlice.DropLastElement()
				nextSlice := m.aggregationStore.GetSlice(indexOfSlice + 1).(*LazySlice)
				nextSlice.PrependElement(lastElement)
			}
		}
	}
}

func (m *SliceManager) checkSliceEdges(modifications *hashset.Set) {
	for _, mod := range modifications.Values() {
		if smod, ok := mod.(ShiftModification); ok {
			pre := smod.pre
			post := smod.post
			sliceIndex := m.aggregationStore.FindSliceByEnd(pre)
			if sliceIndex == -1 {
				continue
			}
			currentSlice := m.aggregationStore.GetSlice(sliceIndex)
			sliceType := currentSlice.GetType()
			if sliceType.IsMovable() {
				// move slice start
				nextSlice := m.aggregationStore.GetSlice(sliceIndex + 1)
				currentSlice.SetTEnd(post)
				nextSlice.SetTStart(post)

				if post < pre {
					// move tuples to nextSlice
					if lazyCurrentSlice, ok := currentSlice.(*LazySlice); ok {
						for lazyCurrentSlice.GetTFirst() < lazyCurrentSlice.GetTLast() && lazyCurrentSlice.GetTLast() >= post {
							lastElement := lazyCurrentSlice.DropLastElement()
							nextSlice.(*LazySlice).PrependElement(lastElement)
						}
					}
				} else {
					// move tuples to currentSlice
					if lazyNextSlice, ok := nextSlice.(*LazySlice); ok {
						for lazyNextSlice.GetTFirst() < lazyNextSlice.GetTLast() && lazyNextSlice.GetTFirst() < post {
							lastElement := lazyNextSlice.DropFirstElement()
							currentSlice.(*LazySlice).PrependElement(lastElement)
						}
					}
				}
			} else {
				if t, ok := sliceType.(*Flexible); ok {
					t.DecrementCount()
				}
				m.splitSlice(sliceIndex, post)
			}

		}

		if dmod, ok := mod.(DeleteModification); ok {
			pre := dmod.pre
			sliceIndex := m.aggregationStore.FindSliceByEnd(pre)
			if sliceIndex >= 0 {
				// slice edge still exist
				currentSlice := m.aggregationStore.GetSlice(sliceIndex)
				sliceType := currentSlice.GetType()
				if sliceType.IsMovable() {
					nextSlice := m.aggregationStore.GetSlice(sliceIndex + 1)
					// move records to new slice
					if lazyNextSlice, ok := nextSlice.(*LazySlice); ok {
						for lazyNextSlice.GetCLast() > 0 {
							lastElement := lazyNextSlice.DropLastElement()
							currentSlice.(*LazySlice).PrependElement(lastElement)
						}
					}
					m.aggregationStore.MergeSlice(sliceIndex)
				} else {
					if t, ok := sliceType.(*Flexible); ok {
						t.DecrementCount()
					}
				}
			}
		}

		if amod, ok := mod.(AddModification); ok {
			newWindowEdge := amod.post
			sliceIndex := m.aggregationStore.FindSliceIndexByTimestamp(newWindowEdge)
			slice := m.aggregationStore.GetSlice(sliceIndex)
			if slice.GetTStart() != newWindowEdge && slice.GetTEnd() != newWindowEdge {
				m.splitSlice(sliceIndex, newWindowEdge)
			}
		}
	}
}

func (m *SliceManager) splitSlice(sliceIndex int, timestamp int64) {
	sliceA := m.aggregationStore.GetSlice(sliceIndex)
	var sliceB Slice
	if timestamp < sliceA.GetTEnd() {
		sliceB = m.sliceFactory.CreateSlice(timestamp, sliceA.GetTEnd(), sliceA.GetCStart(), sliceA.GetCLast(), sliceA.GetType())
		sliceA.SetTEnd(timestamp)
		sliceA.SetType(NewFlexible(1))
		m.aggregationStore.AddSlice(sliceIndex+1, sliceB)
	} else if sliceIndex+1 < m.aggregationStore.Size() {
		sliceA = m.aggregationStore.GetSlice(sliceIndex + 1)
		sliceB = m.sliceFactory.CreateSlice(timestamp, sliceA.GetTEnd(), sliceA.GetCStart(), sliceA.GetCLast(), sliceA.GetType())
		sliceA.SetTEnd(timestamp)
		sliceA.SetType(NewFlexible(1))
		m.aggregationStore.AddSlice(sliceIndex+2, sliceB)
	} else {
		return
	}

	// move records to new slice
	if lazySliceA, ok := sliceA.(*LazySlice); ok {
		for lazySliceA.GetTLast() >= timestamp {
			lastElement := lazySliceA.DropLastElement()
			sliceB.(*LazySlice).PrependElement(lastElement)
		}
	}
}
