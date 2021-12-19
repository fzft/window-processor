package window_processor

import (
	"sync"
)

type Type interface {
	IsMovable() bool
}

type Fixed struct{}

func NewFixed() *Fixed {
	return &Fixed{}
}

func (t *Fixed) IsMovable() bool {
	return false
}

type Flexible struct {
	count int
}

func NewFlexible(count int) *Flexible {
	return &Flexible{
		count: count,
	}
}

func (t *Flexible) GetCount() int {
	return t.count
}

func (t *Flexible) DecrementCount() {
	t.count--
}

func (t *Flexible) IsMovable() bool {
	return t.count == 1
}

type Slice interface {
	// GetTStart slice start timestamp
	GetTStart() int64
	SetTStart(tStart int64)

	// GetTEnd slice end timestamp
	GetTEnd() int64
	SetTEnd(tEnd int64)

	// GetTFirst timestamp of the first record in the slice
	GetTFirst() int64
	Merge(otherSlice Slice)

	// GetTLast timestamp of the last record in the slice
	GetTLast() int64

	// GetType the measure of the slice end
	GetType() Type

	// SetType set the end of the slice
	SetType(t Type)

	// AddElement add a element to slice
	AddElement(element interface{}, ts int64)

	// GetCStart count of first element
	GetCStart() int64

	// GetCLast count of last element
	GetCLast() int64

	// GetAggState ...
	GetAggState() *AggregateState
}

type AbstractSlice struct {
	mu     sync.Mutex
	tStart int64
	tEnd   int64
	t      Type
	tLast  int64
	tFirst int64
	cStart int64
	cLast  int64
}

func (s *AbstractSlice) GetAggState() *AggregateState {
	panic("implement me")
}

func NewAbstractSlice(startTs, endTs, cStart, cLast int64, t Type) *AbstractSlice {
	return &AbstractSlice{
		t:      t,
		tStart: startTs,
		tEnd:   endTs,
		cStart: cStart,
		cLast:  cLast,
		tLast: startTs,
		tFirst:  Max_Value,
	}
}

func (s *AbstractSlice) AddElement(element interface{}, ts int64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.tLast = Max(s.tLast, ts)
	s.tFirst = Min(s.tFirst, ts)
	s.cLast++
}

func (s *AbstractSlice) Merge(other Slice) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.tLast = Max(s.tLast, other.GetTLast())
	s.tFirst = Min(s.tFirst, other.GetTFirst())
	s.SetTEnd(Max(s.tEnd, other.GetTEnd()))
	s.GetAggState().Merge(other.GetAggState())
}

func (s *AbstractSlice) GetTLast() int64 {
	return s.tLast
}

func (s *AbstractSlice) SetTLast(tStart int64) {
	s.tLast = tStart
}

func (s *AbstractSlice) GetTEnd() int64 {
	return s.tEnd
}
func (s *AbstractSlice) GetTStart() int64 {
	return s.tStart
}

func (s *AbstractSlice) SetTStart(tStart int64) {
	s.tStart = tStart
}

func (s *AbstractSlice) SetTEnd(tEnd int64) {
	s.tEnd = tEnd
}

func (s *AbstractSlice) GetTFirst() int64 {
	return s.tFirst
}

func (s *AbstractSlice) SetType(t Type) {
	s.t = t
}

func (s *AbstractSlice) GetType() Type {
	return s.t
}

func (s *AbstractSlice) GetCStart() int64 {
	return s.cStart
}

func (s *AbstractSlice) GetCLast() int64 {
	return s.cLast
}

func (s *AbstractSlice) SetCLast(cLast int64) {
	s.cLast = cLast
}

func (s *AbstractSlice) SetTFirst(tFirst int64) {
	s.tFirst = tFirst
}

type EagerSlice struct {
	*AbstractSlice
	state *AggregateState
}

func NewEagerSlice(stateFactory StateFactory, windowManager *WindowManager, startTs, endTs, startC, endC int64, t Type) *EagerSlice {
	s := new(EagerSlice)
	s.AbstractSlice = NewAbstractSlice(startTs, endTs, startC, endC, t)
	s.state = NewAggregateState(stateFactory, windowManager.GetAggregations(), nil)
	return s
}

func (s *EagerSlice) AddElement(element interface{}, ts int64) {
	s.AbstractSlice.AddElement(element, ts)
	s.state.AddElement(element)
}

func (s *EagerSlice) GetAggState() *AggregateState {
	return s.state
}

func (s *EagerSlice) Merge(other Slice) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.tLast = Max(s.tLast, other.GetTLast())
	s.tFirst = Min(s.tFirst, other.GetTFirst())
	s.SetTEnd(Max(s.tEnd, other.GetTEnd()))
	s.GetAggState().Merge(other.GetAggState())
}

type LazySlice struct {
	*AbstractSlice
	state *AggregateState
	// Defines StreamRecord slice
	records SetState
}

func NewLazySlice(stateFactory StateFactory, windowManager *WindowManager, startTs, endTs, startC, endC int64, t Type) *LazySlice {
	s := new(LazySlice)
	s.records = stateFactory.CreateSetState(StreamRecordComparator)
	s.AbstractSlice = NewAbstractSlice(startTs, endTs, startC, endC, t)
	s.state = NewAggregateState(stateFactory, windowManager.GetAggregations(), s.records)
	return s
}

func (s *LazySlice) AddElement(element interface{}, ts int64) {
	s.AbstractSlice.AddElement(element, ts)
	s.state.AddElement(element)
	s.records.Add(NewStreamRecord(ts, element))
}

func (s *LazySlice) GetAggState() *AggregateState {
	return s.state
}

func (s *LazySlice) Merge(other Slice) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.tLast = Max(s.tLast, other.GetTLast())
	s.tFirst = Min(s.tFirst, other.GetTFirst())
	s.SetTEnd(Max(s.tEnd, other.GetTEnd()))
	s.GetAggState().Merge(other.GetAggState())
}

func (s *LazySlice) PrependElement(newElement StreamRecord) {
	s.AbstractSlice.AddElement(newElement.record, newElement.ts)
	s.records.Add(newElement)
	s.state.AddElement(newElement.record)
}

func (s *LazySlice) DropLastElement() StreamRecord {
	dropRecord := s.records.DropLast().(StreamRecord)
	s.SetCLast(s.GetCLast() - 1)
	if !s.records.IsEmpty() {
		currentLast := s.records.GetLast()
		if r, ok := currentLast.(StreamRecord); ok {
			s.SetTLast(r.ts)
		}
	}
	s.state.RemoveElement(dropRecord)
	return dropRecord
}

func (s *LazySlice) DropFirstElement() StreamRecord {
	dropRecord := s.records.DropFirst().(StreamRecord)
	currentFirst := s.records.GetFirst().(StreamRecord)
	s.SetCLast(s.GetCLast() - 1)
	s.SetTFirst(currentFirst.ts)
	s.state.RemoveElement(dropRecord)
	return dropRecord
}

func (s *LazySlice) GetRecords() SetState {
	return s.records
}

type StreamRecord struct {
	ts     int64
	record interface{}
}

func NewStreamRecord(ts int64, t interface{}) StreamRecord {
	return StreamRecord{ts: ts, record: t}
}

func (r StreamRecord) Equals(o interface{}) bool {
	if v, ok := o.(StreamRecord); ok {
		if v.record == r.record {
			return true
		}
	}
	return false
}

// StreamRecordComparator provides a fast comparison on StreamRecord
func StreamRecordComparator(a, b interface{}) int {
	s1 := a.(StreamRecord)
	s2 := b.(StreamRecord)
	return Int64Compare(s1.ts, s2.ts)
}

type SliceFactory struct {
	windowManager *WindowManager
	stateFactory  StateFactory
}

func NewSliceFactory(windowManager *WindowManager, stateFactory StateFactory) SliceFactory {
	return SliceFactory{windowManager: windowManager, stateFactory: stateFactory}
}

func (f SliceFactory) CreateSlice(startTs, maxValue, startCount, endCount int64, t Type) Slice {
	if !f.windowManager.HasCountMeasure() && (!f.windowManager.HasContextAwareWindow() || f.windowManager.IsSessionWindowCase()) && f.windowManager.GetMaxLateness() > 0 {
		return NewEagerSlice(f.stateFactory, f.windowManager, startTs, maxValue, startCount, endCount, t)
	}
	return NewLazySlice(f.stateFactory, f.windowManager, startTs, maxValue, startCount, endCount, t)
}

func (f SliceFactory) CreateSlice2(startTs, maxValue int64, t Type) Slice {
	if !f.windowManager.HasCountMeasure() && (!f.windowManager.HasContextAwareWindow() || f.windowManager.IsSessionWindowCase()) && f.windowManager.GetMaxLateness() > 0 {
		return NewEagerSlice(f.stateFactory, f.windowManager, startTs, maxValue, f.windowManager.GetCurrentCount(), f.windowManager.GetCurrentCount(), t)
	}
	return NewLazySlice(f.stateFactory, f.windowManager, startTs, maxValue, f.windowManager.GetCurrentCount(), f.windowManager.GetCurrentCount(), t)
}

type StreamSlicer struct {
	sliceManager     *SliceManager
	windowManager    *WindowManager
	maxEventTime     int64
	minNextEdgeTs    int64
	minNextEdgeCount int64
}

func NewStreamSlicer(sliceManager *SliceManager, windowManager *WindowManager) *StreamSlicer {
	return &StreamSlicer{
		sliceManager:  sliceManager,
		windowManager: windowManager,
		maxEventTime: Min_Value,
		minNextEdgeTs: Min_Value,
		minNextEdgeCount: Min_Value,
	}
}

// DetermineSlices processes every tuple in the data stream and checks if new slices have to be created
func (ss *StreamSlicer) DetermineSlices(te int64) {
	if ss.windowManager.HasCountMeasure() {
		if ss.minNextEdgeCount == Min_Value || ss.windowManager.GetCurrentCount() == ss.minNextEdgeCount {
			if ss.maxEventTime == Min_Value {
				ss.maxEventTime = te
			}
			ss.sliceManager.AppendSlice(ss.maxEventTime, NewFixed())
			ss.minNextEdgeCount = ss.calculateNextFixedEdgeCount()
		}
	}
	if ss.windowManager.HasTimeMeasure() {
		isInOrder := ss.isInOrder(te)
		// only need to check for slices if the record is in order
		if isInOrder {
			if ss.windowManager.HasFixedWindows() && ss.minNextEdgeTs == Min_Value {
				ss.minNextEdgeTs = ss.calculateNextFixedEdge(te)
			}
			flexCount := 0
			if ss.windowManager.HasContextAwareWindow() {
				flexCount = ss.calculateNextFlexEdge(te)
			}

			// Tumbling and Sliding windows
			for ss.windowManager.HasFixedWindows() && te > ss.minNextEdgeTs {
				if ss.minNextEdgeTs >= 0 {
					ss.sliceManager.AppendSlice(ss.minNextEdgeTs, NewFixed())
				}
				ss.minNextEdgeTs = ss.calculateNextFixedEdge(te)
			}

			// Emit remaining separator if needed
			if ss.minNextEdgeTs == te {
				if flexCount > 0 {
					ss.sliceManager.AppendSlice(ss.minNextEdgeTs, NewFlexible(flexCount))
				} else {
					ss.sliceManager.AppendSlice(ss.minNextEdgeTs, NewFixed())
				}
				ss.minNextEdgeTs = ss.calculateNextFixedEdge(te)
			} else if flexCount > 0 {
				ss.sliceManager.AppendSlice(te, NewFlexible(flexCount))
			}
		}
	}
	ss.windowManager.IncrementCount()
	ss.maxEventTime = Max(te, ss.maxEventTime)
}

func (ss *StreamSlicer) calculateNextFixedEdgeCount() int64 {
	// next_edge will be the last edge
	var currentMinEdge int64
	if ss.minNextEdgeCount == Min_Value {
		currentMinEdge = 0
	} else {
		currentMinEdge = ss.minNextEdgeCount
	}
	tC := Max(ss.windowManager.GetCurrentCount(), currentMinEdge)
	edge := Max_Value
	for _, tw := range ss.windowManager.GetContextFreeWindows() {
		if tw.GetMeasure() == Count {
			newNextEdge := tw.AssignNextWindowStart(tC)
			edge = Min(newNextEdge, edge)
		}
	}
	return edge

}

func (ss *StreamSlicer) calculateNextFixedEdge(te int64) int64 {
	currentMinEdge := ss.minNextEdgeTs
	tC := Max(te-ss.windowManager.GetMaxLateness(), currentMinEdge)
	edge := Max_Value
	for _, tw := range ss.windowManager.contextFreeWindows {
		if tw.GetMeasure() == Time {
			newNextEdge := tw.AssignNextWindowStart(tC)
			edge = Min(newNextEdge, edge)
		}
	}
	return edge
}

func (ss *StreamSlicer) isInOrder(te int64) bool {
	return te >= ss.maxEventTime
}

func (ss *StreamSlicer) calculateNextFlexEdge(te int64) int {
	tC := Max(ss.maxEventTime, ss.minNextEdgeTs)
	flexCount := 0
	for _, cw := range ss.windowManager.GetContextAwareWindows() {
		newNextEdge := cw.assignNextWindowStart(tC)
		if te >= newNextEdge {
			flexCount++
		}
	}
	return flexCount
}
