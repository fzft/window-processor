package window_processor

import "sync"

type Type interface {
	IsMovable() bool
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
	GetAggState() AggregateState
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

func (s *AbstractSlice) GetAggState() AggregateState {
	panic("implement me")
}

func NewAbstractSlice(startTs, endTs, cStart, cLast int64, t Type) *AbstractSlice {
	return &AbstractSlice{
		t:      t,
		tStart: startTs,
		tEnd:   endTs,
		cStart: cStart,
		cLast:  cLast,
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

type EagerSlice struct {
	AbstractSlice
	state AggregateState
}

func NewEagerSlice(stateFactory StateFactory)

func (s *EagerSlice) AddElement(element interface{}, ts int64) {
	s.AbstractSlice.AddElement(element, ts)

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

func (r StreamRecord) CompareTo(o StreamRecord) bool {
	return r.ts > o.ts
}
