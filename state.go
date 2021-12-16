package window_processor

import (
	"github.com/emirpasic/gods/sets/treeset"
	"github.com/emirpasic/gods/utils"
)


type State interface {
	Clean()
	IsEmpty() bool
}

// ListState list value
type ListState interface {
	State
	Get() []interface{}
	Set(i int, value interface{})
	Add(value interface{})
}

// ValueState a single value
type ValueState interface {
	State
	Get() interface{}
	Set(value interface{})
}

// SetState extends a treeset, a sorted set
type SetState interface {
	State
	GetLast() interface{}
	GetFirst() interface{}
	DropLast() interface{}
	DropFirst() interface{}
	Add(record interface{})
	Range() []interface{}
}

type StateFactory interface {
	CreateValueState() ValueState
	CreateListState() ListState
	CreateSetState(comparator utils.Comparator) SetState
}

type MemoryListState struct {
	innerList []interface{}
}

func (s *MemoryListState) Clean() {
	s.innerList = s.innerList[:0]
}

func (s *MemoryListState) IsEmpty() bool {
	return len(s.innerList) == 0
}

func (s *MemoryListState) Get() []interface{} {
	return s.innerList
}

func (s *MemoryListState) Set(i int, value interface{}) {
	s.innerList[i] = value
}

func (s *MemoryListState) Add(value interface{}) {
	s.innerList = append(s.innerList, value)
}

type MemorySetState struct {
	values *treeset.Set
}

func (s *MemorySetState) Clean() {
	s.values.Clear()
}

func (s *MemorySetState) IsEmpty() bool {
	return s.values.Empty()
}

func (s *MemorySetState) GetLast() interface{} {
	return s.values.Values()[s.values.Size()-1]
}

func (s *MemorySetState) GetFirst() interface{} {
	return s.values.Values()[0]
}

func (s *MemorySetState) DropLast() interface{} {
	v := s.GetLast()
	s.values.Remove(v)
	return v
}

func (s *MemorySetState) DropFirst() interface{} {
	v := s.GetFirst()
	s.values.Remove(v)
	return v
}

func (s *MemorySetState) Add(record interface{}) {
	s.values.Add(record)
}

func (s *MemorySetState) Range() []interface{} {
	return s.values.Values()
}

type MemoryValueState struct {
	value interface{}
}

func (s *MemoryValueState) Clean() {
	s.value = nil
}

func (s *MemoryValueState) IsEmpty() bool {
	return s.value == nil
}

func (s *MemoryValueState) Set(value interface{}) {
	s.value = value
}

func (s *MemoryValueState) Get() interface{} {
	return s.value
}

type MemoryStateFactory struct {
}

func (f MemoryStateFactory) CreateValueState() ValueState {
	return &MemoryValueState{}
}

func (f MemoryStateFactory) CreateListState() ListState {
	return &MemoryListState{}
}

func (f MemoryStateFactory) CreateSetState(comparator utils.Comparator) SetState {
	return &MemorySetState{
		values: treeset.NewWith(comparator),
	}
}

type AggregateValueState struct {
	partialAggregateState ValueState
	aggregateFunction     AggregateFunction
	recordSetState        SetState
}

func NewAggregateValueState(valueState ValueState, aggregateFunction AggregateFunction, recordSet SetState) *AggregateValueState {
	return &AggregateValueState{
		partialAggregateState: valueState,
		aggregateFunction:     aggregateFunction,
		recordSetState:        recordSet,
	}
}

func (s *AggregateValueState) AddElement(element interface{}) {
	if s.partialAggregateState.IsEmpty() || s.partialAggregateState.Get() == nil {
		liftedElement := s.aggregateFunction.Lift(element)
		s.partialAggregateState.Set(liftedElement)
	} else {
		combined := s.aggregateFunction.LiftAndCombine(s.partialAggregateState.Get(), element)
		s.partialAggregateState.Set(combined)
	}
}

func (s *AggregateValueState) RemoveElement(streamRecord StreamRecord) {
	if f, ok := s.aggregateFunction.(InvertibleAggregateFunction); ok {
		newPartial := f.ListAndInvert(s.partialAggregateState.Get(), streamRecord.record)
		s.partialAggregateState.Set(newPartial)
	} else {

	}
}

func (s *AggregateValueState) recompute() {
	s.clear()
	for _, stream := range s.recordSetState.Range() {
		if streamRecord, ok := stream.(StreamRecord); ok {
			s.AddElement(streamRecord)
		}
	}
}

func (s *AggregateValueState) clear() {
	s.partialAggregateState.Clean()
}

func (s *AggregateValueState) Merge(other *AggregateValueState) {
	otherValueState := other.partialAggregateState
	if s.partialAggregateState.IsEmpty() && !otherValueState.IsEmpty() {
		otherValue := otherValueState.Get()
		if f, ok := s.aggregateFunction.(CloneablePartialStateFunction); ok {
			otherValue = f.Clone(otherValue)
		}
		s.partialAggregateState.Set(otherValue)
	} else if !otherValueState.IsEmpty() {
		merged := s.aggregateFunction.Combine(s.partialAggregateState.Get(), otherValueState.Get())
		s.partialAggregateState.Set(merged)
	}
}

func (s *AggregateValueState) HasValue() bool {
	return !s.partialAggregateState.IsEmpty()
}

func (s *AggregateValueState) GetValue() interface{} {
	if s.partialAggregateState.Get() != nil {
		return s.aggregateFunction.Lower(s.partialAggregateState.Get())
	}
	return nil
}

type AggregateState struct {
	aggregateValueStates []*AggregateValueState
}

func NewAggregateState(stateFactory StateFactory, windowFunctions []AggregateFunction, records SetState) *AggregateState {
	s := new(AggregateState)
	for _, f := range windowFunctions {
		s.aggregateValueStates = append(s.aggregateValueStates, NewAggregateValueState(stateFactory.CreateValueState(), f, records))
	}
	return s
}

func (s *AggregateState) AddElement(state interface{}) {
	for _, valueState := range s.aggregateValueStates {
		valueState.AddElement(state)
	}
}

func (s *AggregateState) RemoveElement(toRemove StreamRecord) {
	for _, valueState := range s.aggregateValueStates {
		valueState.RemoveElement(toRemove)
	}
}

func (s *AggregateState) Clear() {
	for _, valueState := range s.aggregateValueStates {
		valueState.clear()
	}
}

func (s *AggregateState) Merge(otherAggState *AggregateState) {
	if s.isMergeable(otherAggState) {
		for i, valueState := range otherAggState.aggregateValueStates {
			s.aggregateValueStates[i].Merge(valueState)
		}
	}
}

func (s *AggregateState) isMergeable(otherAggState *AggregateState) bool {
	return len(otherAggState.aggregateValueStates) <= len(s.aggregateValueStates)
}

func (s *AggregateState) HasValues() bool {
	for _, valueState := range s.aggregateValueStates {
		if valueState.HasValue() {
			return true
		}
	}
	return false
}

func (s *AggregateState) GetValues() []interface{} {
	var objList []interface{}
	for _, valueState := range s.aggregateValueStates {
		if valueState.HasValue() {
			objList = append(objList, valueState.GetValue())
		}
	}
	return objList
}

type AggregateWindowState struct {
	start       int64
	endTs       int64
	measure     WindowMeasure
	windowState *AggregateState
}

func NewAggregateWindowState(startTs, endTs int64, measure WindowMeasure, stateFactory StateFactory, windowFunctionList []AggregateFunction) *AggregateWindowState {
	return &AggregateWindowState{
		start:       startTs,
		endTs:       endTs,
		measure:     measure,
		windowState: NewAggregateState(stateFactory, windowFunctionList, nil),
	}
}

func (s *AggregateWindowState) containsSlice(currentSlice Slice) bool {
	if s.measure == Time {
		return s.GetStart() <= currentSlice.GetTStart() && s.GetEnd() > currentSlice.GetTLast()
	} else {
		return s.GetStart() <= currentSlice.GetCLast() && s.GetEnd() >= currentSlice.GetCLast()
	}
}

func (s *AggregateWindowState) GetMeasure() WindowMeasure {
	return s.measure
}

func (s *AggregateWindowState) GetStart() int64 {
	return s.start
}

func (s *AggregateWindowState) GetEnd() int64 {
	return s.endTs
}

func (s *AggregateWindowState) GetAggValues() []interface{} {
	return s.windowState.GetValues()
}

func (s *AggregateWindowState) HasValue() bool {
	return s.windowState.HasValues()
}

func (s *AggregateWindowState) AddState(aggState *AggregateState) {
	s.windowState.Merge(aggState)
}
