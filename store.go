package window_processor

type AggregationStore interface {
	// GetCurrentSlice get the newest slice
	GetCurrentSlice() Slice

	// FindSliceIndexByTimestamp look for the slice which contains this timestamp
	// the timestamp is >= slice start < slice end
	// return index of containing slice
	FindSliceIndexByTimestamp(ts int64) int

	// FindSliceIndexByCount ...
	FindSliceIndexByCount(count int64) int

	// GetSlice return slice for a given index
	GetSlice(index int) Slice

	// InsertValueToCurrentSlice insert the element to the current slice
	InsertValueToCurrentSlice(element interface{}, ts int64)

	// InsertValueToSlice ...
	InsertValueToSlice(index int, element interface{}, ts int64)

	// AppendSlice append a new slice as new current slice
	AppendSlice(newSlice Slice)

	// Size ...
	Size() int

	// IsEmpty true if AggregationStore contains no slices
	IsEmpty() bool

	// Aggregate generates the window aggregates
	// On every AggregateWindow the aggregated value is set
	Aggregate(aggregateWindows AggregationWindowCollector, minTs, maxTs, minCount, maxCount int64)

	// AddSlice add a new slice at a specific index
	AddSlice(index int, newSlice Slice)

	// MergeSlice merge two slice A and B happens in three steps:
	// 1. Update the end of A such that t end (A) <- t end (B)
	// 2. Update the aggregate of A such that a <- a ⊕ b
	// 3. Delete slice B , which is now merged into A
	MergeSlice(sliceIndex int)

	// FindSliceByEnd ...
	FindSliceByEnd(pre int64) int

	// RemoveSlices ...
	RemoveSlices(maxTimestamp int64)
}

type AggregationStoreFactory interface {
	CreateAggregationStore() AggregationStore
}

type LazyAggregateStore struct {
	slices []Slice
}

func (l *LazyAggregateStore) GetCurrentSlice() Slice {
	return l.slices[len(l.slices)-1]
}

func (l *LazyAggregateStore) FindSliceIndexByTimestamp(ts int64) int {
	for i := l.Size() - 1; i >= 0; i-- {
		currentSlice := l.GetSlice(i)
		if currentSlice.GetTStart() <= ts {
			return i
		}
	}
	return -1
}

func (l *LazyAggregateStore) FindSliceIndexByCount(count int64) int {
	for i := l.Size() - 1; i >= 0; i-- {
		currentSlice := l.GetSlice(i)
		if currentSlice.GetCStart() <= count {
			return i
		}
	}
	return -1
}

func (l *LazyAggregateStore) GetSlice(index int) Slice {
	return l.slices[index]
}

func (l *LazyAggregateStore) InsertValueToCurrentSlice(element interface{}, ts int64) {
	l.GetCurrentSlice().AddElement(element, ts)
}

func (l *LazyAggregateStore) InsertValueToSlice(index int, element interface{}, ts int64) {
	l.GetSlice(index).AddElement(element, ts)
}

func (l *LazyAggregateStore) AppendSlice(newSlice Slice) {
	l.slices = append(l.slices, newSlice)
}

func (l *LazyAggregateStore) Size() int {
	return len(l.slices)
}

func (l *LazyAggregateStore) IsEmpty() bool {
	return l.Size() == 0
}

func (l *LazyAggregateStore) Aggregate(aggregateWindows AggregationWindowCollector, minTs, maxTs, minCount, maxCount int64) {
	startIndex := Max(int64(l.FindSliceIndexByTimestamp(minTs)), 0)
	startIndex = Min(startIndex, int64(l.FindSliceIndexByCount(minCount)))
	endIndex := Min(int64(l.Size()-1), int64(l.FindSliceIndexByTimestamp(maxTs)))
	endIndex = Max(endIndex, int64(l.FindSliceIndexByCount(maxCount)))
	for i := startIndex; i <= endIndex; i++ {
		currentSlice := l.GetSlice(int(i))
		for _, window := range aggregateWindows.Range() {
			if ws, ok := window.(*AggregateWindowState); ok {
				if ws.containsSlice(currentSlice) {
					ws.AddState(currentSlice.GetAggState())
				}
			}
		}
	}
}

func (l *LazyAggregateStore) AddSlice(index int, newSlice Slice) {
	l.slices[index] = newSlice
}

func (l *LazyAggregateStore) MergeSlice(sliceIndex int) {
	sliceA := l.GetSlice(sliceIndex)
	sliceB := l.GetSlice(sliceIndex + 1)
	sliceA.Merge(sliceB)
	l.slices = append(l.slices[:sliceIndex+1], l.slices[:sliceIndex+1]...)
}

func (l *LazyAggregateStore) FindSliceByEnd(start int64) int {
	for i := l.Size() - 1; i >= 0; i-- {
		currentSlice := l.GetSlice(i)
		if currentSlice.GetTEnd() == start {
			return i
		}
	}
	return -1
}

func (l *LazyAggregateStore) RemoveSlices(maxTimestamp int64) {
	index := l.FindSliceIndexByTimestamp(maxTimestamp)
	if index <= 0 {
		return
	}
	l.slices = l.slices[index:]
}
