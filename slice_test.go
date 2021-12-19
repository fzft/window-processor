package window_processor

import (
	"github.com/emirpasic/gods/sets/hashset"
	"github.com/stretchr/testify/assert"
	"testing"
)

type WindowTestContext struct {
	windowContext *WindowContext
	windowMeasure WindowMeasure
}

func NewWindowTestContext(windowMeasure WindowMeasure) *WindowTestContext {
	c := new(WindowTestContext)
	c.windowMeasure = windowMeasure
	c.windowContext = NewWindowContext()
	return c
}

func (t *WindowTestContext) hasActiveWindows() bool {
	return t.windowContext.hasActiveWindows()
}

func (t *WindowTestContext) getActiveWindows() []*ActiveWindow {
	return t.windowContext.getActiveWindows()
}

func (t *WindowTestContext) getWindow(i int) *ActiveWindow {
	return t.windowContext.getWindow(i)
}

func (t *WindowTestContext) mergeWithPre(index int) *ActiveWindow {
	return t.windowContext.mergeWithPre(index)
}

func (t *WindowTestContext) addNewWindow(i int, start, end int64) *ActiveWindow {
	return t.windowContext.addNewWindow(i, start, end)
}

func (t *WindowTestContext) updateContext1(tuple interface{}, position int64, modifiedWindowEdges *hashset.Set) *ActiveWindow {
	if modifiedWindowEdges != nil {
		t.windowContext.modifiedWindowEdges = modifiedWindowEdges
	}
	return t.updateContext2(tuple, position)
}

func (t *WindowTestContext) removeWindow(index int) {
	t.windowContext.removeWindow(index)
}

func (t *WindowTestContext) shiftStart(w *ActiveWindow, pos int64) {
	t.windowContext.shiftStart(w, pos)
}

func (t *WindowTestContext) numberOfActiveWindows() int {
	return t.windowContext.numberOfActiveWindows()
}

func (t *WindowTestContext) shiftEnd(w *ActiveWindow, pos int64) {
	t.windowContext.shiftEnd(w, pos)
}

func (t *WindowTestContext) updateContext2(tuple interface{}, position int64) *ActiveWindow {
	index := t.getWindowIndex(position)
	if index == -1 {
		return t.addNewWindow(0, position-position%10, position+10-position%10)
	} else if position%5 != 0 && position > t.getWindow(index).getEnd() {
		return t.addNewWindow(index+1, position-position%10, position+10-position%10)
	}

	if position == 5 {
		t.shiftStart(t.getWindow(index+1), position)
	} else if position == 15 {
		t.shiftStart(t.getWindow(index), position)
	} else if position == 25 {
		return t.addNewWindow(index, position, position+10-position%10)
	} else if position == 35 {
		return t.mergeWithPre(index)
	}
	return nil
}

func (t *WindowTestContext) assignNextWindowStart(position int64) int64 {
	return position + 10 - position%10
}

func (t *WindowTestContext) triggerWindows(aggregateWindows WindowCollector, lastWatermark, currentWatermark int64) {
	w := t.getWindow(0)
	for w.getEnd() <= currentWatermark {
		aggregateWindows.Trigger(w.getStart(), w.getEnd(), t.windowMeasure)
		t.removeWindow(0)
		if t.hasActiveWindows() {
			return
		}
		w = t.getWindow(0)
	}
}

func (t *WindowTestContext) getWindowIndex(position int64) int {
	var i int
	for ; i < t.numberOfActiveWindows(); i++ {
		s := t.getWindow(i)
		if s.getStart() <= position && s.getEnd() > position {
			return i
		}
	}
	return i - 1
}

type TestWindow struct {
	windowMeasure WindowMeasure
}

func NewTestWindow(windowMeasure WindowMeasure) *TestWindow {
	return &TestWindow{windowMeasure}
}

func (w *TestWindow) GetMeasure() WindowMeasure {
	return w.windowMeasure
}

func (w *TestWindow) CreateContext() Context {
	return NewWindowTestContext(w.windowMeasure)
}

type ReduceAggregateFunctionTest struct {
}

func (r ReduceAggregateFunctionTest) Lift(i interface{}) interface{} {
	return i
}

func (r ReduceAggregateFunctionTest) Combine(partialAggregate1, partialAggregate2 interface{}) interface{} {
	v1 := partialAggregate1.(int)
	v2 := partialAggregate2.(int)
	return v1 + v2
}

func (r ReduceAggregateFunctionTest) LiftAndCombine(partialAggregateType interface{}, inputTuple interface{}) interface{} {
	lifted := r.Lift(inputTuple)
	return r.Combine(partialAggregateType, lifted)
}

func (r ReduceAggregateFunctionTest) Lower(i interface{}) interface{} {
	return i
}

type ReduceAggregateFunctionTest2 struct {
}

func (r ReduceAggregateFunctionTest2) Lift(i interface{}) interface{} {
	return i
}

func (r ReduceAggregateFunctionTest2) Combine(partialAggregate1, partialAggregate2 interface{}) interface{} {
	v1 := partialAggregate1.(int)
	v2 := partialAggregate2.(int)
	return v1 - v2
}

func (r ReduceAggregateFunctionTest2) LiftAndCombine(partialAggregateType interface{}, inputTuple interface{}) interface{} {
	lifted := r.Lift(inputTuple)
	return r.Combine(partialAggregateType, lifted)
}

func (r ReduceAggregateFunctionTest2) Lower(i interface{}) interface{} {
	return i
}

type LazyAggregateStoreTest struct {
	aggregationStore AggregationStore
	stateFactory     StateFactory
	windowManager    *WindowManager
	sliceFactory     SliceFactory
	sliceManager     *SliceManager
}

func SliceSetupTest(tb testing.TB) (func(tb testing.TB), LazyAggregateStoreTest) {
	tb.Log("lazy aggregate store setup")

	lazyAggregateStoreTest := LazyAggregateStoreTest{}
	lazyAggregateStoreTest.aggregationStore = &LazyAggregateStore{}
	lazyAggregateStoreTest.stateFactory = MemoryStateFactory{}
	lazyAggregateStoreTest.windowManager = NewWindowManager(lazyAggregateStoreTest.stateFactory, lazyAggregateStoreTest.aggregationStore)
	lazyAggregateStoreTest.sliceFactory = NewSliceFactory(lazyAggregateStoreTest.windowManager, lazyAggregateStoreTest.stateFactory)
	lazyAggregateStoreTest.windowManager.addAggregation(ReduceAggregateFunctionTest{})
	lazyAggregateStoreTest.sliceManager = NewSliceManager(lazyAggregateStoreTest.sliceFactory, lazyAggregateStoreTest.aggregationStore, lazyAggregateStoreTest.windowManager)

	return func(tb testing.TB) {
		tb.Log("lazy aggregate store teardown")
	}, lazyAggregateStoreTest
}

func TestLazySlice(t *testing.T) {
	teardownTest, store := SliceSetupTest(t)
	defer teardownTest(t)

	store.windowManager.addWindowAssigner(NewTestWindow(Time))

	// out of order stream
	assert.True(t, store.windowManager.GetMaxLateness() > 0)
	// no context free or session window
	assert.True(t, store.windowManager.HasContextAwareWindow())
	assert.False(t, store.windowManager.IsSessionWindowCase())

	slice := store.sliceFactory.CreateSlice2(0, 10, NewFixed())
	_, ok := slice.(*LazySlice)
	assert.True(t, ok)
}

func TestEagerSlice_Session(t *testing.T) {
	teardownTest, store := SliceSetupTest(t)
	defer teardownTest(t)

	store.windowManager.addWindowAssigner(NewSessionWindow(Time, 1000))
	// out of order stream
	assert.True(t, store.windowManager.GetMaxLateness() > 0)
	// no context free or session window
	assert.True(t, store.windowManager.HasContextAwareWindow())
	assert.True(t, store.windowManager.IsSessionWindowCase())
	assert.False(t, store.windowManager.HasCountMeasure())

	slice := store.sliceFactory.CreateSlice2(0, 10, NewFixed())
	_, ok := slice.(*EagerSlice)
	assert.True(t, ok)
}

func TestLazySlice_ContextAware(t *testing.T) {
	teardownTest, store := SliceSetupTest(t)
	defer teardownTest(t)
	store.windowManager.addWindowAssigner(NewSessionWindow(Time, 1000))
	store.windowManager.addWindowAssigner(NewTestWindow(Time))
	// out of order stream
	assert.True(t, store.windowManager.GetMaxLateness() > 0)
	// no context free or session window
	assert.True(t, store.windowManager.HasContextAwareWindow())
	// no special case of session window because of other context aware window
	assert.False(t, store.windowManager.IsSessionWindowCase())
	slice := store.sliceFactory.CreateSlice2(0, 10, NewFixed())
	_, ok := slice.(*LazySlice)
	assert.True(t, ok)
}

// TestSliceManager_ShiftLowerModification Shift the end of a slice to a lower timestamp and move tuples to correct slice
func TestSliceManager_ShiftLowerModification(t *testing.T) {
	teardownTest, store := SliceSetupTest(t)
	defer teardownTest(t)
	store.windowManager.addWindowAssigner(NewTestWindow(Time))

	store.aggregationStore.AppendSlice(store.sliceFactory.CreateSlice2(0, 10, NewFlexible(1)))
	store.sliceManager.ProcessElement(1, 1)
	store.sliceManager.ProcessElement(1, 4)
	store.sliceManager.ProcessElement(1, 8)
	store.sliceManager.ProcessElement(1, 9)

	store.aggregationStore.AppendSlice(store.sliceFactory.CreateSlice2(10, 20, NewFlexible(1)))
	store.sliceManager.ProcessElement(1, 14)
	store.sliceManager.ProcessElement(1, 19)

	store.aggregationStore.AppendSlice(store.sliceFactory.CreateSlice2(20, 30, NewFlexible(1)))
	store.sliceManager.ProcessElement(1, 24)

	// out of order tuple
	// shift slice start from 10-20 to 5-20 and respectively 0-5 -- move record with ts 8 and 9 to next slice
	store.sliceManager.ProcessElement(1, 5)

	// slice 0-5
	assert.Equal(t, int64(0), store.aggregationStore.GetSlice(0).GetTStart())
	assert.Equal(t, int64(5), store.aggregationStore.GetSlice(0).GetTEnd())
	assert.Equal(t, int64(1), store.aggregationStore.GetSlice(0).GetTFirst())
	assert.Equal(t, int64(4), store.aggregationStore.GetSlice(0).GetTLast())

	// slice 5-20
	assert.Equal(t, int64(5), store.aggregationStore.GetSlice(1).GetTStart())
	assert.Equal(t, int64(20), store.aggregationStore.GetSlice(1).GetTEnd())
	assert.Equal(t, int64(5), store.aggregationStore.GetSlice(1).GetTFirst())
	assert.Equal(t, int64(19), store.aggregationStore.GetSlice(1).GetTLast())

	checkRecords([]int64{5, 8, 9, 14, 19}, store.aggregationStore.GetSlice(1).(*LazySlice).GetRecords().Range(), t)
}

// TestSliceManager_ShiftHigherModification Shift the end of a slice to a higher timestamp and move tuples to correct slice
func TestSliceManager_ShiftHigherModification(t *testing.T) {
	teardownTest, store := SliceSetupTest(t)
	defer teardownTest(t)
	store.windowManager.addWindowAssigner(NewTestWindow(Time))

	store.aggregationStore.AppendSlice(store.sliceFactory.CreateSlice2(0, 10, NewFlexible(1)))
	store.sliceManager.ProcessElement(1, 1)

	store.aggregationStore.AppendSlice(store.sliceFactory.CreateSlice2(10, 20, NewFlexible(1)))
	store.sliceManager.ProcessElement(1, 12)
	store.sliceManager.ProcessElement(1, 14)
	store.sliceManager.ProcessElement(1, 19)

	store.aggregationStore.AppendSlice(store.sliceFactory.CreateSlice2(20, 30, NewFlexible(1)))
	store.sliceManager.ProcessElement(1, 24)

	// out of order tuple
	// shift slice end from 0-10 to 0-15 and respectively 15-20 -- move tuple with ts 12 and 14 to previous slice
	store.sliceManager.ProcessElement(1, 15)

	// slice 0-15
	assert.Equal(t, int64(0), store.aggregationStore.GetSlice(0).GetTStart())
	assert.Equal(t, int64(15), store.aggregationStore.GetSlice(0).GetTEnd())
	assert.Equal(t, int64(1), store.aggregationStore.GetSlice(0).GetTFirst())
	assert.Equal(t, int64(14), store.aggregationStore.GetSlice(0).GetTLast())

	// slice 15-20
	assert.Equal(t, int64(15), store.aggregationStore.GetSlice(1).GetTStart())
	assert.Equal(t, int64(20), store.aggregationStore.GetSlice(1).GetTEnd())
	assert.Equal(t, int64(15), store.aggregationStore.GetSlice(1).GetTFirst())
	assert.Equal(t, int64(19), store.aggregationStore.GetSlice(1).GetTLast())

	checkRecords([]int64{1, 12, 14, 15}, store.aggregationStore.GetSlice(0).(*LazySlice).GetRecords().Range(), t)
}

// TestSliceManager_ShiftModificationSplit Split slice into two slices due to a ShiftModification (Slice not Movable) and move tuples into new slice
func TestSliceManager_ShiftModificationSplit(t *testing.T) {
	teardownTest, store := SliceSetupTest(t)
	defer teardownTest(t)
	store.windowManager.addWindowAssigner(NewTestWindow(Time))

	store.aggregationStore.AppendSlice(store.sliceFactory.CreateSlice2(0, 10, NewFlexible(2)))
	sliceType := store.aggregationStore.GetSlice(0).GetType()
	assert.False(t, sliceType.IsMovable())

	store.sliceManager.ProcessElement(1, 1)
	store.sliceManager.ProcessElement(1, 4)
	store.sliceManager.ProcessElement(1, 8)
	store.sliceManager.ProcessElement(1, 9)

	store.aggregationStore.AppendSlice(store.sliceFactory.CreateSlice2(10, 20, NewFlexible(2)))
	store.sliceManager.ProcessElement(1, 14)
	store.sliceManager.ProcessElement(1, 19)

	store.aggregationStore.AppendSlice(store.sliceFactory.CreateSlice2(20, 30, NewFlexible(2)))

	store.sliceManager.ProcessElement(1, 24)

	// shift slice start from 10-20 to 5-20 and respectively split slice into 0-5 and 5-10 -- move tuple with ts 8 and 9
	store.sliceManager.ProcessElement(1, 5)

	// slice 0-5
	assert.Equal(t, int64(0), store.aggregationStore.GetSlice(0).GetTStart())
	assert.Equal(t, int64(5), store.aggregationStore.GetSlice(0).GetTEnd())
	assert.Equal(t, int64(1), store.aggregationStore.GetSlice(0).GetTFirst())
	assert.Equal(t, int64(4), store.aggregationStore.GetSlice(0).GetTLast())

	// add new slice: slice 5 - 10
	assert.Equal(t, int64(5), store.aggregationStore.GetSlice(1).GetTStart())
	assert.Equal(t, int64(10), store.aggregationStore.GetSlice(1).GetTEnd())
	assert.Equal(t, int64(5), store.aggregationStore.GetSlice(1).GetTFirst())
	assert.Equal(t, int64(9), store.aggregationStore.GetSlice(1).GetTLast())

	// slice 10-20
	assert.Equal(t, int64(10), store.aggregationStore.GetSlice(2).GetTStart())
	assert.Equal(t, int64(20), store.aggregationStore.GetSlice(2).GetTEnd())
	assert.Equal(t, int64(14), store.aggregationStore.GetSlice(2).GetTFirst())
	assert.Equal(t, int64(19), store.aggregationStore.GetSlice(2).GetTLast())

	checkRecords([]int64{5, 8, 9}, store.aggregationStore.GetSlice(1).(*LazySlice).GetRecords().Range(), t)
}

// TestSliceManager_ShiftModificationSplit2 Split slice into two slices due to a ShiftModification (Slice is not Movable because Flexible Slice counter != 1) and move tuples into new slice
func TestSliceManager_ShiftModificationSplit2(t *testing.T) {
	teardownTest, store := SliceSetupTest(t)
	defer teardownTest(t)
	store.windowManager.addWindowAssigner(NewTestWindow(Time))

	store.aggregationStore.AppendSlice(store.sliceFactory.CreateSlice2(0, 10, NewFlexible(2)))
	sliceType := store.aggregationStore.GetSlice(0).GetType()
	assert.False(t, sliceType.IsMovable())

	store.sliceManager.ProcessElement(1, 1)

	store.aggregationStore.AppendSlice(store.sliceFactory.CreateSlice2(10, 20, NewFlexible(2)))
	store.sliceManager.ProcessElement(1, 12)
	store.sliceManager.ProcessElement(1, 14)
	store.sliceManager.ProcessElement(1, 17)
	store.sliceManager.ProcessElement(1, 19)

	store.aggregationStore.AppendSlice(store.sliceFactory.CreateSlice2(20, 30, NewFlexible(2)))

	store.sliceManager.ProcessElement(1, 24)

	// shift slice start from 10-20 to 5-20 and respectively split slice into 0-5 and 5-10 -- move tuple with ts 8 and 9
	store.sliceManager.ProcessElement(1, 15)

	// slice 0-10
	assert.Equal(t, int64(0), store.aggregationStore.GetSlice(0).GetTStart())
	assert.Equal(t, int64(10), store.aggregationStore.GetSlice(0).GetTEnd())
	assert.Equal(t, int64(1), store.aggregationStore.GetSlice(0).GetTFirst())
	assert.Equal(t, int64(1), store.aggregationStore.GetSlice(0).GetTLast())

	// add new slice: slice 10-15
	assert.Equal(t, int64(10), store.aggregationStore.GetSlice(1).GetTStart())
	assert.Equal(t, int64(15), store.aggregationStore.GetSlice(1).GetTEnd())
	assert.Equal(t, int64(12), store.aggregationStore.GetSlice(1).GetTFirst())
	assert.Equal(t, int64(14), store.aggregationStore.GetSlice(1).GetTLast())

	// slice 15-20
	assert.Equal(t, int64(15), store.aggregationStore.GetSlice(2).GetTStart())
	assert.Equal(t, int64(20), store.aggregationStore.GetSlice(2).GetTEnd())
	assert.Equal(t, int64(15), store.aggregationStore.GetSlice(2).GetTFirst())
	assert.Equal(t, int64(19), store.aggregationStore.GetSlice(2).GetTLast())

	checkRecords([]int64{15, 17, 19}, store.aggregationStore.GetSlice(2).(*LazySlice).GetRecords().Range(), t)
}

// TestSliceManager_AddModificationSplit Split one slice into two slices due to an AddModification and move tuples into new slice
func TestSliceManager_AddModificationSplit(t *testing.T) {
	teardownTest, store := SliceSetupTest(t)
	defer teardownTest(t)
	store.windowManager.addWindowAssigner(NewTestWindow(Time))

	store.aggregationStore.AppendSlice(store.sliceFactory.CreateSlice2(0, 10, NewFlexible(1)))

	store.sliceManager.ProcessElement(1, 1)

	store.aggregationStore.AppendSlice(store.sliceFactory.CreateSlice2(10, 20, NewFlexible(1)))
	store.sliceManager.ProcessElement(1, 14)
	store.sliceManager.ProcessElement(1, 19)

	store.aggregationStore.AppendSlice(store.sliceFactory.CreateSlice2(20, 30, NewFlexible(1)))

	store.sliceManager.ProcessElement(1, 22)
	store.sliceManager.ProcessElement(1, 24)
	store.sliceManager.ProcessElement(1, 26)
	store.sliceManager.ProcessElement(1, 27)

	// split slice 20-30 into 20-25 and add new slice 25-30 -- move tuples with ts 26 and 27 to new slice
	store.sliceManager.ProcessElement(1, 25)

	// slice 20-25
	assert.Equal(t, int64(20), store.aggregationStore.GetSlice(2).GetTStart())
	assert.Equal(t, int64(25), store.aggregationStore.GetSlice(2).GetTEnd())
	assert.Equal(t, int64(22), store.aggregationStore.GetSlice(2).GetTFirst())
	assert.Equal(t, int64(24), store.aggregationStore.GetSlice(2).GetTLast())

	// slice 25-30
	assert.Equal(t, int64(25), store.aggregationStore.GetSlice(3).GetTStart())
	assert.Equal(t, int64(30), store.aggregationStore.GetSlice(3).GetTEnd())
	assert.Equal(t, int64(25), store.aggregationStore.GetSlice(3).GetTFirst())
	assert.Equal(t, int64(27), store.aggregationStore.GetSlice(3).GetTLast())

	checkRecords([]int64{25, 26, 27, 30}, store.aggregationStore.GetSlice(3).(*LazySlice).GetRecords().Range(), t)
}

// TestSliceManager_DeleteModification DeleteModification: merge slice
func TestSliceManager_DeleteModification(t *testing.T) {
	teardownTest, store := SliceSetupTest(t)
	defer teardownTest(t)
	store.windowManager.addWindowAssigner(NewTestWindow(Time))

	store.aggregationStore.AppendSlice(store.sliceFactory.CreateSlice2(0, 10, NewFlexible(1)))

	store.sliceManager.ProcessElement(1, 1)

	store.aggregationStore.AppendSlice(store.sliceFactory.CreateSlice2(10, 20, NewFlexible(1)))
	store.sliceManager.ProcessElement(1, 14)
	store.sliceManager.ProcessElement(1, 19)

	store.aggregationStore.AppendSlice(store.sliceFactory.CreateSlice2(20, 30, NewFlexible(1)))
	store.sliceManager.ProcessElement(1, 24)
	store.aggregationStore.AppendSlice(store.sliceFactory.CreateSlice2(30, 35, NewFlexible(1)))
	store.sliceManager.ProcessElement(1, 31)
	store.sliceManager.ProcessElement(1, 33)
	store.aggregationStore.AppendSlice(store.sliceFactory.CreateSlice2(35, 45, NewFlexible(1)))
	store.sliceManager.ProcessElement(1, 38)

	// merge slice 20-30 and 30-35
	store.sliceManager.ProcessElement(1, 35)

	// slice 20-35
	assert.Equal(t, int64(20), store.aggregationStore.GetSlice(2).GetTStart())
	assert.Equal(t, int64(35), store.aggregationStore.GetSlice(2).GetTEnd())
	assert.Equal(t, int64(24), store.aggregationStore.GetSlice(2).GetTFirst())
	assert.Equal(t, int64(33), store.aggregationStore.GetSlice(2).GetTLast())

	// slice 35-45
	assert.Equal(t, int64(35), store.aggregationStore.GetSlice(3).GetTStart())
	assert.Equal(t, int64(45), store.aggregationStore.GetSlice(3).GetTEnd())
	assert.Equal(t, int64(35), store.aggregationStore.GetSlice(3).GetTFirst())
	assert.Equal(t, int64(38), store.aggregationStore.GetSlice(3).GetTLast())

	checkRecords([]int64{24, 31, 33}, store.aggregationStore.GetSlice(2).(*LazySlice).GetRecords().Range(), t)
}


func TestLazyAggregateStore_FindSliceByTs(t *testing.T) {
	teardownTest, store := SliceSetupTest(t)
	defer teardownTest(t)

	var list []Slice
	list = append(list, []Slice{
		store.sliceFactory.CreateSlice2(0, 10, NewFixed()),
		store.sliceFactory.CreateSlice2(10, 20, NewFixed()),
		store.sliceFactory.CreateSlice2(20, 30, NewFixed()),
		store.sliceFactory.CreateSlice2(40, 50, NewFixed()),
	}...)

	for _, slice := range list {
		store.aggregationStore.AppendSlice(slice)
	}

	for i, slice := range list {
		expectedSlice := slice
		assert.Equal(t, i, store.aggregationStore.FindSliceIndexByTimestamp(expectedSlice.GetTStart()))
		assert.Equal(t, i, store.aggregationStore.FindSliceIndexByTimestamp(expectedSlice.GetTEnd()-1))
		assert.Equal(t, i, store.aggregationStore.FindSliceIndexByTimestamp(expectedSlice.GetTStart()+5))
	}
}

func TestLazyAggregateStore_GetSliceByIndex(t *testing.T) {
	teardownTest, store := SliceSetupTest(t)
	defer teardownTest(t)

	var list []Slice
	list = append(list, []Slice{
		store.sliceFactory.CreateSlice2(0, 10, NewFixed()),
		store.sliceFactory.CreateSlice2(10, 20, NewFixed()),
		store.sliceFactory.CreateSlice2(20, 30, NewFixed()),
		store.sliceFactory.CreateSlice2(40, 50, NewFixed()),
	}...)
	for _, slice := range list {
		store.aggregationStore.AppendSlice(slice)
	}
	for i, slice := range list {
		assert.Equal(t, slice, store.aggregationStore.GetSlice(i))
	}

	assert.Equal(t, list[len(list)-1], store.aggregationStore.GetCurrentSlice())
}

func TestLazyAggregateStore_InsertValueToSlice(t *testing.T) {
	teardownTest, store := SliceSetupTest(t)
	defer teardownTest(t)

	var list []Slice
	list = append(list, []Slice{
		store.sliceFactory.CreateSlice2(0, 10, NewFixed()),
		store.sliceFactory.CreateSlice2(10, 20, NewFixed()),
		store.sliceFactory.CreateSlice2(20, 30, NewFixed()),
		store.sliceFactory.CreateSlice2(40, 50, NewFixed()),
	}...)
	for _, slice := range list {
		store.aggregationStore.AppendSlice(slice)
	}

	store.aggregationStore.InsertValueToSlice(1, 1, 14)
	store.aggregationStore.InsertValueToSlice(2, 2, 22)
	store.aggregationStore.InsertValueToCurrentSlice(2, 22)

	assert.Nil(t, store.aggregationStore.GetSlice(0).GetAggState().GetValues())
	assert.Equal(t, 1, store.aggregationStore.GetSlice(1).GetAggState().GetValues()[0])

}

func TestLazyAggregateStore_AggregateWindow(t *testing.T) {
	teardownTest, store := SliceSetupTest(t)
	defer teardownTest(t)

	var list []Slice
	list = append(list, []Slice{
		store.sliceFactory.CreateSlice2(0, 10, NewFixed()),
		store.sliceFactory.CreateSlice2(10, 20, NewFixed()),
		store.sliceFactory.CreateSlice2(20, 30, NewFixed()),
		store.sliceFactory.CreateSlice2(30, 40, NewFixed()),
	}...)
	for _, slice := range list {
		store.aggregationStore.AppendSlice(slice)
	}

	store.aggregationStore.InsertValueToSlice(1, 1, 14)
	store.aggregationStore.InsertValueToSlice(2, 2, 22)
	store.aggregationStore.InsertValueToCurrentSlice(3, 33)

	var window []*AggregateWindowState
	window = append(window, []*AggregateWindowState{
		NewAggregateWindowState(10, 40, Time, store.stateFactory, store.windowManager.GetAggregations()),
		NewAggregateWindowState(10, 20, Time, store.stateFactory, store.windowManager.GetAggregations()),
	}...)
}

func checkRecords(values []int64, sliceRecords []interface{}, t *testing.T) {
	for i, slice := range sliceRecords {
		if v, ok := slice.(StreamRecord); ok {
			assert.Equal(t, v.ts, values[i])
		}
	}
}
