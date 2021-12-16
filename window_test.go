package window_processor

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

type WindowTest struct {
	slicingWindowOperator *SlicingWindowOperator
	stateFactory          StateFactory
}

func WindowSetupTest(tb testing.TB) (func(tb testing.TB), WindowTest) {
	tb.Log("window test setup")

	window := WindowTest{}
	window.stateFactory = MemoryStateFactory{}
	window.slicingWindowOperator = NewSlicingWindowOperator(window.stateFactory)

	return func(tb testing.TB) {
		tb.Log("lazy aggregate store teardown")
	}, window
}

func TestFixedBandWindow_InOrder(t *testing.T) {
	teardownTest, window := WindowSetupTest(t)
	defer teardownTest(t)

	window.slicingWindowOperator.AddWindowFunction(ReduceAggregateFunctionTest{})
	window.slicingWindowOperator.AddWindowAssigner(NewFixedBandWindow(Time, 0, 10))

	window.slicingWindowOperator.ProcessElement(1, 0)
	window.slicingWindowOperator.ProcessElement(4, 0)
	window.slicingWindowOperator.ProcessElement(3, 20)
	window.slicingWindowOperator.ProcessElement(4, 30)
	window.slicingWindowOperator.ProcessElement(5, 40)

	resultWindows := window.slicingWindowOperator.ProcessWatermark(22)
	WindowAssertEquals(t, resultWindows[0], 0, 10, 5)
	resultWindows = window.slicingWindowOperator.ProcessWatermark(55)
	assert.True(t, len(resultWindows) == 0)
}

func TestFixedBandWindow_InOrder2(t *testing.T) {
	teardownTest, window := WindowSetupTest(t)
	defer teardownTest(t)

	window.slicingWindowOperator.AddWindowFunction(ReduceAggregateFunctionTest{})
	window.slicingWindowOperator.AddWindowAssigner(NewFixedBandWindow(Time, 18, 10))

	window.slicingWindowOperator.ProcessElement(1, 0)
	window.slicingWindowOperator.ProcessElement(2, 0)
	window.slicingWindowOperator.ProcessElement(3, 20)
	window.slicingWindowOperator.ProcessElement(4, 30)
	window.slicingWindowOperator.ProcessElement(5, 40)

	resultWindows := window.slicingWindowOperator.ProcessWatermark(22)
	assert.True(t, len(resultWindows) == 0)
	resultWindows = window.slicingWindowOperator.ProcessWatermark(55)
	WindowAssertEquals(t, resultWindows[0], 18, 28, 3)
}

func TestFixedBandWindow_InOrder3(t *testing.T) {
	teardownTest, window := WindowSetupTest(t)
	defer teardownTest(t)

	window.slicingWindowOperator.AddWindowFunction(ReduceAggregateFunctionTest{})
	window.slicingWindowOperator.AddWindowAssigner(NewFixedBandWindow(Time, 1, 10))

	window.slicingWindowOperator.ProcessElement(1, 1)
	window.slicingWindowOperator.ProcessElement(2, 19)
	window.slicingWindowOperator.ProcessElement(3, 29)
	window.slicingWindowOperator.ProcessElement(4, 39)
	window.slicingWindowOperator.ProcessElement(5, 49)

	resultWindows := window.slicingWindowOperator.ProcessWatermark(22)
	WindowAssertEquals(t, resultWindows[0], 1, 11, 1)
}

func TestFixedBandWindow_InOrderTwoWindows(t *testing.T) {
	teardownTest, window := WindowSetupTest(t)
	defer teardownTest(t)

	window.slicingWindowOperator.AddWindowFunction(ReduceAggregateFunctionTest{})
	window.slicingWindowOperator.AddWindowAssigner(NewFixedBandWindow(Time, 10, 10))
	window.slicingWindowOperator.AddWindowAssigner(NewFixedBandWindow(Time, 20, 10))

	window.slicingWindowOperator.ProcessElement(1, 1)
	window.slicingWindowOperator.ProcessElement(2, 19)
	window.slicingWindowOperator.ProcessElement(3, 29)
	window.slicingWindowOperator.ProcessElement(4, 39)
	window.slicingWindowOperator.ProcessElement(5, 49)

	resultWindows := window.slicingWindowOperator.ProcessWatermark(22)
	assert.Equal(t, 2, resultWindows[0].GetAggValues()[0])
	resultWindows = window.slicingWindowOperator.ProcessWatermark(55)
	assert.Equal(t, 3, resultWindows[0].GetAggValues()[0])
}

func TestFixedBandWindow_InOrderTwoWindows2(t *testing.T) {
	teardownTest, window := WindowSetupTest(t)
	defer teardownTest(t)

	window.slicingWindowOperator.AddWindowFunction(ReduceAggregateFunctionTest{})
	window.slicingWindowOperator.AddWindowAssigner(NewFixedBandWindow(Time, 14, 11))
	window.slicingWindowOperator.AddWindowAssigner(NewFixedBandWindow(Time, 23, 10))

	window.slicingWindowOperator.ProcessElement(1, 1)
	window.slicingWindowOperator.ProcessElement(2, 19)
	window.slicingWindowOperator.ProcessElement(3, 29)
	window.slicingWindowOperator.ProcessElement(4, 39)
	window.slicingWindowOperator.ProcessElement(5, 49)

	resultWindows := window.slicingWindowOperator.ProcessWatermark(26)
	assert.Equal(t, 2, resultWindows[0].GetAggValues()[0])
	resultWindows = window.slicingWindowOperator.ProcessWatermark(55)
	assert.Equal(t, 3, resultWindows[0].GetAggValues()[0])
}

func TestFixedBandWindow_InOrderTwoWindowsDynamic(t *testing.T) {
	teardownTest, window := WindowSetupTest(t)
	defer teardownTest(t)

	window.slicingWindowOperator.AddWindowFunction(ReduceAggregateFunctionTest{})
	window.slicingWindowOperator.AddWindowAssigner(NewFixedBandWindow(Time, 10, 10))

	window.slicingWindowOperator.ProcessElement(1, 1)
	window.slicingWindowOperator.ProcessElement(2, 19)

	window.slicingWindowOperator.AddWindowAssigner(NewFixedBandWindow(Time, 20, 10))

	window.slicingWindowOperator.ProcessElement(3, 29)
	window.slicingWindowOperator.ProcessElement(4, 39)
	window.slicingWindowOperator.ProcessElement(5, 49)

	resultWindows := window.slicingWindowOperator.ProcessWatermark(22)
	assert.Equal(t, 2, resultWindows[0].GetAggValues()[0])
	resultWindows = window.slicingWindowOperator.ProcessWatermark(55)
	assert.Equal(t, 3, resultWindows[0].GetAggValues()[0])
}

func TestFixedBandWindow_InOrderTwoWindowsDynamic2(t *testing.T) {
	teardownTest, window := WindowSetupTest(t)
	defer teardownTest(t)

	window.slicingWindowOperator.AddWindowFunction(ReduceAggregateFunctionTest{})
	window.slicingWindowOperator.AddWindowAssigner(NewFixedBandWindow(Time, 10, 10))

	window.slicingWindowOperator.ProcessElement(1, 1)
	window.slicingWindowOperator.ProcessElement(2, 19)
	resultWindows := window.slicingWindowOperator.ProcessWatermark(22)
	assert.Equal(t, 2, resultWindows[0].GetAggValues()[0])

	window.slicingWindowOperator.AddWindowAssigner(NewFixedBandWindow(Time, 20, 21))
	window.slicingWindowOperator.ProcessElement(3, 29)
	window.slicingWindowOperator.ProcessElement(4, 39)
	window.slicingWindowOperator.ProcessElement(5, 49)

	resultWindows = window.slicingWindowOperator.ProcessWatermark(55)
	assert.Equal(t, 7, resultWindows[0].GetAggValues()[0])
}

func TestFixedBandWindow_OutOfOrder(t *testing.T) {
	teardownTest, window := WindowSetupTest(t)
	defer teardownTest(t)

	window.slicingWindowOperator.AddWindowFunction(ReduceAggregateFunctionTest{})
	window.slicingWindowOperator.AddWindowAssigner(NewFixedBandWindow(Time, 10, 20))

	window.slicingWindowOperator.ProcessElement(1, 1)
	window.slicingWindowOperator.ProcessElement(1, 29)

	// out of order tuples have to be inserted into the window
	window.slicingWindowOperator.ProcessElement(1, 20)
	window.slicingWindowOperator.ProcessElement(1, 23)
	window.slicingWindowOperator.ProcessElement(1, 25)

	window.slicingWindowOperator.ProcessElement(1, 45)

	resultWindows := window.slicingWindowOperator.ProcessWatermark(22)
	assert.True(t, len(resultWindows) == 0)

	resultWindows = window.slicingWindowOperator.ProcessWatermark(55)
	assert.Equal(t, 4, resultWindows[0].GetAggValues()[0])
}

func TestSessionWindow_InOrder(t *testing.T) {
	teardownTest, window := WindowSetupTest(t)
	defer teardownTest(t)

	window.slicingWindowOperator.AddWindowFunction(ReduceAggregateFunctionTest{})
	window.slicingWindowOperator.AddWindowAssigner(NewSessionWindow(Time, 10))

	window.slicingWindowOperator.ProcessElement(1, 1)
	window.slicingWindowOperator.ProcessElement(2, 19)
	window.slicingWindowOperator.ProcessElement(3, 23)
	window.slicingWindowOperator.ProcessElement(4, 31)
	window.slicingWindowOperator.ProcessElement(5, 49)


	resultWindows := window.slicingWindowOperator.ProcessWatermark(22)
	assert.Equal(t, 1, resultWindows[0].GetAggValues()[0])

	resultWindows = window.slicingWindowOperator.ProcessWatermark(55)
	assert.Equal(t, 9, resultWindows[0].GetAggValues()[0])

	resultWindows = window.slicingWindowOperator.ProcessWatermark(80)
	assert.Equal(t, 5, resultWindows[0].GetAggValues()[0])

}

func TestSessionWindow_InOrderTestClean(t *testing.T) {
	teardownTest, window := WindowSetupTest(t)
	defer teardownTest(t)

	window.slicingWindowOperator.AddWindowFunction(ReduceAggregateFunctionTest{})
	window.slicingWindowOperator.AddWindowAssigner(NewSessionWindow(Time, 10000))

	window.slicingWindowOperator.ProcessElement(1, 1000)
	window.slicingWindowOperator.ProcessElement(2, 19000)
	window.slicingWindowOperator.ProcessElement(3, 23000)
	window.slicingWindowOperator.ProcessElement(4, 31000)
	window.slicingWindowOperator.ProcessElement(5, 49000)


	resultWindows := window.slicingWindowOperator.ProcessWatermark(22000)
	assert.Equal(t, 1, resultWindows[0].GetAggValues()[0])

	resultWindows = window.slicingWindowOperator.ProcessWatermark(55000)
	assert.Equal(t, 9, resultWindows[0].GetAggValues()[0])

	resultWindows = window.slicingWindowOperator.ProcessWatermark(80000)
	assert.Equal(t, 5, resultWindows[0].GetAggValues()[0])

}

func TestSessionWindow_InOrderTest2(t *testing.T) {
	teardownTest, window := WindowSetupTest(t)
	defer teardownTest(t)

	window.slicingWindowOperator.AddWindowFunction(ReduceAggregateFunctionTest{})
	window.slicingWindowOperator.AddWindowAssigner(NewSessionWindow(Time, 10))

	window.slicingWindowOperator.ProcessElement(1, 0)
	window.slicingWindowOperator.ProcessElement(2, 0)
	window.slicingWindowOperator.ProcessElement(3, 20)
	window.slicingWindowOperator.ProcessElement(4, 31)
	window.slicingWindowOperator.ProcessElement(5, 42)


	resultWindows := window.slicingWindowOperator.ProcessWatermark(22)
	assert.Equal(t, 3, resultWindows[0].GetAggValues()[0])
	resultWindows = window.slicingWindowOperator.ProcessWatermark(55)
	assert.Equal(t, 3, resultWindows[0].GetAggValues()[0])
	assert.Equal(t, 4, resultWindows[1].GetAggValues()[0])
	assert.Equal(t, 5, resultWindows[2].GetAggValues()[0])

}

func TestSessionWindow_OutOfOrderTestSimpleInsert(t *testing.T) {
	teardownTest, window := WindowSetupTest(t)
	defer teardownTest(t)

	window.slicingWindowOperator.AddWindowFunction(ReduceAggregateFunctionTest{})
	window.slicingWindowOperator.AddWindowAssigner(NewSessionWindow(Time, 10))
	window.slicingWindowOperator.ProcessElement(1, 1)

	window.slicingWindowOperator.ProcessElement(1, 9)
	window.slicingWindowOperator.ProcessElement(1, 15)
	window.slicingWindowOperator.ProcessElement(1, 30)

	// out of order
	window.slicingWindowOperator.ProcessElement(1, 12)


	resultWindows := window.slicingWindowOperator.ProcessWatermark(50)
	WindowAssertEquals(t, resultWindows[0], 1, 25, 4)
	WindowAssertEquals(t, resultWindows[1], 30, 40, 1)
}

func TestSessionWindow_OutOfOrderTestRightInsert(t *testing.T) {
	teardownTest, window := WindowSetupTest(t)
	defer teardownTest(t)

	window.slicingWindowOperator.AddWindowFunction(ReduceAggregateFunctionTest{})
	window.slicingWindowOperator.AddWindowAssigner(NewSessionWindow(Time, 10))
	window.slicingWindowOperator.ProcessElement(1, 1)

	window.slicingWindowOperator.ProcessElement(1, 9)
	window.slicingWindowOperator.ProcessElement(1, 10)
	window.slicingWindowOperator.ProcessElement(1, 30)

	// out of order
	window.slicingWindowOperator.ProcessElement(1, 12)


	resultWindows := window.slicingWindowOperator.ProcessWatermark(50)
	WindowAssertEquals(t, resultWindows[0], 1, 22, 4)
	WindowAssertEquals(t, resultWindows[1], 30, 40, 1)
}




func WindowAssertEquals(t *testing.T, aggregateWindow AggregateWindow, start, end int64, value interface{}) {
	assert.Equal(t, start, aggregateWindow.GetStart())
	assert.Equal(t, end, aggregateWindow.GetEnd())
	assert.Equal(t, value, aggregateWindow.GetAggValues()[0])
}
