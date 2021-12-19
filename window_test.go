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

func TestSessionWindow_OutOfOrderTestSplitSlice(t *testing.T) {
	teardownTest, window := WindowSetupTest(t)
	defer teardownTest(t)

	window.slicingWindowOperator.AddWindowFunction(ReduceAggregateFunctionTest{})
	window.slicingWindowOperator.AddWindowAssigner(NewSessionWindow(Time, 10))
	window.slicingWindowOperator.ProcessElement(1, 1)

	window.slicingWindowOperator.ProcessElement(1, 30)

	// out of order
	window.slicingWindowOperator.ProcessElement(1, 12)

	resultWindows := window.slicingWindowOperator.ProcessWatermark(22)
	WindowAssertEquals(t, resultWindows[0], 1, 11, 1)
	resultWindows = window.slicingWindowOperator.ProcessWatermark(50)
	WindowAssertEquals(t, resultWindows[0], 12, 22, 1)
	WindowAssertEquals(t, resultWindows[1], 30, 40, 1)
}

func TestSessionWindow_OutOfOrderTestMergeSlice(t *testing.T) {
	teardownTest, window := WindowSetupTest(t)
	defer teardownTest(t)

	window.slicingWindowOperator.AddWindowFunction(ReduceAggregateFunctionTest{})
	window.slicingWindowOperator.AddWindowAssigner(NewSessionWindow(Time, 10))
	window.slicingWindowOperator.ProcessElement(1, 7)

	window.slicingWindowOperator.ProcessElement(1, 30)
	window.slicingWindowOperator.ProcessElement(1, 51)
	window.slicingWindowOperator.ProcessElement(1, 15)
	window.slicingWindowOperator.ProcessElement(1, 21)

	resultWindows := window.slicingWindowOperator.ProcessWatermark(70)
	WindowAssertEquals(t, resultWindows[0], 7, 40, 4)
	WindowAssertEquals(t, resultWindows[1], 51, 61, 1)
}

func TestSessionWindow_OutOfOrderCombinedSessionTumblingMegeSession(t *testing.T) {
	teardownTest, window := WindowSetupTest(t)
	defer teardownTest(t)

	window.slicingWindowOperator.AddWindowFunction(ReduceAggregateFunctionTest{})
	window.slicingWindowOperator.AddWindowAssigner(NewSessionWindow(Time, 10))
	window.slicingWindowOperator.AddWindowAssigner(NewTumblingWindow(Time, 40))
	window.slicingWindowOperator.ProcessElement(1, 7)

	window.slicingWindowOperator.ProcessElement(1, 22)
	window.slicingWindowOperator.ProcessElement(1, 51)

	// merge slice
	window.slicingWindowOperator.ProcessElement(1, 15)
	// add new session
	window.slicingWindowOperator.ProcessElement(1, 37)

	resultWindows := window.slicingWindowOperator.ProcessWatermark(70)
	WindowAssertEquals(t, resultWindows[0], 0, 40, 4)
	WindowAssertEquals(t, resultWindows[1], 7, 32, 3)
	WindowAssertEquals(t, resultWindows[2], 37, 47, 1)
	WindowAssertEquals(t, resultWindows[3], 51, 61, 1)
}

func TestSessionWindow_OutOfOrderCombinedSessionMultiSession(t *testing.T) {
	teardownTest, window := WindowSetupTest(t)
	defer teardownTest(t)

	window.slicingWindowOperator.AddWindowFunction(ReduceAggregateFunctionTest{})
	window.slicingWindowOperator.AddWindowAssigner(NewSessionWindow(Time, 10))
	window.slicingWindowOperator.AddWindowAssigner(NewSessionWindow(Time, 5))

	// events -> 20, 31, 33, 40, 50, 57
	// [20-25, 31-38, 40-45, 50-55, 57-62, 20-30, 31-67]
	window.slicingWindowOperator.ProcessElement(1, 20)
	window.slicingWindowOperator.ProcessElement(1, 40)
	window.slicingWindowOperator.ProcessElement(1, 50)
	window.slicingWindowOperator.ProcessElement(1, 57)
	window.slicingWindowOperator.ProcessElement(1, 33)
	window.slicingWindowOperator.ProcessElement(1, 31)

	resultWindows := window.slicingWindowOperator.ProcessWatermark(70)
	WindowAssertContains(t, resultWindows, 20, 25, 1)
	WindowAssertContains(t, resultWindows, 31, 38, 2)
	WindowAssertContains(t, resultWindows, 40, 45, 1)
	WindowAssertContains(t, resultWindows, 50, 55, 1)
	WindowAssertContains(t, resultWindows, 57, 62, 1)
	WindowAssertContains(t, resultWindows, 20, 30, 1)
	WindowAssertContains(t, resultWindows, 31, 67, 5)
}

func TestSlidingWindow_InOrder(t *testing.T) {
	teardownTest, window := WindowSetupTest(t)
	defer teardownTest(t)

	window.slicingWindowOperator.AddWindowFunction(ReduceAggregateFunctionTest{})
	window.slicingWindowOperator.AddWindowAssigner(NewSlidingWindow(Time, 10, 5))

	// 1-10 ; 5-15; 10-20;
	window.slicingWindowOperator.ProcessElement(1, 1)
	window.slicingWindowOperator.ProcessElement(2, 19)
	window.slicingWindowOperator.ProcessElement(3, 29)
	window.slicingWindowOperator.ProcessElement(4, 39)
	window.slicingWindowOperator.ProcessElement(5, 49)

	resultWindows := window.slicingWindowOperator.ProcessWatermark(22)

	assert.Equal(t, 1, resultWindows[2].GetAggValues()[0])
	assert.False(t, resultWindows[1].HasValue())
	assert.Equal(t, 2, resultWindows[0].GetAggValues()[0])

	resultWindows = window.slicingWindowOperator.ProcessWatermark(55)
	assert.Equal(t, 5, resultWindows[0].GetAggValues()[0]) // 44-55
	assert.Equal(t, 5, resultWindows[1].GetAggValues()[0]) // 40-50
	assert.Equal(t, 4, resultWindows[2].GetAggValues()[0]) // 35-45
	assert.Equal(t, 4, resultWindows[3].GetAggValues()[0]) // 30-40
	assert.Equal(t, 3, resultWindows[4].GetAggValues()[0]) // 25-35
	assert.Equal(t, 3, resultWindows[5].GetAggValues()[0]) // 20-30
	assert.Equal(t, 2, resultWindows[6].GetAggValues()[0]) // 15-25

}

func TestSlidingWindow_InOrder2(t *testing.T) {
	teardownTest, window := WindowSetupTest(t)
	defer teardownTest(t)

	window.slicingWindowOperator.AddWindowFunction(ReduceAggregateFunctionTest{})
	window.slicingWindowOperator.AddWindowAssigner(NewSlidingWindow(Time, 10, 5))

	// 1-10 ; 5-15; 10-20;
	window.slicingWindowOperator.ProcessElement(1, 0)
	window.slicingWindowOperator.ProcessElement(2, 0)
	window.slicingWindowOperator.ProcessElement(3, 20)
	window.slicingWindowOperator.ProcessElement(4, 30)
	window.slicingWindowOperator.ProcessElement(5, 40)

	resultWindows := window.slicingWindowOperator.ProcessWatermark(22)

	assert.False(t, resultWindows[0].HasValue())           // 10-20
	assert.False(t, resultWindows[1].HasValue())           // 5-15
	assert.Equal(t, 3, resultWindows[2].GetAggValues()[0]) // 0-10

	resultWindows = window.slicingWindowOperator.ProcessWatermark(55)
	assert.False(t, resultWindows[0].HasValue())           // 44-55
	assert.Equal(t, 5, resultWindows[1].GetAggValues()[0]) // 40-50
	assert.Equal(t, 5, resultWindows[2].GetAggValues()[0]) // 35-45
	assert.Equal(t, 4, resultWindows[3].GetAggValues()[0]) // 30-40
	assert.Equal(t, 4, resultWindows[4].GetAggValues()[0]) // 25-35
	assert.Equal(t, 3, resultWindows[5].GetAggValues()[0]) // 20-30
	assert.Equal(t, 3, resultWindows[6].GetAggValues()[0]) // 15-25

}

func TestSlidingWindow_InOrderTwoWindows(t *testing.T) {
	teardownTest, window := WindowSetupTest(t)
	defer teardownTest(t)

	window.slicingWindowOperator.AddWindowFunction(ReduceAggregateFunctionTest{})
	window.slicingWindowOperator.AddWindowAssigner(NewSlidingWindow(Time, 10, 5))
	window.slicingWindowOperator.AddWindowAssigner(NewTumblingWindow(Time, 20))

	window.slicingWindowOperator.ProcessElement(1, 1)
	window.slicingWindowOperator.ProcessElement(2, 19)
	window.slicingWindowOperator.ProcessElement(3, 29)
	window.slicingWindowOperator.ProcessElement(4, 39)
	window.slicingWindowOperator.ProcessElement(5, 49)

	resultWindows := window.slicingWindowOperator.ProcessWatermark(22)

	assert.Equal(t, 2, resultWindows[0].GetAggValues()[0]) // 10-20
	assert.False(t, resultWindows[1].HasValue())           // 5- 15
	assert.Equal(t, 1, resultWindows[2].GetAggValues()[0]) // 0-10
	assert.Equal(t, 3, resultWindows[3].GetAggValues()[0]) // 0-20

	resultWindows = window.slicingWindowOperator.ProcessWatermark(55)
	assert.Equal(t, 5, resultWindows[1].GetAggValues()[0]) // 45-50
	assert.Equal(t, 5, resultWindows[1].GetAggValues()[0]) // 40-50
	assert.Equal(t, 4, resultWindows[2].GetAggValues()[0]) // 35-45
	assert.Equal(t, 4, resultWindows[3].GetAggValues()[0]) // 30-40
	assert.Equal(t, 3, resultWindows[4].GetAggValues()[0]) // 25-35
	assert.Equal(t, 3, resultWindows[5].GetAggValues()[0]) // 20-30
	assert.Equal(t, 2, resultWindows[6].GetAggValues()[0]) // 15-25
	assert.Equal(t, 7, resultWindows[7].GetAggValues()[0]) // 20-40

}

func TestSlidingWindow_InOrderTwoWindowsDynamic(t *testing.T) {
	teardownTest, window := WindowSetupTest(t)
	defer teardownTest(t)

	window.slicingWindowOperator.AddWindowFunction(ReduceAggregateFunctionTest{})
	window.slicingWindowOperator.AddWindowAssigner(NewSlidingWindow(Time, 10, 5))

	window.slicingWindowOperator.ProcessElement(1, 1)
	window.slicingWindowOperator.ProcessElement(2, 19)

	window.slicingWindowOperator.AddWindowAssigner(NewTumblingWindow(Time, 20))
	window.slicingWindowOperator.ProcessElement(3, 29)
	window.slicingWindowOperator.ProcessElement(4, 39)
	window.slicingWindowOperator.ProcessElement(5, 49)

	resultWindows := window.slicingWindowOperator.ProcessWatermark(22)

	assert.Equal(t, 2, resultWindows[0].GetAggValues()[0]) // 10-20
	assert.False(t, resultWindows[1].HasValue())           // 5- 15
	assert.Equal(t, 1, resultWindows[2].GetAggValues()[0]) // 0-10
	assert.Equal(t, 3, resultWindows[3].GetAggValues()[0]) // 0-20

	resultWindows = window.slicingWindowOperator.ProcessWatermark(55)
	assert.Equal(t, 5, resultWindows[1].GetAggValues()[0]) // 45-50
	assert.Equal(t, 5, resultWindows[1].GetAggValues()[0]) // 40-50
	assert.Equal(t, 4, resultWindows[2].GetAggValues()[0]) // 35-45
	assert.Equal(t, 4, resultWindows[3].GetAggValues()[0]) // 30-40
	assert.Equal(t, 3, resultWindows[4].GetAggValues()[0]) // 25-35
	assert.Equal(t, 3, resultWindows[5].GetAggValues()[0]) // 20-30
	assert.Equal(t, 2, resultWindows[6].GetAggValues()[0]) // 15-25
	assert.Equal(t, 7, resultWindows[7].GetAggValues()[0]) // 20-40

}

func TestSlidingWindow_InOrderTwoWindowsDynamic2(t *testing.T) {
	teardownTest, window := WindowSetupTest(t)
	defer teardownTest(t)

	window.slicingWindowOperator.AddWindowFunction(ReduceAggregateFunctionTest{})
	window.slicingWindowOperator.AddWindowAssigner(NewTumblingWindow(Time, 20))

	window.slicingWindowOperator.ProcessElement(1, 1)
	window.slicingWindowOperator.ProcessElement(2, 19)
	resultWindows := window.slicingWindowOperator.ProcessWatermark(22)
	assert.Equal(t, 3, resultWindows[0].GetAggValues()[0]) // 0-20

	window.slicingWindowOperator.AddWindowAssigner(NewSlidingWindow(Time, 10, 5))
	window.slicingWindowOperator.ProcessElement(3, 29)
	window.slicingWindowOperator.ProcessElement(4, 39)
	window.slicingWindowOperator.ProcessElement(5, 49)

	resultWindows = window.slicingWindowOperator.ProcessWatermark(55)
	assert.Equal(t, 7, resultWindows[0].GetAggValues()[0]) // 20-40
	assert.Equal(t, 5, resultWindows[1].GetAggValues()[0]) // 45-50
	assert.Equal(t, 5, resultWindows[2].GetAggValues()[0]) // 40-50
	assert.Equal(t, 4, resultWindows[3].GetAggValues()[0]) // 35-45
	assert.Equal(t, 4, resultWindows[4].GetAggValues()[0]) // 30-40
	assert.Equal(t, 3, resultWindows[5].GetAggValues()[0]) // 25-35
	assert.Equal(t, 3, resultWindows[6].GetAggValues()[0]) // 20-30

}

func TestSlidingWindow_OutOfOrder(t *testing.T) {
	teardownTest, window := WindowSetupTest(t)
	defer teardownTest(t)

	window.slicingWindowOperator.AddWindowFunction(ReduceAggregateFunctionTest{})
	window.slicingWindowOperator.AddWindowAssigner(NewSlidingWindow(Time, 10, 5))

	window.slicingWindowOperator.ProcessElement(1, 1)
	window.slicingWindowOperator.ProcessElement(1, 30)
	window.slicingWindowOperator.ProcessElement(1, 20)
	window.slicingWindowOperator.ProcessElement(1, 23)
	window.slicingWindowOperator.ProcessElement(1, 25)
	window.slicingWindowOperator.ProcessElement(1, 45)

	resultWindows := window.slicingWindowOperator.ProcessWatermark(22)
	assert.False(t, resultWindows[0].HasValue())           // 10 -20
	assert.False(t, resultWindows[1].HasValue())           // 5-15
	assert.Equal(t, 1, resultWindows[2].GetAggValues()[0]) //1-10

	resultWindows = window.slicingWindowOperator.ProcessWatermark(55)
	assert.Equal(t, 1, resultWindows[0].GetAggValues()[0]) // 45-55
	assert.Equal(t, 1, resultWindows[1].GetAggValues()[0]) // 40-50
	assert.False(t, resultWindows[2].HasValue())           // 35-45
	assert.Equal(t, 1, resultWindows[3].GetAggValues()[0]) // 30-40
	assert.Equal(t, 2, resultWindows[4].GetAggValues()[0]) // 25-35
	assert.Equal(t, 3, resultWindows[5].GetAggValues()[0]) // 20-30
	assert.Equal(t, 2, resultWindows[6].GetAggValues()[0]) // 15-25
}

func TestTumblingWindow_InOrder(t *testing.T) {
	teardownTest, window := WindowSetupTest(t)
	defer teardownTest(t)

	window.slicingWindowOperator.AddWindowFunction(ReduceAggregateFunctionTest{})
	window.slicingWindowOperator.AddWindowAssigner(NewTumblingWindow(Time, 10))

	window.slicingWindowOperator.ProcessElement(1, 1)
	window.slicingWindowOperator.ProcessElement(2, 19)
	window.slicingWindowOperator.ProcessElement(3, 29)
	window.slicingWindowOperator.ProcessElement(4, 39)
	window.slicingWindowOperator.ProcessElement(5, 49)

	resultWindows := window.slicingWindowOperator.ProcessWatermark(22)
	assert.Equal(t, 1, resultWindows[0].GetAggValues()[0])
	assert.Equal(t, 2, resultWindows[1].GetAggValues()[0])

	resultWindows = window.slicingWindowOperator.ProcessWatermark(55)
	assert.Equal(t, 3, resultWindows[0].GetAggValues()[0])
	assert.Equal(t, 4, resultWindows[1].GetAggValues()[0])
	assert.Equal(t, 5, resultWindows[2].GetAggValues()[0])

}

func TestTumblingWindow_InOrder2(t *testing.T) {
	teardownTest, window := WindowSetupTest(t)
	defer teardownTest(t)

	window.slicingWindowOperator.AddWindowFunction(ReduceAggregateFunctionTest{})
	window.slicingWindowOperator.AddWindowAssigner(NewTumblingWindow(Time, 10))

	window.slicingWindowOperator.ProcessElement(1, 0)
	window.slicingWindowOperator.ProcessElement(2, 0)
	window.slicingWindowOperator.ProcessElement(3, 20)
	window.slicingWindowOperator.ProcessElement(4, 30)
	window.slicingWindowOperator.ProcessElement(5, 40)

	resultWindows := window.slicingWindowOperator.ProcessWatermark(22)
	assert.Equal(t, 3, resultWindows[0].GetAggValues()[0])
	assert.False(t, resultWindows[1].HasValue())

	resultWindows = window.slicingWindowOperator.ProcessWatermark(55)
	assert.Equal(t, 3, resultWindows[0].GetAggValues()[0])
	assert.Equal(t, 4, resultWindows[1].GetAggValues()[0])
	assert.Equal(t, 5, resultWindows[2].GetAggValues()[0])

}

func TestTumblingWindow_InOrderTwoWindow(t *testing.T) {
	teardownTest, window := WindowSetupTest(t)
	defer teardownTest(t)

	window.slicingWindowOperator.AddWindowFunction(ReduceAggregateFunctionTest{})
	window.slicingWindowOperator.AddWindowAssigner(NewTumblingWindow(Time, 10))
	window.slicingWindowOperator.AddWindowAssigner(NewTumblingWindow(Time, 20))

	window.slicingWindowOperator.ProcessElement(1, 1)
	window.slicingWindowOperator.ProcessElement(2, 19)
	window.slicingWindowOperator.ProcessElement(3, 29)
	window.slicingWindowOperator.ProcessElement(4, 39)
	window.slicingWindowOperator.ProcessElement(5, 49)

	resultWindows := window.slicingWindowOperator.ProcessWatermark(22)
	assert.Equal(t, 1, resultWindows[0].GetAggValues()[0])
	assert.Equal(t, 2, resultWindows[1].GetAggValues()[0])
	assert.Equal(t, 3, resultWindows[2].GetAggValues()[0])

	resultWindows = window.slicingWindowOperator.ProcessWatermark(55)
	assert.Equal(t, 3, resultWindows[0].GetAggValues()[0])
	assert.Equal(t, 4, resultWindows[1].GetAggValues()[0])
	assert.Equal(t, 5, resultWindows[2].GetAggValues()[0])
	assert.Equal(t, 7, resultWindows[3].GetAggValues()[0])

}

func TestTumblingWindow_InOrderTwoWindowsDynamic(t *testing.T) {
	teardownTest, window := WindowSetupTest(t)
	defer teardownTest(t)

	window.slicingWindowOperator.AddWindowFunction(ReduceAggregateFunctionTest{})
	window.slicingWindowOperator.AddWindowAssigner(NewTumblingWindow(Time, 10))

	window.slicingWindowOperator.ProcessElement(1, 1)
	window.slicingWindowOperator.ProcessElement(2, 19)
	window.slicingWindowOperator.AddWindowAssigner(NewTumblingWindow(Time, 20))
	window.slicingWindowOperator.ProcessElement(3, 29)
	window.slicingWindowOperator.ProcessElement(4, 39)
	window.slicingWindowOperator.ProcessElement(5, 49)

	resultWindows := window.slicingWindowOperator.ProcessWatermark(22)
	assert.Equal(t, 1, resultWindows[0].GetAggValues()[0])
	assert.Equal(t, 2, resultWindows[1].GetAggValues()[0])
	assert.Equal(t, 3, resultWindows[2].GetAggValues()[0])

	resultWindows = window.slicingWindowOperator.ProcessWatermark(55)
	assert.Equal(t, 3, resultWindows[0].GetAggValues()[0])
	assert.Equal(t, 4, resultWindows[1].GetAggValues()[0])
	assert.Equal(t, 5, resultWindows[2].GetAggValues()[0])
	assert.Equal(t, 7, resultWindows[3].GetAggValues()[0])

}

func TestTumblingWindow_InOrderTwoWindowsDynamic2(t *testing.T) {
	teardownTest, window := WindowSetupTest(t)
	defer teardownTest(t)

	window.slicingWindowOperator.AddWindowFunction(ReduceAggregateFunctionTest{})
	window.slicingWindowOperator.AddWindowAssigner(NewTumblingWindow(Time, 20))

	window.slicingWindowOperator.ProcessElement(1, 1)
	window.slicingWindowOperator.ProcessElement(2, 19)
	resultWindows := window.slicingWindowOperator.ProcessWatermark(22)

	assert.Equal(t, 3, resultWindows[0].GetAggValues()[0])

	window.slicingWindowOperator.AddWindowAssigner(NewTumblingWindow(Time, 10))
	window.slicingWindowOperator.ProcessElement(3, 29)
	window.slicingWindowOperator.ProcessElement(4, 39)
	window.slicingWindowOperator.ProcessElement(5, 49)

	resultWindows = window.slicingWindowOperator.ProcessWatermark(55)
	assert.Equal(t, 3, resultWindows[1].GetAggValues()[0])
	assert.Equal(t, 4, resultWindows[2].GetAggValues()[0])
	assert.Equal(t, 5, resultWindows[3].GetAggValues()[0])
	assert.Equal(t, 7, resultWindows[0].GetAggValues()[0])

}
func TestTumblingWindow_OutOfOrder(t *testing.T) {
	teardownTest, window := WindowSetupTest(t)
	defer teardownTest(t)

	window.slicingWindowOperator.AddWindowFunction(ReduceAggregateFunctionTest{})
	window.slicingWindowOperator.AddWindowAssigner(NewTumblingWindow(Time, 10))

	window.slicingWindowOperator.ProcessElement(1, 1)
	window.slicingWindowOperator.ProcessElement(1, 30)
	window.slicingWindowOperator.ProcessElement(1, 20)
	window.slicingWindowOperator.ProcessElement(1, 23)
	window.slicingWindowOperator.ProcessElement(1, 25)

	window.slicingWindowOperator.ProcessElement(1, 45)
	resultWindows := window.slicingWindowOperator.ProcessWatermark(22)

	assert.Equal(t, 1, resultWindows[0].GetAggValues()[0])
	assert.False(t, resultWindows[1].HasValue())

	resultWindows = window.slicingWindowOperator.ProcessWatermark(55)
	assert.Equal(t, 3, resultWindows[0].GetAggValues()[0])
	assert.Equal(t, 1, resultWindows[1].GetAggValues()[0])
	assert.Equal(t, 1, resultWindows[2].GetAggValues()[0])

}

func TestTumblingWindow_InOrderCount(t *testing.T) {
	teardownTest, window := WindowSetupTest(t)
	defer teardownTest(t)

	window.slicingWindowOperator.AddWindowFunction(ReduceAggregateFunctionTest{})
	window.slicingWindowOperator.AddWindowAssigner(NewTumblingWindow(Count, 3))

	window.slicingWindowOperator.ProcessElement(1, 1)
	window.slicingWindowOperator.ProcessElement(1, 19)
	window.slicingWindowOperator.ProcessElement(1, 29)
	window.slicingWindowOperator.ProcessElement(2, 39)
	window.slicingWindowOperator.ProcessElement(2, 49)
	window.slicingWindowOperator.ProcessElement(2, 50)
	window.slicingWindowOperator.ProcessElement(1, 51)

	resultWindows := window.slicingWindowOperator.ProcessWatermark(55)
	assert.Equal(t, 3, resultWindows[0].GetAggValues()[0])
	assert.Equal(t, 6, resultWindows[1].GetAggValues()[0])

}

func TestTumblingWindow_OutOfOrderCount(t *testing.T) {
	teardownTest, window := WindowSetupTest(t)
	defer teardownTest(t)

	window.slicingWindowOperator.AddWindowFunction(ReduceAggregateFunctionTest{})
	//window.slicingWindowOperator.AddWindowFunction(ReduceAggregateFunctionTest2{})
	window.slicingWindowOperator.AddWindowAssigner(NewTumblingWindow(Count, 3))
	window.slicingWindowOperator.AddWindowAssigner(NewTumblingWindow(Count, 5))

	window.slicingWindowOperator.ProcessElement(1, 1)
	window.slicingWindowOperator.ProcessElement(1, 19)
	window.slicingWindowOperator.ProcessElement(1, 29)
	window.slicingWindowOperator.ProcessElement(2, 39)
	window.slicingWindowOperator.ProcessElement(1, 41)

	// out of order
	window.slicingWindowOperator.ProcessElement(2, 10)
	window.slicingWindowOperator.ProcessElement(2, 50)
	window.slicingWindowOperator.ProcessElement(1, 51)
	window.slicingWindowOperator.ProcessElement(3, 52)

	resultWindows := window.slicingWindowOperator.ProcessWatermark(55)
	assert.Equal(t, 4, resultWindows[0].GetAggValues()[0])
	assert.Equal(t, 4, resultWindows[1].GetAggValues()[0])
	assert.Equal(t, 6, resultWindows[2].GetAggValues()[0])
	assert.Equal(t, 7, resultWindows[3].GetAggValues()[0])

}
func TestTumblingWindow_OutOfOrderCount2(t *testing.T) {
	teardownTest, window := WindowSetupTest(t)
	defer teardownTest(t)

	window.slicingWindowOperator.AddWindowFunction(ReduceAggregateFunctionTest{})
	//window.slicingWindowOperator.AddWindowFunction(ReduceAggregateFunctionTest2{})
	window.slicingWindowOperator.AddWindowAssigner(NewTumblingWindow(Count, 3))
	window.slicingWindowOperator.AddWindowAssigner(NewTumblingWindow(Count, 5))

	window.slicingWindowOperator.ProcessElement(1, 1)
	window.slicingWindowOperator.ProcessElement(1, 19)
	window.slicingWindowOperator.ProcessElement(1, 29)
	window.slicingWindowOperator.ProcessElement(2, 39)
	window.slicingWindowOperator.ProcessElement(1, 41)

	// out of order
	window.slicingWindowOperator.ProcessElement(2, 10)

	resultWindows := window.slicingWindowOperator.ProcessWatermark(30)
	assert.Equal(t, 4, resultWindows[0].GetAggValues()[0])


}

func WindowAssertEquals(t *testing.T, aggregateWindow AggregateWindow, start, end int64, value interface{}) {
	assert.Equal(t, start, aggregateWindow.GetStart())
	assert.Equal(t, end, aggregateWindow.GetEnd())
	assert.Equal(t, value, aggregateWindow.GetAggValues()[0])
}

func WindowAssertContains(t *testing.T, resultWindows []AggregateWindow, start, end int64, value interface{}) {
	for _, w := range resultWindows {
		if w.GetStart() == start && w.GetEnd() == end && w.GetAggValues()[0] == value {
			assert.True(t, true)
			return
		}
	}
	assert.True(t, false)
}
