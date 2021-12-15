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
	window.slicingWindowOperator.AddWindowAssigner(NewFixedBandWindow(Time, 1, 10))

	window.slicingWindowOperator.ProcessElement(1, 1)
	window.slicingWindowOperator.ProcessElement(2, 19)
	window.slicingWindowOperator.ProcessElement(3, 29)
	window.slicingWindowOperator.ProcessElement(4, 39)
	window.slicingWindowOperator.ProcessElement(5, 49)

	resultWindows := window.slicingWindowOperator.ProcessWatermark(55)
	WindowAssertEquals(t, resultWindows[0], 1, 11, 1)
}

func WindowAssertEquals(t *testing.T, aggregateWindow AggregateWindow, start, end int64, value interface{}) {
	assert.Equal(t, start, aggregateWindow.GetStart())
	assert.Equal(t, end, aggregateWindow.GetEnd())
	assert.Equal(t, value, aggregateWindow.GetAggValues()[0])
}
