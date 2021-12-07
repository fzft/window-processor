package window_processor

type AggregationWindowCollector struct {
	aggregationStores []AggregateWindow
	stateFactory StateFactory
	windowFunctions []AggregateFunction
}

func (c *AggregationWindowCollector) Trigger(start, end int64, measure WindowMeasure) {
	aggWindow := NewAggregateWindowState(start, end, measure, c.stateFactory, c.windowFunctions)
	c.aggregationStores.Add()
}

func (c *AggregationWindowCollector) Range() []AggregateWindow {
	return c.aggregationStores
}