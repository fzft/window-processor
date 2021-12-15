package window_processor

type WindowOperator interface {
	// ProcessElement process a new element of the stream
	ProcessElement(el interface{}, ts int64)
	// ProcessWatermark process a watermark at a specific timestamp
	ProcessWatermark(watermarkTs int64) []AggregateWindow
	// AddWindowAssigner add a window assigner to the window operator
	AddWindowAssigner(window Window)
	// AddAggregation add a aggregation
	AddAggregation(windowFunc AggregateFunction)
	// SetMaxLateness set the max lateness for the window operator
	SetMaxLateness(maxLateness int64)
}

// SlicingWindowOperator Implementation of the slicing window operator
type SlicingWindowOperator struct {
	stateFactory  StateFactory
	windowManager *WindowManager
	sliceFactory  SliceFactory
	sliceManager  *SliceManager
	slicer        *StreamSlicer
}

func NewSlicingWindowOperator(stateFactory StateFactory) *SlicingWindowOperator {
	operator := new(SlicingWindowOperator)
	aggregationStore := &LazyAggregateStore{}
	operator.stateFactory = stateFactory
	operator.windowManager = NewWindowManager(stateFactory, aggregationStore)
	operator.sliceFactory = NewSliceFactory(operator.windowManager, stateFactory)
	operator.sliceManager = NewSliceManager(operator.sliceFactory, aggregationStore, operator.windowManager)
	operator.slicer = NewStreamSlicer(operator.sliceManager, operator.windowManager)
	return operator
}

func (s *SlicingWindowOperator) ProcessElement(el interface{}, ts int64) {
	s.slicer.DetermineSlices(ts)
	s.sliceManager.ProcessElement(el, ts)
}

func (s *SlicingWindowOperator) ProcessWatermark(watermarkTs int64) []AggregateWindow {
	return s.windowManager.ProcessWatermark(watermarkTs)
}

func (s *SlicingWindowOperator) AddWindowAssigner(window Window) {
	s.windowManager.addWindowAssigner(window)
}

func (s *SlicingWindowOperator) AddAggregation(windowFunc AggregateFunction) {
	s.windowManager.addAggregation(windowFunc)
}

func (s *SlicingWindowOperator) SetMaxLateness(maxLateness int64) {
	s.windowManager.SetMaxLateness(maxLateness)
}

func (s *SlicingWindowOperator) AddWindowFunction(windowFunc AggregateFunction) {
	s.windowManager.addAggregation(windowFunc)
}