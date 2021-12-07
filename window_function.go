package window_processor

type AggregateFunction interface {
	// Lift transforms a tuple to a partial aggregate
	// If a tuple (p, v) contains its position p and value v, the lift function will transform it to (sum <-v, count <- l)
	Lift(interface{}) interface{}

	// Combine compute the combined aggregate from partial aggregate
	// This method is used in two ways
	// 1. To add a single element to the partial aggregate
	// 2. To merge two big partial aggregate or slice
	Combine(partialAggregate1, partialAggregate2 interface{}) interface{}

	// LiftAndCombine for combining the lift and combine function
	LiftAndCombine(partialAggregateType interface{}, inputTuple interface{}) interface{}

	// Lower transforms a partial aggregate to a final aggregate
	// (sum, count) -> sum/count
	Lower(interface{}) interface{}
}

type InvertibleAggregateFunction interface {
	AggregateFunction

	//Invert remove one partial aggregate from another with an incremental operation
	Invert(currentAggregate, toRemove interface{}) interface{}

	ListAndInvert(partialAggregate, toRemove interface{}) interface{}
}

type CloneablePartialStateFunction interface {
	Clone(partialAggregate interface{}) interface{}
}

type SumAggregateFunction struct {
}

func (f SumAggregateFunction) Lift(input interface{}) interface{} {
	return input
}

func (f SumAggregateFunction) Combine(partialAggregate1, partialAggregate2 interface{}) interface{} {
	v1, ok := partialAggregate1.(int64)
	if !ok {
		return partialAggregate2
	}
	v2, ok := partialAggregate2.(int64)
	if !ok {
		return nil
	}
	return v2 + v1
}

func (f SumAggregateFunction) Lower(input interface{}) interface{} {
	return input
}


