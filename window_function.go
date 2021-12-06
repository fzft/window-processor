package window_processor

type AggregateFunction interface {
	Lift(interface{}) interface{}
	Combine(partialAggregate1, partialAggregate2 interface{}) interface{}
	Lower(interface{}) interface{}
}


