package window_processor

// Comparable ...
type Comparable interface {
	// CompareTo this object with the specified object for order.
	//Returns a negative integer, zero, or a positive integer as this object is less than, equal to, or greater than the specified object.
	CompareTo(o interface{}) int
}

const (
	Min_Value int64 = -99999
	Max_Value int64 = 99999
)

func Min(a, b int64) int64 {
	if a <= b {
		return a
	}
	return b
}

func Max(a, b int64) int64 {
	if a >= b {
		return a
	}
	return b
}

func Int64Compare(a, b int64) int {
	if a < b {
		return -1
	} else if a == b {
		return 0
	} else {
		return 1
	}
}

// IntComparator provides a basic comparison on int
func IntComparator(a, b interface{}) int {
	aAsserted := a.(int)
	bAsserted := b.(int)
	switch {
	case aAsserted > bAsserted:
		return 1
	case aAsserted < bAsserted:
		return -1
	default:
		return 0
	}
}

