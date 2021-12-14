package window_processor

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

var stateFactory = MemoryStateFactory{}

func TestMemoryListState(t *testing.T) {
	s := stateFactory.CreateListState()
	s.Add(1)
	s.Add(2)
	s.Add(3)
	assert.Equal(t, len(s.Get()), 3, "list size should be equal")

	s.Set(1, 4)
	assert.Equal(t, s.Get()[1], 4, "element pos 4 should be equal")

	s.Clean()
	assert.Equal(t, s.IsEmpty(), true)

}

func TestMemorySetState(t *testing.T) {
	s := stateFactory.CreateSetState(IntComparator)

	s.Add(1)
	s.Add(1)
	s.Add(2)
	s.Add(3)


	assert.Equal(t, s.GetFirst(), 1)
	assert.Equal(t, s.GetLast(), 3)
	assert.Equal(t, s.DropFirst(), 1)
	assert.Equal(t, s.DropLast(), 3)

	s.Clean()
	assert.Equal(t, s.IsEmpty(), true)

}

func TestMemoryValueState(t *testing.T) {
	s := stateFactory.CreateValueState()
	s.Set(1)

	assert.Equal(t, s.Get(), 1)

	s.Clean()
	assert.Equal(t, s.IsEmpty(), true)

}
