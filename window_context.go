package window_processor

import (
	"github.com/emirpasic/gods/lists/arraylist"
	"github.com/emirpasic/gods/sets/hashset"
)

type Context interface {
	hasActiveWindows() bool
	getActiveWindows() []*ActiveWindow
	getWindow(i int) *ActiveWindow
	mergeWithPre(index int) *ActiveWindow
	addNewWindow(i int, start, end int64) *ActiveWindow
	updateContext1(tuple interface{}, position int64, modifiedWindowEdges *hashset.Set) *ActiveWindow
	updateContext2(tuple interface{}, position int64) *ActiveWindow
	assignNextWindowStart(position int64) int64
	triggerWindows(aggregateWindows WindowCollector, lastWatermark, currentWatermark int64)
	removeWindow(index int)
	shiftStart(w *ActiveWindow, pos int64)
	numberOfActiveWindows() int
	shiftEnd(w *ActiveWindow, pos int64)
}

type WindowContext struct {
	activeWindows       *arraylist.List
	modifiedWindowEdges *hashset.Set
}

func NewWindowContext() *WindowContext {
	return &WindowContext{
		activeWindows: arraylist.New(),
	}
}

func (c *WindowContext) hasActiveWindows() bool {
	return c.activeWindows.Empty()
}

func (c *WindowContext) getActiveWindows() []*ActiveWindow {
	activeWindows := make([]*ActiveWindow, c.numberOfActiveWindows())
	for _, w := range c.activeWindows.Values() {
		activeWindows = append(activeWindows, w.(*ActiveWindow))
	}
	return activeWindows
}

func (c *WindowContext) addNewWindow(i int, start, end int64) *ActiveWindow {
	w := newActiveWindow(start, end)
	c.activeWindows.Insert(i, w)
	c.modifiedWindowEdges.Add(newAddModification(start))
	c.modifiedWindowEdges.Add(newAddModification(end))
	return w
}

func (c *WindowContext) getWindow(i int) *ActiveWindow {
	if v, ok := c.activeWindows.Get(i); ok {
		return v.(*ActiveWindow)
	}
	return nil
}

func (c *WindowContext) mergeWithPre(index int) *ActiveWindow {
	w := c.getWindow(index)
	preW := c.getWindow(index - 1)
	c.shiftEnd(preW, w.end)
	c.removeWindow(index)
	return preW

}

func (c *WindowContext) triggerWindows(aggregateWindows WindowCollector, lastWatermark, currentWatermark int64) {
	panic("implement me")
}

func (c *WindowContext) assignNextWindowStart(position int64) int64 {
	panic("implement me")
}

func (c *WindowContext) removeWindow(index int) {
	c.modifiedWindowEdges.Add(newDeleteModification(c.getWindow(index).start))
	c.modifiedWindowEdges.Add(newDeleteModification(c.getWindow(index).end))
	c.activeWindows.Remove(index)
}

func (c *WindowContext) shiftStart(w *ActiveWindow, pos int64) {
	c.modifiedWindowEdges.Add(newShiftModification(w.start, pos))
	w.setStart(pos)
}

func (c *WindowContext) numberOfActiveWindows() int {
	return c.activeWindows.Size()
}

func (c *WindowContext) shiftEnd(w *ActiveWindow, pos int64) {
	//c.modifiedWindowEdges.Add(newShiftModification(w.end, pos))
	w.setEnd(pos)
}

func (c *WindowContext) updateContext1(tuple interface{}, position int64, modifiedWindowEdges *hashset.Set) *ActiveWindow {
	if modifiedWindowEdges != nil {
		c.modifiedWindowEdges = modifiedWindowEdges
	}
	return c.updateContext2(tuple, position)
}

func (c *WindowContext) updateContext2(tuple interface{}, position int64) *ActiveWindow {
	panic("implement me")
}

type ActiveWindow struct {
	start int64
	end   int64
}

func newActiveWindow(start, end int64) *ActiveWindow {
	return &ActiveWindow{
		start: start,
		end:   end,
	}
}

func (w *ActiveWindow) getStart() int64 {
	return w.start
}

func (w *ActiveWindow) getEnd() int64 {
	return w.end
}

func (w *ActiveWindow) setEnd(end int64) {
	w.end = end
}

func (w *ActiveWindow) setStart(start int64) {
	w.start = start
}

// ActiveWindowComparator provides a fast comparison on ActiveWindow
func ActiveWindowComparator(a, b interface{}) int {
	s1 := a.(*ActiveWindow)
	s2 := b.(*ActiveWindow)
	return Int64Compare(s1.start, s2.start)
}

type AddModification struct {
	post int64
}

func newAddModification(post int64) AddModification {
	return AddModification{post: post}
}

type DeleteModification struct {
	pre int64
}

func newDeleteModification(pre int64) DeleteModification {
	return DeleteModification{pre: pre}
}

type ShiftModification struct {
	pre  int64
	post int64
}

func newShiftModification(pre, post int64) ShiftModification {
	return ShiftModification{pre: pre, post: post}
}

type SessionContext struct {
	windowContext *WindowContext
	measure       WindowMeasure
	gap           int64
}

func NewSessionContext(measure WindowMeasure) *SessionContext {
	c := new(SessionContext)
	c.measure = measure
	c.windowContext = NewWindowContext()
	return c
}

func (c *SessionContext) hasActiveWindows() bool {
	return c.windowContext.hasActiveWindows()
}

func (c *SessionContext) getActiveWindows() []*ActiveWindow {
	return c.windowContext.getActiveWindows()
}

func (c *SessionContext) getWindow(i int) *ActiveWindow {
	return c.windowContext.getWindow(i)
}

func (c *SessionContext) mergeWithPre(index int) *ActiveWindow {
	return c.windowContext.mergeWithPre(index)
}

func (c *SessionContext) addNewWindow(i int, start, end int64) *ActiveWindow {
	return c.windowContext.addNewWindow(i, start, end)
}

func (c *SessionContext) updateContext1(tuple interface{}, position int64, modifiedWindowEdges *hashset.Set) *ActiveWindow {
	if modifiedWindowEdges != nil {
		c.windowContext.modifiedWindowEdges = modifiedWindowEdges
	}
	return c.updateContext2(tuple, position)
}

func (c *SessionContext) assignNextWindowStart(position int64) int64 {
	return c.windowContext.assignNextWindowStart(position)
}

func (c *SessionContext) triggerWindows(aggregateWindows WindowCollector, lastWatermark, currentWatermark int64) {
	c.windowContext.triggerWindows(aggregateWindows, lastWatermark, currentWatermark)
}

func (c *SessionContext) removeWindow(index int) {
	c.windowContext.removeWindow(index)
}

func (c *SessionContext) shiftStart(w *ActiveWindow, pos int64) {
	c.windowContext.shiftStart(w, pos)
}

func (c *SessionContext) numberOfActiveWindows() int {
	return c.windowContext.numberOfActiveWindows()
}

func (c *SessionContext) shiftEnd(w *ActiveWindow, pos int64) {
	c.windowContext.shiftEnd(w, pos)
}

func (c *SessionContext) updateContext2(tuple interface{}, position int64) *ActiveWindow {
	if c.hasActiveWindows() {
		c.addNewWindow(0, position, position)
		return c.getWindow(0)
	}

	sessionIndex := c.getSession(position)
	if sessionIndex == -1 {
		c.addNewWindow(0, position, position)
	} else {
		s := c.getWindow(sessionIndex)
		if s.getStart()-c.gap > position {
			return c.addNewWindow(sessionIndex, position, position)
		} else if s.getStart() > position && s.getStart()-c.gap < position {
			c.shiftStart(s, position)
			if sessionIndex > 0 {
				preSession := c.getWindow(sessionIndex - 1)
				if preSession.getEnd()+c.gap >= s.getStart() {
					return c.mergeWithPre(sessionIndex)
				}
			}
			return s
		} else if s.getEnd() < position && s.getEnd()+c.gap >= position {
			c.shiftEnd(s, position)
			if sessionIndex < c.numberOfActiveWindows()-1 {
				nextSession := c.getWindow(sessionIndex - 1)
				if s.getEnd()+c.gap >= nextSession.getStart() {
					return c.mergeWithPre(sessionIndex + 1)
				}
			}
		} else if s.getEnd()+c.gap < position {
			return c.addNewWindow(sessionIndex+1, position, position)
		}
	}
	return nil
}

func (c *SessionContext) getSession(position int64) int {
	i := 0
	for ; i <= c.numberOfActiveWindows(); i++ {
		s := c.getWindow(i)
		if s.getStart()-c.gap <= position && s.getEnd()+c.gap >= position {
			return i
		} else if s.getStart()-c.gap > position {
			return i - 1
		}
	}
	return i - 1
}

func (c *SessionContext) AssignNextWindowStart(pos int64) int64 {
	return pos + c.gap
}

func (c *SessionContext) TriggerWindows(aggregateWindows WindowCollector, lastWatermark, currentWatermark int64) {
	s := c.getWindow(0)
	for s.getEnd()+c.gap < currentWatermark {
		aggregateWindows.Trigger(s.getStart(), s.getEnd()+c.gap, c.measure)
		c.removeWindow(0)
		if c.hasActiveWindows() {
			return
		}
		s = c.getWindow(0)
	}
}
