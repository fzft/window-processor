package window_processor

type WindowContext struct {
	activeWindows       []*ActiveWindow
	modifiedWindowEdges []interface{}
}

func NewWindowContext() *WindowContext {
	return &WindowContext{}
}

func (c *WindowContext) hasActiveWindows() bool {
	return len(c.activeWindows) > 0
}

func (c *WindowContext) addNewWindow(i int, start, end int64) *ActiveWindow {
	w := newActiveWindow(start, end)
	c.activeWindows[i] = w
	c.modifiedWindowEdges = append(c.modifiedWindowEdges, newAddModification(start), newAddModification(end))
	return w
}

func (c *WindowContext) getWindow(i int) *ActiveWindow {
	return c.activeWindows[i]
}

func (c *WindowContext) mergeWithPre(index int) *ActiveWindow {
	w := c.activeWindows[index]
	preW := c.activeWindows[index-1]
	c.shiftEnd(preW, w.end)
	c.removeWindow(index)
	return preW

}

func (c *WindowContext) removeWindow(index int) {
	c.modifiedWindowEdges = append(c.modifiedWindowEdges,
		newDeleteModification(c.activeWindows[index].start),
		newDeleteModification(c.activeWindows[index].end),
	)
}

func (c *WindowContext) shiftStart(w *ActiveWindow, pos int64) {
	c.modifiedWindowEdges = append(c.modifiedWindowEdges, newShiftModification(w.start, pos))
	w.setStart(pos)
}

func (c *WindowContext) numberOfActiveWindows() int {
	return len(c.activeWindows)
}

func (c *WindowContext) shiftEnd(w *ActiveWindow, pos int64) {
	//c.modifiedWindowEdges = append(c.modifiedWindowEdges, newShiftModification(w.end, pos))
	w.setEnd(pos)
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

func (w *ActiveWindow) compareTo(o ActiveWindow) bool {
	return w.start >= o.start
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
