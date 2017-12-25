package collections

type Enumerator interface {
	Current() interface{}
	MoveNext() bool
	Reset()
}

type DefaultEnumerator struct {
	source []interface{}
	index  int32
}

func NewEnumeratorFromSource(source []interface{}) Enumerator {
	return &DefaultEnumerator{
		source: source,
		index:  -1,
	}
}

func (e *DefaultEnumerator) Current() interface{} {
	if e.index < 0 || e.index >= int32(len(e.source)) {
		return nil
	}
	return e.source[e.index]
}

func (e *DefaultEnumerator) MoveNext() bool {
	if e.index = e.index + 1; e.index >= int32(len(e.source)) {
		return false
	}
	return true
}

func (e *DefaultEnumerator) Reset() {
	e.index = -1
}
