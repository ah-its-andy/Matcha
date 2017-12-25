package collections

import "sync"

type Enumerable interface {
	Count() int32
	Clear()
	GetEnumerator() Enumerator
	Range(r func(int32, interface{}) error) error
	ToArray() []interface{}

	CountBy(predicate func(interface{}) bool) (int32, error)
	Any(predicate func(interface{}) bool) (bool, error)
	First(predicate func(interface{}) bool) (interface{}, error)
	FirstOrDefault(predicate func(interface{}) bool) (interface{}, error)

	Single(predicate func(interface{}) bool) (interface{}, error)
	SingleOrDefault(predicate func(interface{}) bool) (interface{}, error)

	Contains(item interface{}) (bool, error)

	ToList() (List, error)
}

func TryForEach(src Enumerable, r func(interface{}) error) error {
	s := src.GetEnumerator()
	s.Reset()
	defer s.Reset()
	var err error
	for s.MoveNext() {
		err = r(s.Current())
		if err != nil {
			goto ForEnd
		}
	}
ForEnd:
	return err
}

func ForEach(src Enumerable, r func(interface{})) {
	s := src.GetEnumerator()
	s.Reset()
	defer s.Reset()
	for s.MoveNext() {
		r(s.Current())
	}
}

func ForEachSync(src Enumerable, r func(interface{})) {
	var wg sync.WaitGroup
	wg.Add(int(src.Count()))
	s := src.GetEnumerator()
	s.Reset()
	defer s.Reset()
	for s.MoveNext() {
		r(s.Current())
		wg.Done()
	}
	wg.Wait()
}

type DefaultEnumerable struct {
	source []interface{}
}

func NewEnumerableFromSource(source []interface{}) Enumerable {
	return &DefaultEnumerable{
		source: source,
	}
}

func (e *DefaultEnumerable) CountBy(predicate func(interface{}) bool) (int32, error) {
	if e.source == nil {
		return 0, ERRORSourceIsNil()
	}
	c := 0
	err := e.Range(func(_ int32, v interface{}) error {
		if v == nil {
			return ERRORNilReference()
		}
		if predicate(v) {
			c = c + 1
		}
		return nil
	})
	if err != nil {
		return 0, err
	}
	return int32(c), nil
}
func (e *DefaultEnumerable) Any(predicate func(interface{}) bool) (bool, error) {
	if e.source == nil {
		return false, ERRORSourceIsNil()
	}
	var err error
	r := false
	for _, v := range e.source {
		if v == nil {
			err = ERRORNilReference()
			goto ForEnd
		}
		if predicate(v) {
			r = true
			goto ForEnd
		}
	}
ForEnd:
	if err != nil {
		return false, err
	}
	return r, nil
}
func (e *DefaultEnumerable) First(predicate func(interface{}) bool) (interface{}, error) {
	if e.source == nil {
		return false, ERRORSourceIsNil()
	}
	var r interface{}
	var err error

	for _, v := range e.source {
		if v == nil {
			err = ERRORNilReference()
			goto ForEnd
		}
		if predicate(v) {
			r = v
			goto ForEnd
		}
	}
	goto ForEnd

ForEnd:
	if err != nil {
		return nil, err
	}
	if r == nil {
		return nil, ERRORSourceSequenceEmpty()
	}
	return r, nil
}
func (e *DefaultEnumerable) FirstOrDefault(predicate func(interface{}) bool) (interface{}, error) {
	if e.source == nil {
		return false, ERRORSourceIsNil()
	}
	var r interface{}
	var err error

	for _, v := range e.source {
		if v == nil {
			err = ERRORNilReference()
			goto ForEnd
		}
		if predicate(v) {
			r = v
			goto ForEnd
		}
	}
	goto ForEnd

ForEnd:
	if err != nil {
		return nil, err
	}
	return r, nil
}
func (e *DefaultEnumerable) Single(predicate func(interface{}) bool) (interface{}, error) {
	if e.source == nil {
		return false, ERRORSourceIsNil()
	}
	var r interface{}
	var err error

	for _, v := range e.source {
		if v == nil {
			err = ERRORNilReference()
		}
		if predicate(v) {
			if r != nil {
				err = ERRORInputSequenceContainsMoreThanOneElement()
				goto ForEnd
			} else {
				r = v
			}
		}
	}
	goto ForEnd

ForEnd:
	if err != nil {
		return nil, err
	}
	if r == nil {
		return nil, ERRORSourceSequenceEmpty()
	}
	return r, nil
}
func (e *DefaultEnumerable) SingleOrDefault(predicate func(interface{}) bool) (interface{}, error) {
	if e.source == nil {
		return false, ERRORSourceIsNil()
	}
	var r interface{}
	var err error

	for _, v := range e.source {
		if v == nil {
			err = ERRORNilReference()
		}
		if predicate(v) {
			if r != nil {
				err = ERRORInputSequenceContainsMoreThanOneElement()
				goto ForEnd
			} else {
				r = v
			}
		}
	}
	goto ForEnd

ForEnd:
	if err != nil {
		return nil, err
	}
	return r, nil
}

func (e *DefaultEnumerable) Count() int32 {
	return int32(len(e.source))
}

func (e *DefaultEnumerable) Clear() {
	e.source = make([]interface{}, 0)
}

func (e *DefaultEnumerable) Range(r func(int32, interface{}) error) error {
	var err error
	for i, v := range e.source {
		err = r(int32(i), v)
		if err != nil {
			goto ForEnd
		}
	}
ForEnd:
	return err
}

func (e *DefaultEnumerable) Contains(item interface{}) (bool, error) {
	return e.Any(func(i interface{}) bool {
		return i == item
	})
}

func (e *DefaultEnumerable) GetEnumerator() Enumerator {
	s := make([]interface{}, e.Count())
	e.Range(func(i int32, v interface{}) error {
		s[i] = v
		return nil
	})
	return NewEnumeratorFromSource(s)
}

func (e *DefaultEnumerable) ToArray() []interface{} {
	return e.source
}

func (e *DefaultEnumerable) ToList() (List, error) {
	return &DefaultList{
		DefaultEnumerable: e,
	}, nil
}
