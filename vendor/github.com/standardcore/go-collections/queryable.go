package collections

import "sort"

type CompareResult int16

const (
	_ CompareResult = iota
	CompareBigger
	CompareEqual
	CompareSmaller
)

type QueryKind int16

const (
	_ QueryKind = iota
	QueryWhere
	QuerySelect
	QueryGroupBy
	QueryOrderBy
	QueryOrderByDescending
	QueryTake
	QuerySkip
	QuerySelectWhile
	QuerySkipWhile
)

type Queryable interface {
	CountBy(predicate func(interface{}) bool) (int32, error)
	Any(predicate func(interface{}) bool) (bool, error)
	First(predicate func(interface{}) bool) (interface{}, error)
	FirstOrDefault(predicate func(interface{}) bool) (interface{}, error)

	Single(predicate func(interface{}) bool) (interface{}, error)
	SingleOrDefault(predicate func(interface{}) bool) (interface{}, error)

	ToList() (List, error)
	//queries
	Where(predicate func(interface{}) bool) Queryable
	Select(selector func(interface{}) interface{}) Queryable
	GroupBy(keySelector func(interface{}) interface{}) Queryable
	OrderBy(compare func(x interface{}, y interface{}) CompareResult) Queryable
	OrderByDescending(compare func(x interface{}, y interface{}) CompareResult) Queryable
	Take(count int32) Queryable
	Skip(count int32) Queryable
	SelectWhile(predicate func(interface{}) bool, selector func(interface{}) interface{}) Queryable
	SkipWhile(predicate func(interface{}) bool) Queryable
}

type QueryDescriptor struct {
	kind QueryKind
	r    []interface{}
}

type Grouping struct {
	key   interface{}
	elems Enumerable
}

func (g *Grouping) Key() interface{} {
	return g.key
}

func (g *Grouping) Elements() Enumerable {
	return g.elems
}

type DefaultQueryable struct {
	source Enumerable

	queries []*QueryDescriptor
}

func (q *DefaultQueryable) CountBy(predicate func(interface{}) bool) (int32, error) {
	q.source = q.query(q.source)
	return q.source.CountBy(predicate)
}
func (q *DefaultQueryable) Any(predicate func(interface{}) bool) (bool, error) {
	q.source = q.query(q.source)
	return q.source.Any(predicate)
}
func (q *DefaultQueryable) First(predicate func(interface{}) bool) (interface{}, error) {
	q.source = q.query(q.source)
	return q.source.First(predicate)
}
func (q *DefaultQueryable) FirstOrDefault(predicate func(interface{}) bool) (interface{}, error) {
	q.source = q.query(q.source)
	return q.source.FirstOrDefault(predicate)
}

func (q *DefaultQueryable) Single(predicate func(interface{}) bool) (interface{}, error) {
	q.source = q.query(q.source)
	return q.source.Single(predicate)
}
func (q *DefaultQueryable) SingleOrDefault(predicate func(interface{}) bool) (interface{}, error) {
	q.source = q.query(q.source)
	return q.source.SingleOrDefault(predicate)
}

func (q *DefaultQueryable) ToList() (List, error) {
	q.source = q.query(q.source)
	return q.source.ToList()
}

func (q *DefaultQueryable) Where(predicate func(interface{}) bool) Queryable {
	return q.appendQuery(QueryWhere, predicate)
}

func (q *DefaultQueryable) Select(selector func(interface{}) interface{}) Queryable {
	return q.appendQuery(QuerySelect, selector)
}

func (q *DefaultQueryable) GroupBy(keySelector func(interface{}) interface{}) Queryable {
	return q.appendQuery(QueryGroupBy, keySelector)
}

func (q *DefaultQueryable) OrderBy(compare func(x interface{}, y interface{}) CompareResult) Queryable {
	return q.appendQuery(QueryOrderBy, compare)
}

func (q *DefaultQueryable) OrderByDescending(compare func(x interface{}, y interface{}) CompareResult) Queryable {
	return q.appendQuery(QueryOrderByDescending, compare)
}

func (q *DefaultQueryable) Take(count int32) Queryable {
	return q.appendQuery(QueryTake, count)
}

func (q *DefaultQueryable) Skip(count int32) Queryable {
	return q.appendQuery(QuerySkip, count)
}

func (q *DefaultQueryable) SelectWhile(predicate func(interface{}) bool, selector func(interface{}) interface{}) Queryable {
	return q.appendQuery(QuerySelectWhile, predicate)
}

func (q *DefaultQueryable) SkipWhile(predicate func(interface{}) bool) Queryable {
	return q.appendQuery(QuerySkipWhile, predicate)
}

func (query *DefaultQueryable) appendQuery(k QueryKind, qs ...interface{}) Queryable {
	query.queries = append(query.queries, &QueryDescriptor{
		kind: k,
		r:    qs,
	})
	return query
}

func (q *DefaultQueryable) clone(src Enumerable) Enumerable {
	s := make([]interface{}, src.Count())
	src.Range(func(i int32, v interface{}) error {
		s[i] = v
		return nil
	})
	return NewEnumerableFromSource(s)
}

func (q *DefaultQueryable) query(s Enumerable) Enumerable {
	source := q.clone(s)
	for _, v := range q.queries {
		source = q.exec(source, v.kind, v.r)
	}
	return source
}

func (q *DefaultQueryable) exec(source Enumerable, k QueryKind, args []interface{}) Enumerable {
	r := source
	switch k {
	case QueryWhere:
		r = q.execWhere(source, args[0].(func(interface{}) bool))
	case QuerySelect:
		r = q.execSelect(source, args[0].(func(interface{}) interface{}))
	case QueryGroupBy:
		r = q.execGroupBy(source, args[0].(func(interface{}) interface{}))
	case QueryOrderBy:
		r = q.execOrderBy(source, args[0].(func(x interface{}, y interface{}) CompareResult))
	case QueryOrderByDescending:
		r = q.execOrderByDescending(source, args[0].(func(x interface{}, y interface{}) CompareResult))
	case QueryTake:
		r = q.execTake(source, args[0].(int32))
	case QuerySkip:
		r = q.execSkip(source, args[0].(int32))
	case QuerySelectWhile:
		r = q.execSelectWhile(source, args[0].(func(interface{}) bool), args[1].(func(interface{}) interface{}))
	case QuerySkipWhile:
		r = q.execSkipWhile(source, args[0].(func(interface{}) bool))
	}
	return r
}

func (q *DefaultQueryable) execWhere(source Enumerable, predicate func(interface{}) bool) Enumerable {
	t := NewList()
	ForEach(source, func(v interface{}) {
		if predicate(v) {
			t.Add(v)
		}
	})
	return t
}

func (q *DefaultQueryable) execSelect(source Enumerable, selector func(interface{}) interface{}) Enumerable {
	s := NewList()
	var item interface{}
	ForEach(source, func(v interface{}) {
		item = selector(v)
		s.Add(item)
	})
	return s
}

func (q *DefaultQueryable) execGroupBy(source Enumerable, keySelector func(interface{}) interface{}) Enumerable {
	m := make(map[interface{}]List, 0)
	var key interface{}
	ForEach(source, func(v interface{}) {
		key = keySelector(v)
		list, ok := m[key]
		if ok {
			list.Add(v)
		} else {
			list = NewList()
			list.Add(v)
			m[key] = list
		}
	})
	s := NewList()
	var g *Grouping
	for k, v := range m {
		g = &Grouping{
			key:   k,
			elems: v,
		}
		s.Add(g)
	}
	return s
}

type SortableItem struct {
	v       interface{}
	compare func(x interface{}, y interface{}) CompareResult
}

type ASC []*SortableItem

func (s ASC) Len() int {
	return len(s)
}
func (s ASC) Swap(i, j int) {
	s[i], s[j] = s[j], s[i]
}
func (s ASC) Less(i, j int) bool {
	left := s[i]
	right := s[j]
	r := left.compare(left, right)
	if r == CompareSmaller {
		return true
	}
	return false
}

type DESC []*SortableItem

func (s DESC) Len() int {
	return len(s)
}
func (s DESC) Swap(i, j int) {
	s[i], s[j] = s[j], s[i]
}
func (s DESC) Less(i, j int) bool {
	left := s[i]
	right := s[j]
	r := left.compare(left, right)
	if r == CompareBigger {
		return true
	}
	return false
}

func (q *DefaultQueryable) execOrderBy(source Enumerable, compare func(x interface{}, y interface{}) CompareResult) Enumerable {
	items := make([]*SortableItem, 0)
	var item *SortableItem
	ForEach(source, func(v interface{}) {
		item = &SortableItem{
			v:       v,
			compare: compare,
		}
		items = append(items, item)
	})
	sortable := ASC(items)
	sort.Sort(sortable)
	s := NewList()
	for _, v := range sortable {
		s.Add(v)
	}
	return s
}

func (q *DefaultQueryable) execOrderByDescending(source Enumerable, compare func(x interface{}, y interface{}) CompareResult) Enumerable {
	items := make([]*SortableItem, 0)
	var item *SortableItem
	ForEach(source, func(v interface{}) {
		item = &SortableItem{
			v:       v,
			compare: compare,
		}
		items = append(items, item)
	})
	sortable := DESC(items)
	sort.Sort(sortable)
	s := NewList()
	for _, v := range sortable {
		s.Add(v)
	}
	return s
}

func (q *DefaultQueryable) execTake(source Enumerable, count int32) Enumerable {
	return NewEnumerableFromSource(source.ToArray()[0 : count-1])
}

func (q *DefaultQueryable) execSkip(source Enumerable, count int32) Enumerable {
	return NewEnumerableFromSource(source.ToArray()[count-1:])
}

func (q *DefaultQueryable) execSelectWhile(source Enumerable, predicate func(interface{}) bool, selector func(interface{}) interface{}) Enumerable {
	s := NewList()
	var item interface{}
	ForEach(source, func(v interface{}) {
		if predicate(v) {
			item = selector(v)
			s.Add(item)
		}
	})
	return s
}

func (q *DefaultQueryable) execSkipWhile(source Enumerable, predicate func(interface{}) bool) Enumerable {
	s := NewList()
	ForEach(source, func(v interface{}) {
		if !predicate(v) {
			s.Add(v)
		}
	})
	return s
}
