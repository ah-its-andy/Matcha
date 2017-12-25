package essentials

type Equalable interface {
	Equal(obj Equalable) bool
}
