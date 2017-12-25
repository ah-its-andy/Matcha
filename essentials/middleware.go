package essentials

type Middleware interface {
	Equalable
	Execute(ctx *MatchaContext) error
}
