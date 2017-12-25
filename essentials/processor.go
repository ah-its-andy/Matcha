package essentials

import "errors"

var ProcessorWaitNext error = errors.New("processor: wait for next round")

type Processor interface {
	Process() error
}
