package essentials

import (
	"sync"
	"time"
)

type InfiniteProcessor struct {
	sess       *Session
	processors []Processor
	wg         sync.WaitGroup
	running    bool
	mu         sync.Mutex
}

func NewInfiniteProcessor(sess *Session) *InfiniteProcessor {
	return &InfiniteProcessor{
		sess:    sess,
		running: false,
		processors: []Processor{
			NewSucceedMessageProcessor(sess),
			NewFailedMessageProcessor(sess),
			NewRollbackMessageProcessor(sess),
		},
	}
}

func (p *InfiniteProcessor) Stop() {
	p.setRunning(false)
	p.wg.Wait()
}

func (p *InfiniteProcessor) Process() {
	p.setRunning(true)
	for _, processor := range p.processors {
		go p.infiniteProcess(processor)
		p.wg.Add(1)
	}
}

func (p *InfiniteProcessor) setRunning(v bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.running = v
}

func (p *InfiniteProcessor) getRunning() bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.running
}

func (p *InfiniteProcessor) infiniteProcess(processor Processor) {
	for {
		if !p.getRunning() {
			goto ForEnd
		}

		err := processor.Process()
		if err != nil {
			p.sess.Logger().Errorln(err)
		}
		if err != ProcessorWaitNext {
			p.sess.Logger().Debugln("InfiniteProcessor wait for next round")
			time.Sleep(time.Second * 60)
		}
	}
ForEnd:
	p.wg.Done()
}
