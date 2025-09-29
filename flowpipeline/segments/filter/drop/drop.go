package drop

import (
	"sync"

	"github.com/BelWue/flowpipeline/segments"
)

type Drop struct {
	segments.BaseFilterSegment
}

func (segment Drop) New(config map[string]string) segments.Segment {
	return &Drop{}
}

func (segment *Drop) Run(wg *sync.WaitGroup) {
	defer func() {
		close(segment.Out)
		wg.Done()
	}()

	for msg := range segment.In {
		if segment.Drops != nil {
			segment.Drops <- msg
		}
	}
}

func init() {
	segment := &Drop{}
	segments.RegisterSegment("drop", segment)
}
