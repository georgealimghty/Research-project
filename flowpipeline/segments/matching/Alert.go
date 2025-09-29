package matching

import (
	"github.com/BelWue/flowpipeline/segments"
)

type Alert struct {
	segments.BaseSegment
}

// func (segment Alert) New(config map[string]string) segments.Segment {
// 	return &Alert{}
// }

// func (segment Alert) Run(wg *sync.WaitGroup) {
// 	defer func() {
// 		close(segment.Out)
// 		wg.Done()
// 	}()
// }

// func init() {
// 	segment := &Alert{}
// 	segments.RegisterSegment("alert", segment)
// }
