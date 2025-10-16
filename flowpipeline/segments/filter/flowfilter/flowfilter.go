// Runs flows through a filter and forwards only matching flows. Reuses our own
// https://github.com/BelWue/flowfilter project, see the docs there.
package flowfilter

import (
	"sync"

	"github.com/rs/zerolog/log"

	"regexp"

	"github.com/BelWue/flowfilter/parser"
	"github.com/BelWue/flowpipeline/pb"
	"github.com/BelWue/flowpipeline/segments"
)

type FlowFilter struct {
	segments.BaseFilterSegment
	Filter string // optional, default is empty

	expression *parser.Expression
	useTid     bool
}

func (segment FlowFilter) New(config map[string]string) segments.Segment {
	var err error

	newSegment := &FlowFilter{
		Filter: config["filter"],
	}

	// Allow users to write `tid` in filter expressions; internally rewrite to `cid`
	// and remember to evaluate against Tid rather than Cid.
	if containsTid(config["filter"]) {
		newSegment.useTid = true
		rewritten := rewriteTidToCid(config["filter"])
		newSegment.expression, err = parser.Parse(rewritten)
	} else {
		newSegment.expression, err = parser.Parse(config["filter"])
	}
	if err != nil {
		log.Error().Err(err).Msg("FlowFilter: Syntax error in filter expression: ")
		return nil
	}
	filter := &Filter{}
	if _, err := filter.CheckFlow(newSegment.expression, &pb.EnrichedFlow{}); err != nil {
		log.Error().Err(err).Msg("FlowFilter: Semantic error in filter expression: ")
		return nil
	}
	return newSegment
}

func (segment *FlowFilter) Run(wg *sync.WaitGroup) {
	defer func() {
		close(segment.Out)
		wg.Done()
	}()

	log.Info().Msgf("FlowFilter: Using filter expression: %s", segment.Filter)

	filter := &Filter{}
	filter.useTid = segment.useTid
	for msg := range segment.In {
		if match, _ := filter.CheckFlow(segment.expression, msg); match {
			segment.Out <- msg
		} else if segment.Drops != nil {
			segment.Drops <- msg
		}
	}
}

func init() {
	segment := &FlowFilter{}
	segments.RegisterSegment("flowfilter", segment)
}

// containsTid returns true if the filter expression references the `tid` key
// as a standalone identifier (case-insensitive). We use \b word boundaries
// which in RE2 include letters, digits and underscores, matching our DSL.
func containsTid(s string) bool {
	re := regexp.MustCompile(`(?i)\btid\b`)
	return re.FindStringIndex(s) != nil
}

// rewriteTidToCid replaces standalone `tid` occurrences with `cid` (case-insensitive).
func rewriteTidToCid(s string) string {
	// Use capturing groups to keep surrounding text intact while swapping only the token.
	re := regexp.MustCompile(`(?i)(\b)tid(\b)`)
	return re.ReplaceAllString(s, `${1}cid${2}`)
}
