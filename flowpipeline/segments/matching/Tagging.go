package matching

import (
	"strings"

	"github.com/BelWue/flowpipeline/pb"
)

// Default tagging constants used to mark matched flows so they can be filtered later.
// These values are intentionally chosen to be uncommon in regular traffic so that
// downstream filter expressions can easily select them.
const (
	// Tag ID placed into EnrichedFlow.Tid. Use this in flowfilter: `tid 65001`.
	TagIDBadIP uint32 = 65001
)

// TagAsBadIP annotates the given flow as matched against a bad IP list.
// This sets fields that are easy to reference from flowfilter:
// - Tid: numeric tag identifier
// - Inlist: boolean convenience flag for list membership
// - Note: short human-readable marker (lowercase, no spaces)
func TagAsBadIP(flow *pb.EnrichedFlow) {
	if flow == nil {
		return
	}
	flow.Tid = TagIDBadIP
	flow.Inlist = true
	// Keep the note short and machine-friendly so it can also be parsed if needed.
	setNoteIfEmpty(flow, "bad_ip")
}

// TagWithID applies a generic numeric tag to the flow and toggles Inlist=true.
// Use when you want to assign a specific tag distinct from the default bad IP tag.
func TagWithID(flow *pb.EnrichedFlow, tagID uint32, note string) {
	if flow == nil {
		return
	}
	flow.Tid = tagID
	flow.Inlist = true
	setNoteIfEmpty(flow, note)
}

// setNoteIfEmpty sets Note if it's currently empty. The value is normalized to a
// short, lowercase, hyphen/underscore-only token to keep it machine-friendly.
func setNoteIfEmpty(flow *pb.EnrichedFlow, note string) {
	if flow.Note != "" {
		return
	}
	n := strings.TrimSpace(note)
	if n == "" {
		return
	}
	n = strings.ToLower(n)
	n = strings.ReplaceAll(n, " ", "_")
	flow.Note = n
}
