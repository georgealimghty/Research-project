package matching

import (
	"bufio"
	"fmt"
	"log"
	"net"
	"os"
	"strings"
	"sync"

	"github.com/BelWue/flowpipeline/segments"
)

type MatchingSegment struct {
	segments.BaseSegment
	CSVData    map[string]string
	ipListPath string
}

// New implements segments.Segment.
func (m *MatchingSegment) New(config map[string]string) segments.Segment {
	println("Initializing matching segment....")

	m.CSVData = make(map[string]string)
	m.ipListPath = "segments/matching/bad_ips.txt"
	if p, ok := config["ip_list_path"]; ok && strings.TrimSpace(p) != "" {
		m.ipListPath = p
	}

	return m
}

// Run implements segments.Segment.
func (m *MatchingSegment) Run(wg *sync.WaitGroup) {
	defer func() {
		close(m.Out)
		wg.Done()
	}()

	// Initialization stage: load IP list once
	file, err := os.Open(m.ipListPath)
	if err != nil {
		log.Fatalf("matching: failed to open ip list file '%s': %v", m.ipListPath, err)
	}
	scanner := bufio.NewScanner(file)
	scanner.Buffer(make([]byte, 0, 64*1024), 1024*1024)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		m.CSVData[line] = "bad_ip"
	}
	if err := scanner.Err(); err != nil {
		_ = file.Close()
		log.Fatalf("matching: error reading ip list file '%s': %v", m.ipListPath, err)
	}
	if err := file.Close(); err != nil {
		log.Fatalf("matching: error closing ip list file '%s': %v", m.ipListPath, err)
	}
	fmt.Printf("matching: loaded %d IPs from %s\n", len(m.CSVData), m.ipListPath)

	// Processing stage: continuous loop on incoming flows
	for msg := range m.In {
		matched := false
		// Prefer existing string fields if populated, otherwise derive from bytes
		src := msg.GetSourceIP()
		if src == "" {
			src = net.IP(msg.GetSrcAddr()).String()
		}
		dst := msg.GetDestinationIP()
		if dst == "" {
			dst = net.IP(msg.GetDstAddr()).String()
		}

		if src != "" {
			if _, ok := m.CSVData[src]; ok {
				matched = true
			}
		}
		if dst != "" {
			if _, ok := m.CSVData[dst]; ok {
				matched = true
			}
		}

		if matched {
			if msg.Note == "" {
				msg.Note = "inlist"
			}
			if src != "" && m.CSVData[src] != "" {
				fmt.Printf("MATCH FOUND! IP: %s is on the blocklist\n", src)
			} else if dst != "" && m.CSVData[dst] != "" {
				fmt.Printf("MATCH FOUND! IP: %s is on the blocklist\n", dst)
			}
		}

		m.Out <- msg
	}
}

// Name implements segments.Segment.
func (m *MatchingSegment) Name() string {
	return "matching"
}

// Close implements segments.Segment.
func (m *MatchingSegment) Close() {
	println("Closing matching segment.")
}

func init() {
	matchingSegment := &MatchingSegment{}
	segments.RegisterSegment("matching", matchingSegment)
}
