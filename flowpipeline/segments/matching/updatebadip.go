package matching

import (
	"bufio"
	"io"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"
)

// Environment variables to override defaults:
// BAD_IP_URL: remote URL to fetch the list from
// BAD_IP_PATH: local path to write the list to

func init() {
	go startBadIPUpdater()
}

func startBadIPUpdater() {
	url := strings.TrimSpace(os.Getenv("BAD_IP_URL"))
	if url == "" {
		url = "https://nerd.cesnet.cz/nerd/data/bad_ips.txt"
	}

	path := strings.TrimSpace(os.Getenv("BAD_IP_PATH"))
	if path == "" {
		path = "segments/matching/bad_ips.txt"
	}

	// If download fails, try again later
	if err := downloadAndReplace(url, path); err != nil {
		log.Printf("matching: initial bad_ips update failed: %v", err)
	}

	ticker := time.NewTicker(time.Hour)
	defer ticker.Stop()
	for range ticker.C {
		if err := downloadAndReplace(url, path); err != nil {
			log.Printf("matching: bad_ips update failed: %v", err)
		}
	}
}

// downloadAndReplace downloads from url, writes to a temp file in the same
// directory as dest, then atomically renames it into place.
func downloadAndReplace(url string, dest string) error {
	resp, err := http.Get(url)
	if err != nil {
		return err
	}
	defer resp.Body.Close() //The response from the server (resp.Body) is a data stream that needs to be closed when you're done with it. defer ensures it gets closed automatically, even if errors happen later.

	if resp.StatusCode != http.StatusOK {
		return &httpStatusError{code: resp.StatusCode}
	}

	dir := filepath.Dir(dest)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return err
	}

	tmp, err := os.CreateTemp(dir, "bad_ips_*.txt")
	if err != nil {
		return err
	}
	tmpPath := tmp.Name()

	// copy and normalize newlines
	writer := bufio.NewWriter(tmp)
	if _, err := io.Copy(writer, resp.Body); err != nil {
		tmp.Close()
		os.Remove(tmpPath)
		return err
	}
	if err := writer.Flush(); err != nil {
		tmp.Close()
		os.Remove(tmpPath)
		return err
	}
	if err := tmp.Close(); err != nil {
		os.Remove(tmpPath)
		return err
	}

	// atomic replace
	if err := os.Rename(tmpPath, dest); err != nil {
		os.Remove(tmpPath)
		return err
	}
	log.Printf("matching: updated %s from %s", dest, url)
	return nil
}

type httpStatusError struct{ code int }

func (e *httpStatusError) Error() string { return "unexpected HTTP status: " + http.StatusText(e.code) }
