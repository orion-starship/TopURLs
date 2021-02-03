package aggregator

import (
	"bufio"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"sort"
	"time"
)

const (
	writeBufferSize = 1 << 20 // 1MiB
)

// Stat contains the frequency count of a single URL in the input.
type Stat struct {
	URL   string
	Count int64
}

// Aggregator aggregates the counts of all URLs and outputs the top 100 URLs by count.
type Aggregator struct {
	inputScanner *bufio.Scanner
	inputSize    int64
	memLimit     int64
	shardIndex   int
	err          error
	urls         []string
	memUsage     int64
	tmpDir       string
}

// New creates an Aggregator from input file and memory limit.
func New(input *os.File, memLimit int64) (*Aggregator, error) {
	info, err := input.Stat()
	if err != nil {
		return nil, err
	}
	a := &Aggregator{
		inputScanner: bufio.NewScanner(input),
		inputSize:    info.Size(),
		memLimit:     memLimit,
		tmpDir:       filepath.Join(".", fmt.Sprint("tmp-", time.Now().UnixNano())),
	}
	log.Printf("Input file %d bytes, memory limit %d bytes, using tmp dir %s\n", a.inputSize, a.memLimit, a.tmpDir)
	return a, nil
}

// TopURLs outputs the top 100 URLs and their frequency counts.
func (a *Aggregator) TopURLs() ([]Stat, error) {
	os.MkdirAll(a.tmpDir, os.ModePerm)
	defer os.RemoveAll(a.tmpDir)

	var urls []string
	var memUsage int64
	for a.inputScanner.Scan() {
		s := a.inputScanner.Text()
		urls = append(urls, s)
		memUsage += int64(len(s))
		if memUsage >= a.memLimit {
			if err := a.flush(urls); err != nil {
				return nil, err
			}
			urls = make([]string, 0)
			memUsage = 0
		}
	}
	if a.inputScanner.Err() != nil {
		return nil, a.inputScanner.Err()
	}

	return a.top()
}

func (a *Aggregator) flush(urls []string) error {
	outputFile, err := os.Create(filepath.Join(a.tmpDir, fmt.Sprintf("shard-%06d", a.shardIndex)))
	if err != nil {
		return err
	}
	defer outputFile.Close()
	out := bufio.NewWriterSize(outputFile, writeBufferSize)
	a.shardIndex++

	sort.Strings(urls)
	log.Printf("urls: %v", urls)
	var last *string
	var count int64
	for _, url := range urls {
		if last != nil {
			if url == *last {
				count++
				continue
			}
			out.WriteString(fmt.Sprintf("%s %d\n", *last, count))
		}
		last = &url
		count = 1
	}
	if last != nil {
		out.WriteString(fmt.Sprint(*last, " ", count))
	}
	return out.Flush()
}

func (a *Aggregator) top() ([]Stat, error) {
	var stats []Stat

	return stats, nil
}
