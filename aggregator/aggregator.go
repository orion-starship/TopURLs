package aggregator

import (
	"bufio"
	"container/heap"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"path/filepath"
	"sort"
	"strings"
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

// Aggregator aggregates the frequency of all URLs and outputs the top URLs by frequency.
type Aggregator struct {
	inputScanner *bufio.Scanner
	memLimit     int64
	topEntries   int
	tmpDir       string
	shardIndex   int
	err          error
	output       *os.File
}

// New creates an Aggregator that drives the aggregation and output.
func New(input *os.File, memLimit int64, topEntries int, output *os.File) (*Aggregator, error) {
	info, err := input.Stat()
	if err != nil {
		return nil, err
	}
	a := &Aggregator{
		inputScanner: bufio.NewScanner(input),
		memLimit:     memLimit,
		topEntries:   topEntries,
		tmpDir:       filepath.Join(".", fmt.Sprint("tmp-", time.Now().UnixNano())),
		output:       output,
	}
	log.Printf("Input %s has %d bytes; memory limit = %d bytes; expected to use %d shards; tmp dir = %s; output to %s for %d most frequent URLs\n",
		info.Name(), info.Size(), a.memLimit, info.Size()/a.memLimit+1, a.tmpDir, output.Name(), a.topEntries)
	return a, nil
}

// Run writes the top 100 URLs and their frequency counts into the specified output file.
func (a *Aggregator) Run() error {
	os.MkdirAll(a.tmpDir, os.ModePerm)
	defer os.RemoveAll(a.tmpDir)

	if err := a.shuffle(); err != nil {
		return err
	}
	return a.aggregateTop()
}

func (a *Aggregator) shuffle() error {
	var urls []string
	var memUsage int64
	for a.inputScanner.Scan() {
		u := a.inputScanner.Text()
		if len(u) == 0 {
			continue
		}
		if len(u) > 1024 {
			log.Printf("URL is too long, skipping: %s ...", u[:512])
			continue
		}
		urls = append(urls, u)
		memUsage += int64(len(u))
		if memUsage >= a.memLimit {
			sort.Strings(urls)
			if err := a.flushShard(urls); err != nil {
				return err
			}
			urls = make([]string, 0)
			memUsage = 0
		}
	}
	if len(urls) > 0 {
		sort.Strings(urls)
		if err := a.flushShard(urls); err != nil {
			return err
		}
	}
	return a.inputScanner.Err()
}

// flushShard writes down URLs and their frequencies into a file sorted lexigraphically by the URLs.
func (a *Aggregator) flushShard(urls []string) error {
	shardFile, err := os.Create(filepath.Join(a.tmpDir, fmt.Sprintf("shard-%06d", a.shardIndex)))
	if err != nil {
		return err
	}
	defer shardFile.Close()
	out := bufio.NewWriterSize(shardFile, writeBufferSize)
	a.shardIndex++

	// Record the frequency of each URL in this shard.
	var last string
	var count int64
	for _, url := range urls {
		if len(last) != 0 {
			if url == last {
				count++
				continue
			}
			if _, err := out.WriteString(fmt.Sprintf("%s %d\n", last, count)); err != nil {
				return err
			}
		}
		last = url
		count = 1
	}
	if len(last) != 0 {
		if _, err := out.WriteString(fmt.Sprint(last, " ", count)); err != nil {
			return err
		}
	}
	return out.Flush()
}

func (a *Aggregator) aggregateTop() error {
	// Create an iterator that merges Stats from all shards, and outputs one Stat for each URL.
	info, err := ioutil.ReadDir(a.tmpDir)
	if err != nil {
		return err
	}
	if len(info) != a.shardIndex {
		return fmt.Errorf("Expected %d files in temp dir, actual = %d", a.shardIndex, len(info))
	}
	var shards []*shardIterator
	for _, file := range info {
		if !strings.HasPrefix(file.Name(), "shard-") {
			return fmt.Errorf("Unexpected shard file name: %s", file.Name())
		}
		f, err := os.Open(filepath.Join(a.tmpDir, (file.Name())))
		if err != nil {
			return err
		}
		shards = append(shards, newShardIterator(bufio.NewScanner(f), file.Name()))
	}
	statsIt := newStatsIterator(shards)

	// Scan all Stats to keep the entries with the highest count.
	var h countHeap
	for statsIt.Next() {
		if len(h) < a.topEntries || h.MinCount() <= statsIt.Value().Count {
			heap.Push(&h, statsIt.Value())
		}
		for len(h) > a.topEntries {
			heap.Pop(&h)
		}
	}

	// Output the most frequency URLs, ordered by their frequency.
	sort.Slice(h, func(i, j int) bool {
		return !byLowerCount(h, i, j)
	})
	out := bufio.NewWriterSize(a.output, writeBufferSize)
	for _, stat := range h {
		if _, err := out.WriteString(fmt.Sprint(stat.URL, " ", stat.Count, "\n")); err != nil {
			return err
		}
	}
	return out.Flush()
}

// This comparison function orders Stats based on their count and URL. It ensures deterministic output.
func byLowerCount(stats []*Stat, i, j int) bool {
	if stats[i].Count != stats[j].Count {
		return stats[i].Count < stats[j].Count
	}
	return stats[i].URL > stats[j].URL
}

// A min heap for tracking Stats with the highest counts.
type countHeap []*Stat

func (h countHeap) Len() int { return len(h) }

func (h countHeap) Less(i, j int) bool {
	return byLowerCount(h, i, j)
}

func (h countHeap) Swap(i, j int) {
	h[i], h[j] = h[j], h[i]
}

func (h *countHeap) Push(stat interface{}) {
	*h = append(*h, stat.(*Stat))
}

func (h *countHeap) Pop() interface{} {
	prev := *h
	n := len(prev)
	it := prev[n-1]
	prev[n-1] = nil
	*h = prev[0 : n-1]
	return it
}

func (h *countHeap) MinCount() int64 {
	return (*h)[0].Count
}
