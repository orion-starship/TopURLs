package aggregator

import (
	"bufio"
	"container/heap"
	"fmt"
	"log"
	"strconv"
	"strings"
)

// Iterator for a single shard file, line by line.
// The file is assumed to be sorted by URL.
type shardIterator struct {
	// Reads underlying file.
	scanner *bufio.Scanner
	// Parsed Stat of the current line.
	stat *Stat

	file string
	err  error
	done bool
}

func newShardIterator(scanner *bufio.Scanner, file string) *shardIterator {
	return &shardIterator{scanner: scanner, file: file}
}

// Iterates to the next position in the shard file.
// Returns true on success,. Returns false when there is an error, or at EOF.
func (s *shardIterator) Next() bool {
	if s.err != nil || s.done {
		return false
	}
	if !s.scanner.Scan() {
		if s.scanner.Err() != nil {
			s.err = s.scanner.Err()
		}
		s.done = true
		return false
	}
	line := s.scanner.Text()
	idx := strings.LastIndexByte(line, ' ')
	url := line[:idx]
	if s.stat != nil && url <= s.stat.URL {
		s.err = fmt.Errorf("shard file %s is unsorted. previous: %v, next line:\n%s", s.file, *s.stat, line)
		return false
	}
	s.stat = &Stat{URL: url}
	s.stat.Count, s.err = strconv.ParseInt(line[idx+1:], 10, 64)
	if s.err != nil {
		return false
	}
	return true
}

func (s *shardIterator) Value() *Stat {
	return s.stat
}

func (s *shardIterator) Err() error {
	return s.err
}

func (s *shardIterator) Done() bool {
	return s.done
}

type iteratorHeap []*shardIterator

func (h iteratorHeap) Len() int { return len(h) }

func (h iteratorHeap) Less(i, j int) bool {
	return h[i].stat.URL < h[j].stat.URL
}

func (h iteratorHeap) Swap(i, j int) {
	h[i], h[j] = h[j], h[i]
}

func (h *iteratorHeap) Push(it interface{}) {
	*h = append(*h, it.(*shardIterator))
}

func (h *iteratorHeap) Pop() interface{} {
	prev := *h
	n := len(prev)
	it := prev[n-1]
	prev[n-1] = nil
	*h = prev[0 : n-1]
	return it
}

// Iterator for URL stats over all shards, sorted by URL in lexicographical order.
type statsIterator struct {
	heap iteratorHeap
	stat *Stat
	err  error
}

func newStatsIterator(shards []*shardIterator) *statsIterator {
	s := &statsIterator{}
	for _, shard := range shards {
		if shard.Value() != nil {
			log.Fatalf("already started iterating file %s value %v", shard.file, *shard.stat)
		}
		if !shard.Next() {
			if shard.Err() != nil {
				log.Fatalf("failed to start iterating file %s", shard.file)
			}
			// Ignore empty shards.
			continue
		}
		heap.Push(&s.heap, shard)
	}
	return s
}

// Iterates to the next URL with count.
// Returns true on success,. Returns false when there is an error, or at EOF for all underlying shards.
func (s *statsIterator) Next() bool {
	if s.Done() {
		return false
	}
	s.stat = &Stat{URL: s.peek().URL, Count: 0}
	for !s.Done() {
		next := s.peek()
		if s.stat.URL != next.URL {
			return true
		}
		s.stat.Count += next.Count
		it := heap.Pop(&s.heap).(*shardIterator)
		if !it.Next() {
			if it.Err() != nil {
				s.err = it.Err()
				return false
			}
		} else {
			heap.Push(&s.heap, it)
		}
	}
	return true
}

func (s *statsIterator) Value() *Stat {
	return s.stat
}

func (s *statsIterator) Err() error {
	return s.err
}

func (s *statsIterator) Done() bool {
	return len(s.heap) == 0
}

func (s *statsIterator) peek() *Stat {
	if s.Done() {
		return nil
	}
	return s.heap[0].stat
}
