package aggregator

import (
	"bufio"
	"fmt"
	"reflect"
	"strings"
	"testing"
)

func TestShardIterator(t *testing.T) {
	const input = "a 1\nb 5\nc 3\nd 4"
	it := newShardIterator(bufio.NewScanner(strings.NewReader(input)), "test")

	if it.Done() {
		t.Fatalf("got it.Done(), want false")
	}
	if it.Err() != nil {
		t.Fatalf("got it.Err() == %s, want nil", it.Err())
	}

	wants := []Stat{
		{URL: "a", Count: 1},
		{URL: "b", Count: 5},
		{URL: "c", Count: 3},
		{URL: "d", Count: 4},
	}
	for _, want := range wants {
		if !it.Next() {
			t.Fatalf("got it.Next() == false, want true")
		}
		if !reflect.DeepEqual(*it.Value(), want) {
			t.Errorf("got %v, want %v", *it.Value(), want)
		}
	}

	if it.Next() {
		t.Errorf("got it.Next() == true, want false")
	}
	if !it.Done() {
		t.Error("got !it.Done(), want true")
	}
	if it.Err() != nil {
		t.Errorf("got it.Err() == %s, want nil", it.Err())
	}
}

func TestStatsIterator(t *testing.T) {
	inputs := []string{
		"a 1\nb 5\nc 3\nd 4",
		"b 7\nc 4\ne 1",
		"a 12\nd 2\nf 3",
		"",
	}
	var shards []*shardIterator
	for i, input := range inputs {
		shards = append(shards, newShardIterator(bufio.NewScanner(strings.NewReader(input)), fmt.Sprint("test", i)))
	}
	it := newStatsIterator(shards)

	if it.Done() {
		t.Fatalf("got it.Done(), want false")
	}
	if it.Err() != nil {
		t.Fatalf("got it.Err() == %s, want nil", it.Err())
	}

	wants := []Stat{
		{URL: "a", Count: 13},
		{URL: "b", Count: 12},
		{URL: "c", Count: 7},
		{URL: "d", Count: 6},
		{URL: "e", Count: 1},
		{URL: "f", Count: 3},
	}
	for _, want := range wants {
		if !it.Next() {
			t.Fatalf("got it.Next() == false, want true")
		}
		if !reflect.DeepEqual(*it.Value(), want) {
			t.Errorf("got %v, want %v", *it.Value(), want)
		}
	}

	if it.Next() {
		t.Errorf("got it.Next() == true, want false")
	}
	if !it.Done() {
		t.Error("got !it.Done(), want true")
	}
	if it.Err() != nil {
		t.Errorf("got it.Err() == %s, want nil", it.Err())
	}
}
