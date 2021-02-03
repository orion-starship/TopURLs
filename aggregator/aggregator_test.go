package aggregator

import (
	"fmt"
	"io/ioutil"
	"math/rand"
	"os"
	"sort"
	"strings"
	"testing"
	"time"
)

func TestAggregatorRandomized(t *testing.T) {
	rand.Seed(time.Now().UTC().UnixNano())

	// Create input file.
	input, err := ioutil.TempFile(".", "input")
	if err != nil {
		t.Fatal(err)
	}
	inputName := input.Name()
	defer os.Remove(inputName)

	// Initialize input data.
	sampleURLs := []string{
		"https://pingcap.com/",
		"https://www.bilibili.com/",
		"https://www.github.com/",
		"https://www.amazon.com/",
		"https://store.steampowered.com/",
		"http://a.z/",
		"http://maps.google.com/",
		"http://www.facebook.com/",
		"http://www.instagram.com/",
		"http://www.apple.com/",
		"http://www.sina.com.cn/",
		"https://www.google.com/maps/place/Laurelwood+Park/@37.5250563,-122.3218649,17.13z/data=!4m8!1m2!2m1!1spingcap+redwood+city!3m4!1s0x808f9fa52732d6fd:0x3e9cafe9b58dc5e0!8m2!3d37.5260496!4d-122.3232331",
		"https://github.com/pingcap/tidb/blob/release-5.0-rc/distsql/stream.go",
		"https://www.google.com/maps/place/Mentougou+District,+Beijing,+China/@39.9952896,115.5192323,10z/data=!3m1!4b1!4m5!3m4!1s0x35f069d0e2a0d0db:0x30076dd69ae5de28!8m2!3d39.9406479!4d116.1020082",
	}
	inputEntries := 200 + rand.Intn(2000)
	inputSize := 0
	count := make(map[string]int)
	for i := 0; i < inputEntries; i++ {
		url := sampleURLs[rand.Intn(len(sampleURLs))]
		inputSize += len(url)
		count[url]++
		input.WriteString(url)
		input.WriteString("\n")
	}
	if err := input.Sync(); err != nil {
		t.Fatal(err)
	}

	// Reopen input file for reading.
	if err := input.Close(); err != nil {
		t.Fatal(err)
	}
	if input, err = os.Open(inputName); err != nil {
		t.Fatal(err)
	}

	// Create output file.
	output, err := ioutil.TempFile(".", "output")
	if err != nil {
		t.Fatal(err)
	}
	defer output.Close()
	defer os.Remove(output.Name())

	// Count frequency of each URL and output the top.
	memLimit := 2000 + rand.Intn(6000)
	// Limit number of shards to <= 201. Openning too many files may exceed the process' limit.
	if memLimit < inputSize/200 {
		memLimit = inputSize / 200
	}
	topLimit := 1 + rand.Intn(len(sampleURLs)-1)
	aggregator, err := New(input, int64(memLimit), topLimit, output)
	if err != nil {
		t.Fatal(err)
	}
	if err := aggregator.Run(); err != nil {
		t.Fatal(err)
	}

	// Generate expected data.
	var wantStats []*Stat
	for u, c := range count {
		wantStats = append(wantStats, &Stat{URL: u, Count: int64(c)})
	}
	sort.Slice(wantStats, func(i, j int) bool {
		return !byLowerCount(wantStats, i, j)
	})
	var wantBuilder strings.Builder
	for i := 0; i < topLimit; i++ {
		if _, err := wantBuilder.WriteString(fmt.Sprintf("%s %d\n", wantStats[i].URL, wantStats[i].Count)); err != nil {
			t.Fatal(err)
		}
	}
	want := wantBuilder.String()

	rawStats, err := ioutil.ReadFile(output.Name())
	if err != nil {
		t.Fatal(err)
	}
	got := string(rawStats)
	if want != got {
		t.Fatalf("got\n%s\nwant\n%s\n", got, want)
	}
}
