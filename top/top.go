package main

import (
	"flag"
	"log"
	"os"
	"top_urls/aggregator"
)

var inputFile = flag.String("input", "example_urls.txt", "Input file name.")
var outputFile = flag.String("output", "top_urls.txt", "Output file name.")
var memLimit = flag.Int64("mem_limit", 768<<20 /* 768MiB */, "Memory usage limit.")
var topEntries = flag.Int("top_entries", 100, "Number of the most frequency URLs to output.")

func main() {
	flag.Parse()

	input, err := os.Open(*inputFile)
	if err != nil {
		log.Fatal("Unable to open input file '", *inputFile, "': ", err)
	}
	defer input.Close()

	output, err := os.Create(*outputFile)
	if err != nil {
		log.Fatal("Unable to create output file '", *outputFile, "': ", err)
	}
	defer output.Close()

	aggregator, err := aggregator.New(input, *memLimit, *topEntries, output)
	if err != nil {
		log.Fatal(err)
	}
	if err := aggregator.Run(); err != nil {
		log.Fatal(err)
	}
}
