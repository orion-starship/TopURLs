package main

import (
	"flag"
	"fmt"
)

var total = flag.Int("total", 1000, "Total number of URLs to generate.")

func main() {
	for i := 0; i < *total; i++ {
		fmt.Println("Hello, 世界")
	}
}
