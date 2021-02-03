This program:
1.  Takes a list of URLs as input (e.g. `top/example_urls.txt`).
2.  Processes the input by chunk (chunk size default to `768MiB`).
3.  In each chunk, records the frequency of each URL, sorted by URL.
4.  Aggregates the frequency per unique URL across all chunks.
5.  Keeps the top 100 (default) URLs with the highest frequencies.

Computation complexity is `O(n lg n)`, from sorting URLs for each chunk.
Memory usage is bounded by the chunk size, which is `768MiB` plus overhead.
Disk access is `O(input size)`.

To run on sample data,

```bash
top $ go build
top $ ./top
```

TODO:
Test more error cases.
