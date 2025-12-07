package main

import (
	"bufio"
	"encoding/csv"
	"fmt"
	"os"
	"strings"
)

func main() {

	if len(os.Args) != 2 {
		fmt.Fprintln(os.Stderr, "Usage: ./aggregate <N>")
		os.Exit(1)
	}

	// Column index (1-indexed)
	N := 0
	fmt.Sscanf(os.Args[1], "%d", &N)
	if N <= 0 {
		fmt.Fprintln(os.Stderr, "Column index N must be >= 1")
		os.Exit(1)
	}

	counts := make(map[string]int)

	scanner := bufio.NewScanner(os.Stdin)
	for scanner.Scan() {

		line := scanner.Text()

		// Expect: key<TAB>value
		parts := strings.SplitN(line, "\t", 2)
		if len(parts) < 2 {
			// Malformed input → skip
			continue
		}

		value := parts[1]           // full original dataset line
		r := csv.NewReader(strings.NewReader(value))
		r.FieldsPerRecord = -1 // allow variable column count

		cols, err := r.Read()
		if err != nil {
			// Malformed CSV → skip
			continue
		}

		var key string
		if len(cols) < N {
			// Missing → empty string
			key = ""
		} else {
			// N is 1-indexed → cols[N-1]
			key = cols[N-1]
		}

		// Count occurrences
		counts[key]++
		fmt.Printf("%s\t%d\n", key, counts[key])
	}

	if err := scanner.Err(); err != nil {
		fmt.Fprintln(os.Stderr, "Error reading stdin:", err)
	}

}
