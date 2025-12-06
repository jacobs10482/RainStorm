package main

import (
	"bufio"
	"encoding/csv"
	"fmt"
	"os"
	"strings"
)

func main() {

	scanner := bufio.NewScanner(os.Stdin)

	for scanner.Scan() {
		line := scanner.Text()

		// Expect input as: key<TAB>value
		parts := strings.SplitN(line, "\t", 2)
		if len(parts) != 2 {
			continue
		}

		key := parts[0]
		value := parts[1]

		// Use CSV parser (handles quoted commas correctly)
		r := csv.NewReader(strings.NewReader(value))
		r.FieldsPerRecord = -1 // allow variable column count

		cols, err := r.Read()
		if err != nil {
			// Malformed CSV → skip
			continue
		}

		// Extract fields 1–3 (1-indexed)
		f1, f2, f3 := "", "", ""

		if len(cols) >= 1 {
			f1 = cols[0]
		}
		if len(cols) >= 2 {
			f2 = cols[1]
		}
		if len(cols) >= 3 {
			f3 = cols[2]
		}

		// Output transformed tuple
		newValue := fmt.Sprintf("%s,%s,%s", f1, f2, f3)
		fmt.Printf("%s\t%s\n", key, newValue)
	}

	if err := scanner.Err(); err != nil {
		fmt.Fprintln(os.Stderr, "Error reading stdin:", err)
	}
}
