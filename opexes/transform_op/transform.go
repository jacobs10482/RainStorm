package main

import (
	"bufio"
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
			continue // malformed input
		}

		key := parts[0]
		value := parts[1]

		// Split CSV fields
		cols := strings.Split(value, ",")

		// Extract fields 1-3 (1-indexed)
		f1 := ""
		f2 := ""
		f3 := ""

		if len(cols) >= 1 {
			f1 = cols[0]
		}
		if len(cols) >= 2 {
			f2 = cols[1]
		}
		if len(cols) >= 3 {
			f3 = cols[2]
		}

		// Construct new value
		newValue := fmt.Sprintf("%s,%s,%s", f1, f2, f3)

		// Output transformed tuple
		fmt.Printf("%s\t%s\n", key, newValue)
	}

	if err := scanner.Err(); err != nil {
		fmt.Fprintln(os.Stderr, "Error reading stdin:", err)
	}
}
