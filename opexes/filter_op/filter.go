package main

import (
	"bufio"
	"fmt"
	"os"
	"strings"
)

func main() {

	if len(os.Args) != 2 {
		fmt.Fprintln(os.Stderr, "Usage: ./filter <pattern>")
		os.Exit(1)
	}

	pattern := os.Args[1]

	scanner := bufio.NewScanner(os.Stdin)
	for scanner.Scan() {
		line := scanner.Text()

		// Expect input as: key<TAB>value
		parts := strings.SplitN(line, "\t", 2)
		if len(parts) != 2 {
			// Malformed input → skip
			continue
		}

		key := parts[0]
		value := parts[1]

		// Case-sensitive match
		if strings.Contains(value, pattern) {
			// Output as: key<TAB>value
			// Both key and value = full original line
			fmt.Printf("%s\t%s\n", key, value)
		}
	}

	if err := scanner.Err(); err != nil {
		fmt.Fprintln(os.Stderr, "Error reading stdin:", err)
	}
}
