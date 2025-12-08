package main

import (
	"bufio"
	"fmt"
	"os"
	"strings"
)

func main() {

	if len(os.Args) < 2 {
		fmt.Fprintln(os.Stderr, "Usage: ./filter <pattern>")
		os.Exit(1)
	}

	// Join all arguments to handle cases where AssignTask splits "Street Name" into multiple args
	rawPattern := strings.Join(os.Args[1:], " ")
	pattern := strings.Trim(rawPattern, "\"\u201c\u201d")

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
			// Output matched tuples as: key<TAB>value
			fmt.Printf("%s\t%s\n", key, value)
			os.Stdout.Sync()
		} else {
			// For non-matching (filtered-out) tuples, emit a special drop marker
			// so the worker does not block waiting for operator output.
			fmt.Printf("%s\t__DROP__\n", key)
			os.Stdout.Sync()
		}
	}

	if err := scanner.Err(); err != nil {
		fmt.Fprintln(os.Stderr, "Error reading stdin:", err)
	}
}
