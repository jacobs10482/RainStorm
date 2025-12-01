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

		// Case-sensitive match
		if strings.Contains(line, pattern) {
			// Output as: key<TAB>value
			// Both key and value = full original line
			fmt.Printf("%s\t%s\n", line, line)
		}
	}

	if err := scanner.Err(); err != nil {
		fmt.Fprintln(os.Stderr, "Error reading stdin:", err)
	}
}
