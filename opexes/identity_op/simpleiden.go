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
        line := strings.TrimSpace(scanner.Text())
        if line == "" {
            continue
        }

        // Expect: key<tab>value
        // Just output the same line.
        fmt.Println(line)
    }

    if err := scanner.Err(); err != nil {
        fmt.Fprintln(os.Stderr, "scanner error:", err)
    }
}
