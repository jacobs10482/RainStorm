package main

import (
    "bufio"
    "fmt"
    "os"
    "strconv"
    "strings"
)

func main() {
    // We allow an argument (N) to be passed for consistency, 
    // but we don't strictly require it to be used.
    // So we just don't check os.Args length strictly or we check >= 1.

    counts := make(map[string]int)

    scanner := bufio.NewScanner(os.Stdin)
    for scanner.Scan() {
        line := scanner.Text()
        
        // Expect input from Filter: <Key> \t <1>
        parts := strings.SplitN(line, "\t", 2)
        if len(parts) < 2 { continue }

        key := parts[0]
        valStr := parts[1]

        countDelta, _ := strconv.Atoi(valStr)
        if countDelta == 0 { countDelta = 1 } 

        counts[key] += countDelta

        fmt.Printf("%s\t%d\n", key, counts[key])
    }
}