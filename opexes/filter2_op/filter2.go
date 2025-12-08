package main

import (
    "bufio"
    "encoding/csv"
    "fmt"
    "os"
    "strconv"
    "strings"
)

func main() {
    // Expect at least: ./filter <N> <Pattern...>
    if len(os.Args) < 3 {
        // Fallback or exit
        os.Exit(1)
    }

    // 1. Get Column Index N (First Argument)
    // Clean any accidental quotes just in case
    cleanN := strings.Trim(os.Args[1], "\"\u201c\u201d")
    N, _ := strconv.Atoi(cleanN)
    if N < 1 {
        N = 1 
    }

    // 2. Get Pattern (Remaining Arguments)
    // We join the remaining args with a space to handle patterns like "SIGN POLE"
    // that the Worker might have split into multiple arguments.
    rawPattern := strings.Join(os.Args[2:], " ")
    pattern := strings.Trim(rawPattern, "\"\u201c\u201d")

    scanner := bufio.NewScanner(os.Stdin)
    for scanner.Scan() {
        line := scanner.Text()
        parts := strings.SplitN(line, "\t", 2)
        if len(parts) != 2 { continue }

        originalKey := parts[0]
        value := parts[1] 

        // 1. Check Filter Condition
        if strings.Contains(value, pattern) {
            
            // 2. Parse CSV to get Column N
            r := csv.NewReader(strings.NewReader(value))
            r.FieldsPerRecord = -1
            cols, err := r.Read()
            
            extractedKey := ""
            if err == nil && len(cols) >= N {
                extractedKey = cols[N-1]
            }

            // 3. Output: <ExtractedKey, 1>
            fmt.Printf("%s\t1\n", extractedKey)
            os.Stdout.Sync()
        } else {
            // No Match: Unblock the worker
            fmt.Printf("%s\t__DROP__\n", originalKey)
            os.Stdout.Sync()
        }
    }

}