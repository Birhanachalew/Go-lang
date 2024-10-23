
package main

import "fmt"

// Function that sends numbers to the channel
func sendData(ch chan int) {
    for i := 0; i < 5; i++ {
        ch <- i // Send data to channel
    }
    close(ch) // Close the channel when done
}

func main() {
    // Create a channel
    ch := make(chan int)

    // Start a goroutine that sends data
    go sendData(ch)

    // Receive data from the channel
    for val := range ch {
        fmt.Println("Received:", val)
    }

    fmt.Println("Channel closed, program finished.")
}
