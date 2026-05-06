package main

import (
	"net"
	"fmt"
	"os"
	"log"
	"time"
)
const metricsInterval = 5 * time.Second

type Metrics struct {
	totalBytes int
	chunkCount int
	lastArrival time.Time
	intervalSum time.Duration
	lastPrintTime time.Time
}

func main() {
	if len(os.Args) != 2 {
		log.Fatalf("Usage: ./snowcast_listener <listener_port>")
		return
	}

	listenPort := os.Args[1]
	addr, err := net.ResolveUDPAddr("udp", ":"+listenPort)
	if err != nil {
		fmt.Println("Error Resolving Address: ", err)
		return
	}
	conn, err := net.ListenUDP("udp", addr)
	if err != nil {
		fmt.Println("Error Connecting to Port: ", err)
		return
	}

	buffer := make([]byte, 1500)
	m := &Metrics{lastPrintTime: time.Now()}

	for {
		n, err := conn.Read(buffer)
		if err != nil {
			fmt.Println("Error reading from UDP connection: ", err)
			continue
		}
		now := time.Now()

		// start tracking inter-arrival interval
		if !m.lastArrival.IsZero() {
			m.intervalSum += now.Sub(m.lastArrival)
		}
		m.lastArrival = now
		m.totalBytes += n
		m.chunkCount++

		if now.Sub(m.lastPrintTime) >= metricsInterval {
			elapsed := now.Sub(m.lastPrintTime).Seconds()
			throughput := float64(m.totalBytes) / elapsed / 1024.0

			avgInterval := 0.0
			if m.chunkCount > 1 {
				avgInterval = float64(m.intervalSum.Milliseconds()) / float64(m.chunkCount-1)
			}

			// buffer health for seeing how close interval is to expected tick
			health := "good"
			if avgInterval < 70 || avgInterval > 120 {
				health = "degraded"
			}

			fmt.Fprintf(os.Stderr, "\n[metrics] throughput: %.1f KB/s | avg interval: %.1fms | buffer: %s | chunks: %d\n",
				throughput, avgInterval, health, m.chunkCount)

			m.totalBytes = 0
			m.chunkCount = 0
			m.intervalSum = 0
			m.lastPrintTime = now
		}
	}
}