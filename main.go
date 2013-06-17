package main

import (
	"flag"
	"runtime"
	"log"
	"net"
	"fmt"
	"strings"
	"time"
)

const (
	GET = "get %s\r\n"
	SET = "set %s 0 0 %d\r\n%s\r\n"
)

var (
	host = flag.String("host", ":11211", "Host of memcache server")
	size = flag.Int("size", 1000, "Size of key value")
	iterations = flag.Int("n", 100, "How many requests total")
	workers = flag.Int("workers", 100, "Number of workers")
)

var value string

func listen(channel chan string) {
	socket, err := net.Dial("tcp", *host)
	if err != nil {
		log.Fatal(err)
		return
	}
	defer socket.Close()

	for {
		key := <- channel
		fmt.Fprintf(socket, SET, key, *size, value)
	}
}

func main() {
	flag.Parse()
	runtime.GOMAXPROCS(runtime.NumCPU())
	i := 0
	log.Printf("Making %d workers...", *workers)
	channels := make([]chan string, *workers)
	for i = 0; i < *workers; i++ {
		channels[i] = make(chan string)
		go listen(channels[i])
	}
	value = strings.Repeat("x", *size)
	keys := make([]string, *iterations)
	// log.Print(value)
	log.Printf("Precalculating all %d keys...", *iterations)
	for i = 0; i < *iterations; i++ {
		// log.Printf("%s", i)
		keys[i] = fmt.Sprintf("thread_id=%d&last_modifed=12345679&order=1&secure=0", i)
	}

	log.Printf("Rock and roll.")
	start := time.Now()
	for i = 0; i < *iterations; i++ {
		channels[i % *workers] <- keys[i]
	}
	duration := float32(time.Now().Sub(start).Nanoseconds())/1E9
	log.Printf("Requests %f/s", float32(*iterations) / duration)
	log.Printf("Duration %f", duration)
}