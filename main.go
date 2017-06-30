package main

import (
	"flag"
	"runtime"
	"log"
	"net"
	"fmt"
	"bytes"
	"time"
	"github.com/cheggaaa/pb"
	"sync"
	"encoding/json"
	"math/rand"
	"bufio"
)

const (
	SET = "set %s 0 0 %d\r\n"
	GET = "get %s\r\n"
)

type McCmd struct {
	command int
	cmdstr []byte
	validx int
}

var (
	host = flag.String("host", ":11211", "Host of memcache server")
	minsize = flag.Int("minsize", 1000, "Min size of key value")
	maxsize = flag.Int("maxsize", 1000, "Max size of key value")
	iterations = flag.Int("n", 100, "How many requests total")
	workers = flag.Int("workers", 100, "Number of workers")
	ratio = flag.Int("sets", 100, "Percentage of SETs from total operations")
	prefixweights = flag.String("prefixes", "{}", "Key prefix and their weights (frequency), in JSON format")
)

var value []byte
var weights map[string]interface{}
var prefixes []string

func listen(channel chan McCmd, wg *sync.WaitGroup, bar *pb.ProgressBar) {
	socket, err := net.Dial("tcp", *host)
	if err != nil {
		log.Fatal(err)
		return
	}
	defer socket.Close()

	buf := bufio.NewReader(socket)

	for {
		cmd := <- channel
		// send command
		socket.Write(cmd.cmdstr)
		if cmd.command == 0 {
			socket.Write(value[cmd.validx:])
			buf.ReadBytes('\n')
		} else {
			for {
				retmsg, _ := buf.ReadBytes('\n')
				if string(retmsg) == "END\r\n" {
					break
				}
			}
		}
		bar.Increment()
		wg.Done()
	}
}

func main() {
	flag.Parse()
	runtime.GOMAXPROCS(runtime.NumCPU())
	i := 0
	bar := pb.New(*iterations)
	wg := new(sync.WaitGroup)
	log.Printf("Making %d workers...", *workers)
	channels := make([]chan McCmd, *workers)
	for i = 0; i < *workers; i++ {
		channels[i] = make(chan McCmd)
		go listen(channels[i], wg, bar)
	}
	value = append(bytes.Repeat([]byte("x"), *maxsize), []byte("\r\n")...)
	keys := make([]McCmd, *iterations)
	// log.Print(value)
	log.Printf("Precalculating all %d keys...", *iterations)
	json.Unmarshal([]byte(*prefixweights), &weights)
	for k, v := range weights {
		// log.Printf("prefix: %v weight: %v", k,v)
		pfx := make([]string, int(v.(float64)))
		for i = range pfx {
			pfx[i] = k
		}
		prefixes = append(prefixes, pfx...)
	}
	prefixcount := len(prefixes)
	// log.Printf("prefixes: %v %v", prefixcount, prefixes)
	rand.Seed(time.Now().Unix())
	for i = 0; i < *iterations; i++ {
		// log.Printf("%s", i)
		// shuffle keys while they are still being generated
		j := rand.Intn(i + 1)
                keys[i].command = keys[j].command
		if int((float64(i) / float64(*iterations)) * 100) < *ratio {
			keys[j].command = 0
		} else {
			keys[j].command = 1
		}
		prefix := ""
		if prefixcount > 0 {
			prefix = prefixes[int((float64(i) / float64(*iterations)) * float64(prefixcount))]
		}
                j = rand.Intn(i + 1)
                keys[i].cmdstr = keys[j].cmdstr
		keys[j].cmdstr = []byte(fmt.Sprintf("%sthread_id=%d&last_modifed=12345679&order=1&secure=0", prefix, i))
	}
	log.Print("Generating commands...")
	for i = 0; i < *iterations; i++ {
		if keys[i].command == 0 {
			valsize := *minsize + rand.Intn(*maxsize - *minsize + 1)
			keys[i].cmdstr = []byte(fmt.Sprintf(SET, keys[i].cmdstr, valsize))
			keys[i].validx = *maxsize - valsize
		} else {
			keys[i].cmdstr = []byte(fmt.Sprintf(GET, keys[i].cmdstr))
		}
		// log.Printf("keys[%d]:", i)
		// log.Printf("command: %d", keys[i].command)
		// log.Printf("cmdstr: %s", keys[i].cmdstr)
		// log.Printf("validx: %d", keys[i].validx)
	}
	// log.Printf("keys: %v", keys)

	log.Printf("Rock and roll.")
	bar.Start()
	start := time.Now()
	for i = 0; i < *iterations; i++ {
		wg.Add(1)
		channels[i % *workers] <- keys[i]
	}
	wg.Wait()
	duration := float32(time.Now().Sub(start).Nanoseconds())/1E9
	bar.Finish()
	log.Printf("Requests %f/s", float32(*iterations) / duration)
	log.Printf("Duration %f", duration)
}
