// Command gateway is a local fixture simulating one fixed gateway: it
// forwards telescope drift readings and observation progress reports to the
// observatory service. It demonstrates the client-side reliability rules:
// every message carries a stable id, capacity responses (503) are retried,
// and transport errors are retried with the same id (safe because receiving
// is idempotent).
//
// Usage:
//
//	gateway -server http://127.0.0.1:8080 -gateway gw-a \
//	  -telescopes T1,T2,T3 -rate 10 -duration 30s
package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"math"
	"math/rand"
	"net/http"
	"strings"
	"sync/atomic"
	"time"
)

type stats struct {
	sent    atomic.Int64
	dups    atomic.Int64
	retries atomic.Int64
}

type client struct {
	base  string
	hc    *http.Client
	stats *stats
}

// send delivers one message, retrying until the server accepts it.
func (c *client) send(msg map[string]any) {
	body, _ := json.Marshal(msg)
	for attempt := 1; ; attempt++ {
		resp, err := c.hc.Post(c.base+"/v1/messages", "application/json", bytes.NewReader(body))
		if err != nil {
			c.stats.retries.Add(1)
			log.Printf("RETRY id=%v transport error (attempt %d): %v", msg["msg_id"], attempt, err)
			time.Sleep(backoff(attempt))
			continue
		}
		data, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		switch resp.StatusCode {
		case http.StatusOK:
			var out struct {
				Seq       int64 `json:"seq"`
				Duplicate bool  `json:"duplicate"`
			}
			if err := json.Unmarshal(data, &out); err != nil {
				log.Fatalf("invalid response body: %s", data)
			}
			if out.Duplicate {
				c.stats.dups.Add(1)
			}
			c.stats.sent.Add(1)
			log.Printf("RECV seq=%d id=%v dup=%v", out.Seq, msg["msg_id"], out.Duplicate)
			return
		case http.StatusServiceUnavailable:
			c.stats.retries.Add(1)
			log.Printf("BUSY id=%v: server at capacity, retrying in 1s", msg["msg_id"])
			time.Sleep(time.Second)
		default:
			log.Fatalf("REJECTED id=%v status=%d body=%s", msg["msg_id"], resp.StatusCode, data)
		}
	}
}

func backoff(attempt int) time.Duration {
	d := time.Duration(attempt) * 200 * time.Millisecond
	if d > 2*time.Second {
		d = 2 * time.Second
	}
	return d
}

func main() {
	server := flag.String("server", "http://127.0.0.1:8080", "observatory base URL")
	gateway := flag.String("gateway", "gw-a", "gateway name (must be configured on the server)")
	telescopes := flag.String("telescopes", "T1,T2,T3", "comma-separated telescope names")
	rate := flag.Float64("rate", 5, "readings per second")
	duration := flag.Duration("duration", 30*time.Second, "how long to generate readings")
	seed := flag.Int64("seed", 1, "random seed (deterministic traffic for a fixed seed)")
	lateFraction := flag.Float64("late-fraction", 0.1, "fraction of readings that are late")
	lateMin := flag.Duration("late-min", 30*time.Second, "minimum lateness of late readings")
	lateMax := flag.Duration("late-max", 5*time.Minute, "maximum lateness of late readings")
	progressPeriod := flag.Duration("progress-period", 2*time.Second, "interval between progress reports")
	progressLag := flag.Duration("progress-lag", 5*time.Second, "progress reports trail wall clock by this much")
	flushOnExit := flag.Bool("flush-on-exit", true, "on exit, report progress that closes all windows so far")
	flag.Parse()

	tels := strings.Split(*telescopes, ",")
	rnd := rand.New(rand.NewSource(*seed))
	st := &stats{}
	c := &client{base: *server, hc: &http.Client{Timeout: 3 * time.Second}, stats: st}

	prefix := fmt.Sprintf("%s-%d", *gateway, time.Now().Unix())
	var n int64
	nextID := func() string {
		n++
		return fmt.Sprintf("%s-%06d", prefix, n)
	}

	sendReading := func() {
		ts := time.Now().Unix()
		if rnd.Float64() < *lateFraction {
			lag := *lateMin + time.Duration(rnd.Int63n(int64(*lateMax-*lateMin)+1))
			ts = time.Now().Add(-lag).Unix()
		}
		c.send(map[string]any{
			"gateway":   *gateway,
			"msg_id":    nextID(),
			"type":      "reading",
			"telescope": tels[rnd.Intn(len(tels))],
			"ts":        ts,
			"drift":     math.Round(rnd.Float64()*200) / 100,
		})
	}
	sendProgress := func(p int64) {
		c.send(map[string]any{
			"gateway":  *gateway,
			"msg_id":   nextID(),
			"type":     "progress",
			"progress": p,
		})
	}

	log.Printf("gateway %q -> %s: %g readings/s for %s, progress every %s",
		*gateway, *server, *rate, *duration, *progressPeriod)

	readingTicker := time.NewTicker(time.Duration(float64(time.Second) / *rate))
	progressTicker := time.NewTicker(*progressPeriod)
	deadline := time.NewTimer(*duration)
loop:
	for {
		select {
		case <-deadline.C:
			break loop
		case <-readingTicker.C:
			sendReading()
		case <-progressTicker.C:
			sendProgress(time.Now().Add(-*progressLag).Unix())
		}
	}
	readingTicker.Stop()
	progressTicker.Stop()
	if *flushOnExit {
		// Push the watermark past the end of the current minute window so
		// every window with readings gets published before we exit.
		sendProgress(time.Now().Add(60 * time.Second).Unix())
	}
	log.Printf("done: sent=%d duplicates=%d retries=%d", st.sent.Load(), st.dups.Load(), st.retries.Load())
}
