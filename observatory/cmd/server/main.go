// Command server runs the observatory drift-aggregation service.
//
// Usage:
//
//	server -addr :8080 -db observatory.db -gateways gw-a,gw-b -capacity 1000
package main

import (
	"context"
	"flag"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"observatory/internal/obs"
)

func main() {
	addr := flag.String("addr", ":8080", "HTTP listen address")
	dbPath := flag.String("db", "observatory.db", "SQLite database file")
	capacity := flag.Int64("capacity", 1000, "max unprocessed received messages before new receives get a retryable response")
	gatewaysFlag := flag.String("gateways", "gw-a,gw-b", "comma-separated names of the fixed gateways")
	flag.Parse()

	var gateways []string
	seen := map[string]bool{}
	for _, g := range strings.Split(*gatewaysFlag, ",") {
		g = strings.TrimSpace(g)
		if g == "" || seen[g] {
			continue
		}
		seen[g] = true
		gateways = append(gateways, g)
	}
	if len(gateways) == 0 {
		log.Fatal("at least one gateway must be configured via -gateways")
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	store, err := obs.Open(ctx, *dbPath, gateways, *capacity)
	if err != nil {
		log.Fatalf("open store: %v", err)
	}
	defer store.Close()

	go store.RunProcessor(ctx)

	srv := &http.Server{Addr: *addr, Handler: obs.NewServer(store)}
	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		srv.Shutdown(shutdownCtx)
	}()

	log.Printf("observatory listening on %s db=%s gateways=%v capacity=%d", *addr, *dbPath, gateways, *capacity)
	if err := srv.ListenAndServe(); err != http.ErrServerClosed {
		log.Fatalf("http server: %v", err)
	}
}
