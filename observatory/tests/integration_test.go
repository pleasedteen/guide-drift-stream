// Package integration runs end-to-end tests against the real server binary,
// including a kill -9 crash-recovery scenario and a gateway-fixture smoke run.
package integration

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"testing"
	"time"
)

var (
	serverBin  string
	gatewayBin string
)

func TestMain(m *testing.M) {
	dir, err := os.MkdirTemp("", "obs-bin-")
	if err != nil {
		panic(err)
	}
	defer os.RemoveAll(dir)
	serverBin = filepath.Join(dir, "obs-server")
	gatewayBin = filepath.Join(dir, "obs-gateway")
	build := func(pkg, out string) {
		cmd := exec.Command("go", "build", "-o", out, pkg)
		cmd.Dir = ".."
		if b, err := cmd.CombinedOutput(); err != nil {
			panic(fmt.Sprintf("build %s: %v\n%s", pkg, err, b))
		}
	}
	build("./cmd/server", serverBin)
	build("./cmd/gateway", gatewayBin)
	os.Exit(m.Run())
}

func freePort(t *testing.T) string {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	return fmt.Sprint(l.Addr().(*net.TCPAddr).Port)
}

type serverProc struct {
	cmd  *exec.Cmd
	base string
	log  *bytes.Buffer
}

func startServer(t *testing.T, db string) *serverProc {
	t.Helper()
	port := freePort(t)
	cmd := exec.Command(serverBin,
		"-addr", "127.0.0.1:"+port,
		"-db", db,
		"-gateways", "gw-a,gw-b",
		"-capacity", "1000",
	)
	logBuf := &bytes.Buffer{}
	cmd.Stdout = logBuf
	cmd.Stderr = logBuf
	if err := cmd.Start(); err != nil {
		t.Fatalf("start server: %v", err)
	}
	p := &serverProc{cmd: cmd, base: "http://127.0.0.1:" + port, log: logBuf}
	t.Cleanup(func() {
		p.cmd.Process.Kill()
		p.cmd.Wait()
		if t.Failed() {
			t.Logf("server log:\n%s", p.log)
		}
	})
	waitFor(t, "server ready", func() bool {
		resp, err := http.Get(p.base + "/v1/progress")
		if err != nil {
			return false
		}
		resp.Body.Close()
		return resp.StatusCode == 200
	})
	return p
}

func (p *serverProc) kill(t *testing.T) {
	t.Helper()
	if err := p.cmd.Process.Kill(); err != nil {
		t.Fatalf("kill: %v", err)
	}
	p.cmd.Wait()
}

func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("timeout waiting for %s", what)
}

func post(t *testing.T, base string, msg map[string]any) (seq int64, dup bool) {
	t.Helper()
	body, _ := json.Marshal(msg)
	resp, err := http.Post(base+"/v1/messages", "application/json", bytes.NewReader(body))
	if err != nil {
		t.Fatalf("post %v: %v", msg["msg_id"], err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Fatalf("post %v: status %d", msg["msg_id"], resp.StatusCode)
	}
	var out struct {
		Seq       int64 `json:"seq"`
		Duplicate bool  `json:"duplicate"`
	}
	json.NewDecoder(resp.Body).Decode(&out)
	return out.Seq, out.Duplicate
}

func get(t *testing.T, url string, out any) {
	t.Helper()
	resp, err := http.Get(url)
	if err != nil {
		t.Fatalf("get %s: %v", url, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Fatalf("get %s: status %d", url, resp.StatusCode)
	}
	if err := json.NewDecoder(resp.Body).Decode(out); err != nil {
		t.Fatalf("decode %s: %v", url, err)
	}
}

type progressInfo struct {
	Received  int64  `json:"received"`
	Processed int64  `json:"processed"`
	Pending   int64  `json:"pending"`
	Watermark *int64 `json:"watermark"`
}

type versionRow struct {
	Telescope   string  `json:"telescope"`
	WindowStart int64   `json:"window_start"`
	Version     int     `json:"version"`
	Count       int     `json:"count"`
	MaxDrift    float64 `json:"max_drift"`
	TriggerSeq  int64   `json:"trigger_seq"`
}

func readingMsg(gw, id, tel string, ts int64, drift float64) map[string]any {
	return map[string]any{"gateway": gw, "msg_id": id, "type": "reading", "telescope": tel, "ts": ts, "drift": drift}
}

func progressMsg(gw, id string, p int64) map[string]any {
	return map[string]any{"gateway": gw, "msg_id": id, "type": "progress", "progress": p}
}

func waitDrained(t *testing.T, base string, wantReceived int64) {
	t.Helper()
	waitFor(t, "pipeline drained", func() bool {
		var p progressInfo
		get(t, base+"/v1/progress", &p)
		return p.Received == wantReceived && p.Pending == 0
	})
}

func liveVersions(t *testing.T, base string) []versionRow {
	t.Helper()
	var out struct {
		Versions []versionRow `json:"versions"`
	}
	get(t, base+"/v1/versions", &out)
	return out.Versions
}

// TestKillRecovery drives traffic, kills the server with SIGKILL mid-stream,
// restarts it on the same SQLite file, and verifies that confirmed messages
// are not lost, published versions are neither duplicated nor missed, and a
// replay of the log matches the live pipeline exactly.
func TestKillRecovery(t *testing.T) {
	db := filepath.Join(t.TempDir(), "live.db")
	srv := startServer(t, db)

	// Phase 1: readings in window 600000 and 600060, then close 600000.
	post(t, srv.base, readingMsg("gw-a", "a-1", "T1", 600001, 0.1))
	post(t, srv.base, readingMsg("gw-a", "a-2", "T1", 600002, 0.5))
	post(t, srv.base, readingMsg("gw-b", "b-1", "T1", 600003, 0.3))
	post(t, srv.base, readingMsg("gw-b", "b-2", "T2", 600004, 0.4))
	post(t, srv.base, readingMsg("gw-a", "a-3", "T1", 600061, 0.7))
	post(t, srv.base, progressMsg("gw-a", "a-4", 600060))
	post(t, srv.base, progressMsg("gw-b", "b-3", 600060))           // closes window 600000
	post(t, srv.base, readingMsg("gw-a", "a-5", "T1", 600005, 0.9)) // late -> revision

	// A retry of an already-received message must return the original seq.
	if seq, dup := post(t, srv.base, readingMsg("gw-a", "a-5", "T1", 600005, 0.9)); seq != 8 || !dup {
		t.Fatalf("idempotent retry: seq=%d dup=%v, want 8,true", seq, dup)
	}

	waitDrained(t, srv.base, 8)

	// Crash the process hard.
	srv.kill(t)

	// Restart on the same database file.
	srv = startServer(t, db)

	// Everything confirmed before the crash is still there and processed.
	waitDrained(t, srv.base, 8)
	v := liveVersions(t, srv.base)
	want := []versionRow{
		{Telescope: "T1", WindowStart: 600000, Version: 1, Count: 3, MaxDrift: 0.5, TriggerSeq: 7},
		{Telescope: "T1", WindowStart: 600000, Version: 2, Count: 4, MaxDrift: 0.9, TriggerSeq: 8},
		{Telescope: "T2", WindowStart: 600000, Version: 1, Count: 1, MaxDrift: 0.4, TriggerSeq: 7},
	}
	if !reflect.DeepEqual(v, want) {
		t.Fatalf("versions after restart:\n got %+v\nwant %+v", v, want)
	}

	// Phase 2: advance the watermark past finalization of window 600000.
	post(t, srv.base, progressMsg("gw-a", "a-6", 600180))
	post(t, srv.base, progressMsg("gw-b", "b-4", 600190))           // closes 600060, finalizes 600000
	post(t, srv.base, readingMsg("gw-a", "a-7", "T1", 600006, 9.9)) // too late -> log only
	waitDrained(t, srv.base, 11)

	v = liveVersions(t, srv.base)
	want = append(want, versionRow{Telescope: "T1", WindowStart: 600060, Version: 1, Count: 1, MaxDrift: 0.7, TriggerSeq: 10})
	if !reflect.DeepEqual(v, want) {
		t.Fatalf("versions after phase 2:\n got %+v\nwant %+v", v, want)
	}

	// The too-late reading is in the receive log, marked ignored.
	var msg struct {
		Processed   bool   `json:"processed"`
		Disposition string `json:"disposition"`
	}
	get(t, srv.base+"/v1/messages/11", &msg)
	if !msg.Processed || msg.Disposition != "ignored_finalized" {
		t.Fatalf("message 11 = %+v, want processed + ignored_finalized", msg)
	}

	// Replay the full log: must equal the live pipeline version-for-version.
	resp, err := http.Post(srv.base+"/v1/replays", "application/json", nil)
	if err != nil {
		t.Fatal(err)
	}
	var summary struct {
		ID        int64 `json:"replay_id"`
		PrefixSeq int64 `json:"prefix_seq"`
	}
	json.NewDecoder(resp.Body).Decode(&summary)
	resp.Body.Close()
	if summary.PrefixSeq != 11 {
		t.Fatalf("replay prefix = %d, want 11", summary.PrefixSeq)
	}
	var replay struct {
		Versions []versionRow `json:"versions"`
	}
	get(t, fmt.Sprintf("%s/v1/replays/%d", srv.base, summary.ID), &replay)
	if !reflect.DeepEqual(replay.Versions, v) {
		t.Fatalf("replay != live:\nreplay %+v\nlive   %+v", replay.Versions, v)
	}
}

// TestGatewayFixtureSmoke runs the real gateway fixture against the server
// and checks that traffic flows, windows close, and replay matches live.
func TestGatewayFixtureSmoke(t *testing.T) {
	db := filepath.Join(t.TempDir(), "smoke.db")
	srv := startServer(t, db)

	run := func(gw string, seed int64) (*exec.Cmd, *bytes.Buffer) {
		cmd := exec.Command(gatewayBin,
			"-server", srv.base,
			"-gateway", gw,
			"-telescopes", "T1,T2",
			"-rate", "30",
			"-duration", "3s",
			"-seed", fmt.Sprint(seed),
			"-late-fraction", "0.3",
			"-late-min", "5s",
			"-late-max", "90s",
			"-progress-period", "300ms",
			"-progress-lag", "1s",
		)
		logBuf := &bytes.Buffer{}
		cmd.Stdout = logBuf
		cmd.Stderr = logBuf
		if err := cmd.Start(); err != nil {
			t.Fatal(err)
		}
		return cmd, logBuf
	}
	c1, log1 := run("gw-a", 1)
	c2, log2 := run("gw-b", 2)
	for i, c := range []*exec.Cmd{c1, c2} {
		if err := c.Wait(); err != nil {
			logs := []*bytes.Buffer{log1, log2}
			t.Fatalf("gateway fixture %d: %v\n%s", i, err, logs[i])
		}
	}

	waitFor(t, "fixture traffic processed", func() bool {
		var p progressInfo
		get(t, srv.base+"/v1/progress", &p)
		return p.Received > 0 && p.Pending == 0 && p.Watermark != nil
	})

	var windows struct {
		Windows []struct {
			Versions int `json:"versions"`
		} `json:"windows"`
	}
	get(t, srv.base+"/v1/windows", &windows)
	if len(windows.Windows) == 0 {
		t.Fatal("no windows after fixture run")
	}
	for _, w := range windows.Windows {
		if w.Versions == 0 {
			t.Fatalf("window without published version: %+v", windows.Windows)
		}
	}

	// Replay agrees with live for the fixture traffic as well.
	resp, err := http.Post(srv.base+"/v1/replays", "application/json", nil)
	if err != nil {
		t.Fatal(err)
	}
	var summary struct {
		ID int64 `json:"replay_id"`
	}
	json.NewDecoder(resp.Body).Decode(&summary)
	resp.Body.Close()
	var replay struct {
		Versions []versionRow `json:"versions"`
	}
	get(t, fmt.Sprintf("%s/v1/replays/%d", srv.base, summary.ID), &replay)
	if live := liveVersions(t, srv.base); !reflect.DeepEqual(replay.Versions, live) {
		t.Fatalf("replay != live (%d vs %d versions)", len(replay.Versions), len(live))
	}
	t.Logf("fixture run: %d windows, %d versions", len(windows.Windows), len(replay.Versions))
}
