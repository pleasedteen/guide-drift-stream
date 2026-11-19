package obs

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func newTestServer(t *testing.T, capacity int64) (*Store, *httptest.Server) {
	t.Helper()
	store, _ := tempStore(t, twoGW, capacity)
	ts := httptest.NewServer(NewServer(store))
	t.Cleanup(ts.Close)
	return store, ts
}

func post(t *testing.T, base, body string) (int, map[string]any) {
	t.Helper()
	resp, err := http.Post(base+"/v1/messages", "application/json", strings.NewReader(body))
	if err != nil {
		t.Fatalf("post: %v", err)
	}
	defer resp.Body.Close()
	var out map[string]any
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	return resp.StatusCode, out
}

func get(t *testing.T, url string, out any) int {
	t.Helper()
	resp, err := http.Get(url)
	if err != nil {
		t.Fatalf("get %s: %v", url, err)
	}
	defer resp.Body.Close()
	if err := json.NewDecoder(resp.Body).Decode(out); err != nil {
		t.Fatalf("decode %s: %v", url, err)
	}
	return resp.StatusCode
}

func TestHTTPFlow(t *testing.T) {
	store, ts := newTestServer(t, 100)
	ctx := context.Background()

	// Validation errors.
	if code, out := post(t, ts.URL, `{"gateway":"gw-x","msg_id":"1","type":"reading","telescope":"T1","ts":1,"drift":0.1}`); code != 400 {
		t.Fatalf("unknown gateway: code=%d out=%v", code, out)
	}
	if code, _ := post(t, ts.URL, `{"gateway":"gw-a","msg_id":"1","type":"reading","telescope":"T1","ts":1,"drift":-0.1}`); code != 400 {
		t.Fatalf("negative drift: code=%d", code)
	}
	if code, _ := post(t, ts.URL, `{"gateway":"gw-a","msg_id":"1","type":"reading","telescope":"T1","ts":1,"drift":0.1,"bogus":1}`); code != 400 {
		t.Fatalf("unknown field: code=%d", code)
	}
	if code, _ := post(t, ts.URL, `{"gateway":"gw-a","msg_id":"1","type":"nonsense"}`); code != 400 {
		t.Fatalf("bad type: code=%d", code)
	}

	// Real traffic.
	code, out := post(t, ts.URL, `{"gateway":"gw-a","msg_id":"a-1","type":"reading","telescope":"T1","ts":600005,"drift":0.5}`)
	if code != 200 || out["duplicate"].(bool) || out["seq"].(float64) != 1 {
		t.Fatalf("first post: code=%d out=%v", code, out)
	}
	code, out = post(t, ts.URL, `{"gateway":"gw-a","msg_id":"a-1","type":"reading","telescope":"T1","ts":600005,"drift":0.5}`)
	if code != 200 || !out["duplicate"].(bool) || out["seq"].(float64) != 1 {
		t.Fatalf("retry: code=%d out=%v", code, out)
	}
	post(t, ts.URL, `{"gateway":"gw-a","msg_id":"a-2","type":"progress","progress":600060}`)
	post(t, ts.URL, `{"gateway":"gw-b","msg_id":"b-1","type":"progress","progress":600060}`)

	if _, err := store.ProcessAll(ctx); err != nil {
		t.Fatal(err)
	}

	var prog ProgressInfo
	get(t, ts.URL+"/v1/progress", &prog)
	if prog.Received != 3 || prog.Processed != 3 || prog.Pending != 0 {
		t.Fatalf("progress = %+v", prog)
	}
	if prog.Watermark == nil || *prog.Watermark != 600060 {
		t.Fatalf("watermark = %v", prog.Watermark)
	}

	var windows struct {
		Windows []WindowSummary `json:"windows"`
	}
	get(t, ts.URL+"/v1/windows?telescope=T1", &windows)
	if len(windows.Windows) != 1 || windows.Windows[0].Versions != 1 || windows.Windows[0].Count != 1 {
		t.Fatalf("windows = %+v", windows.Windows)
	}

	var detail WindowDetail
	get(t, ts.URL+"/v1/windows/T1/600000", &detail)
	if len(detail.History) != 1 || detail.History[0].TriggerSeq != 3 {
		t.Fatalf("window detail = %+v, want v1 triggered by seq 3", detail)
	}
	if code := get(t, ts.URL+"/v1/windows/T1/600005", &detail); code != 400 {
		t.Fatalf("unaligned window_start: code=%d", code)
	}
	if code := get(t, ts.URL+"/v1/windows/T9/600000", &detail); code != 404 {
		t.Fatalf("unknown window: code=%d", code)
	}

	var msg MessageDetail
	get(t, ts.URL+"/v1/messages/3", &msg)
	if msg.Disposition != DispApplied || len(msg.Triggered) != 1 || msg.Triggered[0].WindowStart != 600000 {
		t.Fatalf("message 3 = %+v", msg)
	}

	var versions struct {
		Versions []VersionRow `json:"versions"`
	}
	get(t, ts.URL+"/v1/versions", &versions)
	if len(versions.Versions) != 1 {
		t.Fatalf("versions = %+v", versions)
	}

	// Replay over HTTP.
	resp, err := http.Post(ts.URL+"/v1/replays", "application/json", nil)
	if err != nil {
		t.Fatal(err)
	}
	var summary ReplaySummary
	json.NewDecoder(resp.Body).Decode(&summary)
	resp.Body.Close()
	if resp.StatusCode != 201 || summary.ID != 1 || summary.PrefixSeq != 3 || summary.VersionCount != 1 {
		t.Fatalf("replay create: code=%d summary=%+v", resp.StatusCode, summary)
	}
	var replay ReplayDetail
	get(t, ts.URL+"/v1/replays/1", &replay)
	if len(replay.Versions) != 1 || replay.Versions[0].TriggerSeq != 3 {
		t.Fatalf("replay = %+v", replay)
	}
	if code := get(t, ts.URL+"/v1/replays/99", &replay); code != 404 {
		t.Fatalf("unknown replay: code=%d", code)
	}
}

func TestHTTPCapacityRetryable(t *testing.T) {
	_, ts := newTestServer(t, 1) // processor not running: nothing drains
	mk := func(id string) string {
		return fmt.Sprintf(`{"gateway":"gw-a","msg_id":%q,"type":"reading","telescope":"T1","ts":600005,"drift":0.5}`, id)
	}
	if code, _ := post(t, ts.URL, mk("a-1")); code != 200 {
		t.Fatalf("first: code=%d", code)
	}
	code, out := post(t, ts.URL, mk("a-2"))
	if code != 503 || out["retryable"] != true {
		t.Fatalf("at capacity: code=%d out=%v", code, out)
	}
	// Duplicate of an accepted message is still answered with its position.
	code, out = post(t, ts.URL, mk("a-1"))
	if code != 200 || out["seq"].(float64) != 1 || !out["duplicate"].(bool) {
		t.Fatalf("duplicate at capacity: code=%d out=%v", code, out)
	}
}

func TestHTTPAsyncProcessor(t *testing.T) {
	store, ts := newTestServer(t, 100)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go store.RunProcessor(ctx)

	for i := 0; i < 20; i++ {
		post(t, ts.URL, fmt.Sprintf(`{"gateway":"gw-a","msg_id":"a-%d","type":"reading","telescope":"T1","ts":%d,"drift":0.5}`, i, 600000+i))
	}
	post(t, ts.URL, `{"gateway":"gw-a","msg_id":"p-1","type":"progress","progress":600060}`)
	post(t, ts.URL, `{"gateway":"gw-b","msg_id":"p-2","type":"progress","progress":600060}`)

	deadline := time.Now().Add(5 * time.Second)
	for {
		var prog ProgressInfo
		get(t, ts.URL+"/v1/progress", &prog)
		if prog.Pending == 0 && prog.Received == 22 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("processor did not drain: %+v", prog)
		}
		time.Sleep(20 * time.Millisecond)
	}
	var detail WindowDetail
	get(t, ts.URL+"/v1/windows/T1/600000", &detail)
	if len(detail.History) != 1 || detail.History[0].Count != 20 {
		t.Fatalf("window = %+v, want v1 with count 20", detail)
	}
}
